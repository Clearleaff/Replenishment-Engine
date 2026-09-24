use std::{collections::HashSet, sync::Arc, time::Instant};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use data_platform_common::InventoryBalanceState;
use replenishment_agent::{
    LlmReasoning, ReorderDecision, ReorderSimulation, RiskLevel, ToolCallRecord,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    config::{LlmConfig, LlmProvider},
    llm_tools::{ToolExecutionContext, ToolLoopGuard, execute_tool, tool_definitions},
    safety_firewall::{FirewallOutcome, LlmDecisionResult, SafetyFirewall},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmActionProposal {
    pub action: String,
    pub reorder_quantity: i32,
    pub urgency: String,
    pub risk_level: String,
    pub summary: String,
    pub key_points: Vec<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct LlmDecision {
    pub reasoning: LlmReasoning,
    pub proposed_quantity: i32,
    #[allow(dead_code)]
    pub action: String,
    pub risk_level: RiskLevel,
    pub fallback_reason: Option<LlmFallbackReason>,
}

#[derive(Debug)]
struct CircuitState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct LlmClient {
    http: reqwest::Client,
    pub config: Arc<LlmConfig>,
    circuit: Arc<Mutex<CircuitState>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LlmClientStatus {
    pub provider: LlmProvider,
    pub configured: bool,
    pub circuit_open: bool,
    pub consecutive_failures: u32,
    pub model: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmFallbackReason {
    Disabled,
    CircuitOpen,
    ProviderError,
    PolicyRejected,
}

impl LlmClient {
    pub fn new(config: LlmConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_default();

        Self {
            http,
            config: Arc::new(config),
            circuit: Arc::new(Mutex::new(CircuitState {
                consecutive_failures: 0,
                opened_at: None,
            })),
        }
    }

    pub async fn status(&self) -> LlmClientStatus {
        let circuit = self.circuit.lock().await;
        LlmClientStatus {
            provider: self.config.provider,
            configured: self.config.configured(),
            circuit_open: self.is_open_locked(&circuit),
            consecutive_failures: circuit.consecutive_failures,
            model: self.config.model.clone(),
        }
    }

    pub async fn propose_with_tools(
        &self,
        ctx: &ToolExecutionContext<'_>,
        balance: &InventoryBalanceState,
    ) -> LlmDecision {
        let deterministic = deterministic_reasoning(ctx.base_decision);

        if !self.config.configured() {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: ctx.base_decision.recommended_quantity,
                action: if ctx.base_decision.recommended_quantity > 0 {
                    "REORDER".to_owned()
                } else {
                    "WAIT".to_owned()
                },
                risk_level: ctx.base_decision.risk_level,
                fallback_reason: Some(LlmFallbackReason::Disabled),
            };
        }

        if self.circuit_is_open().await {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: ctx.base_decision.recommended_quantity,
                action: if ctx.base_decision.recommended_quantity > 0 {
                    "REORDER".to_owned()
                } else {
                    "WAIT".to_owned()
                },
                risk_level: ctx.base_decision.risk_level,
                fallback_reason: Some(LlmFallbackReason::CircuitOpen),
            };
        }

        match self.run_tool_calling_loop(ctx, balance).await {
            Ok((proposal, tool_calls, tools_used)) => {
                let firewall = SafetyFirewall::new(self.config.max_order_quantity);
                let candidate = LlmDecisionResult {
                    action: proposal.action.clone(),
                    reorder_quantity: proposal.reorder_quantity,
                    urgency: proposal.urgency.clone(),
                    risk_level: proposal.risk_level.clone(),
                    reasoning: LlmReasoning {
                        summary: proposal.summary.clone(),
                        key_points: proposal.key_points.clone(),
                        confidence: proposal.confidence,
                        tools_used,
                        tool_calls,
                    },
                };

                match firewall.validate(candidate, balance) {
                    FirewallOutcome::Passed(res) => {
                        self.record_success().await;
                        let risk = parse_risk_level(&res.risk_level);
                        LlmDecision {
                            reasoning: res.reasoning,
                            proposed_quantity: res.reorder_quantity,
                            action: res.action,
                            risk_level: risk,
                            fallback_reason: None,
                        }
                    }
                    FirewallOutcome::Clamped {
                        clamped_quantity,
                        reason,
                        mut decision,
                        ..
                    } => {
                        self.record_success().await;
                        decision
                            .reasoning
                            .key_points
                            .push(format!("CLAMPED: {reason}"));
                        let risk = parse_risk_level(&decision.risk_level);
                        LlmDecision {
                            reasoning: decision.reasoning,
                            proposed_quantity: clamped_quantity,
                            action: decision.action,
                            risk_level: risk,
                            fallback_reason: None,
                        }
                    }
                    FirewallOutcome::Rejected { reason } => {
                        warn!(
                            reason,
                            "Safety firewall rejected LLM proposal; degrading to fallback"
                        );
                        self.record_success().await;
                        LlmDecision {
                            reasoning: deterministic,
                            proposed_quantity: ctx.base_decision.recommended_quantity,
                            action: "REVIEW".to_owned(),
                            risk_level: ctx.base_decision.risk_level,
                            fallback_reason: Some(LlmFallbackReason::PolicyRejected),
                        }
                    }
                }
            }
            Err(err) => {
                warn!(
                    ?err,
                    "LLM tool-calling loop failed; degrading to deterministic fallback"
                );
                self.record_failure().await;
                LlmDecision {
                    reasoning: deterministic,
                    proposed_quantity: ctx.base_decision.recommended_quantity,
                    action: if ctx.base_decision.recommended_quantity > 0 {
                        "REORDER".to_owned()
                    } else {
                        "WAIT".to_owned()
                    },
                    risk_level: ctx.base_decision.risk_level,
                    fallback_reason: Some(LlmFallbackReason::ProviderError),
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn validate_or_fallback(
        &self,
        decision: &ReorderDecision,
        simulation: &ReorderSimulation,
        balance: &InventoryBalanceState,
    ) -> LlmDecision {
        let deterministic = LlmReasoning {
            summary: format!(
                "Deterministic fallback recommends {} units for SKU {} at {} because policy found {:?} risk and simulation selected capacity-safe alternative.",
                simulation.selected_quantity,
                decision.sku_id,
                decision.location_code,
                decision.risk_level
            ),
            key_points: decision.reason_codes.clone(),
            confidence: 0.80,
            tools_used: Vec::new(),
            tool_calls: Vec::new(),
        };

        if !self.config.configured() {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: simulation.selected_quantity,
                action: if simulation.selected_quantity > 0 {
                    "REORDER".to_owned()
                } else {
                    "WAIT".to_owned()
                },
                risk_level: decision.risk_level,
                fallback_reason: Some(LlmFallbackReason::Disabled),
            };
        }

        if self.circuit_is_open().await {
            return LlmDecision {
                reasoning: deterministic,
                proposed_quantity: simulation.selected_quantity,
                action: if simulation.selected_quantity > 0 {
                    "REORDER".to_owned()
                } else {
                    "WAIT".to_owned()
                },
                risk_level: decision.risk_level,
                fallback_reason: Some(LlmFallbackReason::CircuitOpen),
            };
        }

        let firewall = SafetyFirewall::new(self.config.max_order_quantity);
        let candidate = LlmDecisionResult {
            action: if simulation.selected_quantity > 0 {
                "REORDER".to_owned()
            } else {
                "WAIT".to_owned()
            },
            reorder_quantity: simulation.selected_quantity,
            urgency: format!("{:?}", decision.risk_level).to_ascii_uppercase(),
            risk_level: format!("{:?}", decision.risk_level).to_ascii_uppercase(),
            reasoning: deterministic.clone(),
        };

        match firewall.validate(candidate, balance) {
            FirewallOutcome::Passed(res) => LlmDecision {
                reasoning: res.reasoning,
                proposed_quantity: res.reorder_quantity,
                action: res.action,
                risk_level: decision.risk_level,
                fallback_reason: None,
            },
            FirewallOutcome::Clamped {
                clamped_quantity,
                decision: res,
                ..
            } => LlmDecision {
                reasoning: res.reasoning,
                proposed_quantity: clamped_quantity,
                action: res.action,
                risk_level: decision.risk_level,
                fallback_reason: None,
            },
            FirewallOutcome::Rejected { .. } => LlmDecision {
                reasoning: deterministic,
                proposed_quantity: simulation.selected_quantity,
                action: "REVIEW".to_owned(),
                risk_level: decision.risk_level,
                fallback_reason: Some(LlmFallbackReason::PolicyRejected),
            },
        }
    }

    async fn run_tool_calling_loop(
        &self,
        ctx: &ToolExecutionContext<'_>,
        balance: &InventoryBalanceState,
    ) -> Result<(LlmActionProposal, Vec<ToolCallRecord>, Vec<String>)> {
        let key = self
            .config
            .api_key
            .as_deref()
            .context("LLM API key missing")?;

        let initial_prompt = format!(
            "Stock depletion detected for SKU {} at warehouse location {}. \
             Current stock is on_hand={}, reserved={}, available={}, max_stock={}. \
             Investigate demand signals, event calendar, supplier disruptions, and simulation using tools, \
             then output your replenishment decision in JSON format.",
            balance.sku_id,
            balance.location_code,
            balance.on_hand,
            balance.reserved,
            balance.available,
            balance.max_stock
        );

        let mut messages = vec![
            json!({ "role": "system", "content": &self.config.system_prompt }),
            json!({ "role": "user", "content": initial_prompt }),
        ];

        let mut tool_calls_recorded = Vec::new();
        let mut tools_used = HashSet::new();
        let mut loop_guard = ToolLoopGuard::new(self.config.max_identical_tool_calls);
        let max_rounds = self.config.max_tool_rounds;
        let mut last_content: Option<String> = None;
        let mut cumulative_tokens: u32 = 0;
        let mut completed_rounds: usize = 0;

        for round in 0..max_rounds {
            let request_body = json!({
                "model": self.config.model,
                "temperature": 0.1,
                "messages": messages,
                "tools": tool_definitions(),
                "tool_choice": "auto",
            });

            let mut response = self
                .http
                .post(&self.config.endpoint)
                .bearer_auth(key)
                .json(&request_body)
                .send()
                .await
                .context("LLM request failed")?;

            let mut retries = 0;
            while (response.status() == StatusCode::TOO_MANY_REQUESTS
                || response.status() == StatusCode::SERVICE_UNAVAILABLE)
                && retries < 4
            {
                retries += 1;
                let status = response.status();
                let retry_header = response
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.to_owned());
                let err_body = response.text().await.unwrap_or_default();
                let sleep_secs = parse_retry_delay(retry_header.as_deref(), &err_body, retries);
                let hint = err_body
                    .lines()
                    .next()
                    .unwrap_or("")
                    .chars()
                    .take(150)
                    .collect::<String>();
                warn!(
                    status = %status,
                    sleep_secs,
                    retries,
                    hint = %hint,
                    "LLM provider rate limited or unavailable; backing off"
                );
                tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
                response = self
                    .http
                    .post(&self.config.endpoint)
                    .bearer_auth(key)
                    .json(&request_body)
                    .send()
                    .await
                    .context("LLM request retry failed")?;
            }

            if response.status() == StatusCode::TOO_MANY_REQUESTS
                || response.status() == StatusCode::SERVICE_UNAVAILABLE
            {
                let status = response.status();
                let err_text = response.text().await.unwrap_or_default();
                bail!(
                    "LLM provider failed with HTTP {status} after {retries} backoff retries: {err_text}"
                );
            }
            if !response.status().is_success() {
                let status = response.status();
                let err_text = response.text().await.unwrap_or_default();
                bail!("LLM provider returned HTTP {status}: {err_text}");
            }

            let envelope: ChatCompletionResponse = response
                .json()
                .await
                .context("Failed to parse LLM response JSON envelope")?;
            let choice = envelope
                .choices
                .into_iter()
                .next()
                .context("LLM response contained no choices")?;

            let round_tokens = envelope.usage.as_ref().map(|u| u.total_tokens).unwrap_or(0);
            cumulative_tokens += round_tokens;
            completed_rounds = round + 1;

            if let Some(content) = choice
                .message
                .content
                .as_deref()
                .filter(|s| !s.trim().is_empty())
            {
                last_content = Some(content.to_owned());
            }

            if choice.message.tool_calls.is_empty() {
                let missing_calendar = !tools_used.contains("get_event_calendar");
                let missing_signals = !tools_used.contains("get_supplier_signals");

                if (missing_calendar || missing_signals) && round + 1 < max_rounds {
                    let mut reminders = Vec::new();
                    if missing_calendar {
                        reminders.push(
                            "`get_event_calendar` to check upcoming festive spikes or holidays",
                        );
                    }
                    if missing_signals {
                        reminders.push("`get_supplier_signals` to inspect supply chain bottlenecks or supplier disruptions");
                    }
                    info!(
                        round,
                        missing_calendar,
                        missing_signals,
                        "LLM stopped calling tools early without querying mandatory domain tools; prompting continuation"
                    );
                    messages.push(json!({
                        "role": "assistant",
                        "content": choice.message.content.as_deref().unwrap_or("")
                    }));
                    messages.push(json!({
                        "role": "user",
                        "content": format!(
                            "Before finalizing your replenishment decision, you are REQUIRED to query: {}. Please invoke these tools now.",
                            reminders.join(" and ")
                        )
                    }));
                    continue;
                }

                info!(
                    round,
                    "LLM stopped calling tools; generating final decision"
                );
                break;
            }

            // Append assistant tool_calls message
            messages.push(json!({
                "role": "assistant",
                "content": choice.message.content,
                "tool_calls": choice.message.tool_calls,
            }));

            // Execute each requested tool call
            for call in choice.message.tool_calls {
                tools_used.insert(call.function.name.clone());

                let tool_output = match loop_guard
                    .check_and_record(&call.function.name, &call.function.arguments)
                {
                    Ok(()) => {
                        execute_tool(&call.function.name, &call.function.arguments, ctx).await
                    }
                    Err(loop_err) => {
                        warn!(tool = %call.function.name, error = %loop_err, "Loop guard blocked tool call");
                        crate::llm_tools::ToolCallOutput::error(loop_err)
                    }
                };

                let output_str = serde_json::to_string(&tool_output).unwrap_or_default();
                tool_calls_recorded.push(ToolCallRecord {
                    tool_name: call.function.name.clone(),
                    input: call.function.arguments.clone(),
                    output: output_str.clone(),
                    timestamp: Utc::now(),
                });

                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call.id,
                    "name": call.function.name,
                    "content": output_str,
                }));
            }
        }

        // Final structured answer parsing with 1 retry on parse failure (Amendment #1)
        let parsed_action = self
            .obtain_final_action(&mut messages, last_content, key, &mut cumulative_tokens)
            .await?;
        let mut sorted_tools: Vec<String> = tools_used.into_iter().collect();
        sorted_tools.sort();

        info!(
            sku_id = %ctx.base_decision.sku_id,
            location_code = %ctx.base_decision.location_code,
            rounds = completed_rounds,
            estimated_tokens = self.config.estimate_per_call,
            actual_tokens = cumulative_tokens,
            delta = (cumulative_tokens as i64) - (self.config.estimate_per_call as i64),
            "LLM tool-calling loop completed; actual token usage recorded against estimate"
        );

        Ok((parsed_action, tool_calls_recorded, sorted_tools))
    }

    async fn obtain_final_action(
        &self,
        messages: &mut Vec<serde_json::Value>,
        last_content: Option<String>,
        key: &str,
        cumulative_tokens: &mut u32,
    ) -> Result<LlmActionProposal> {
        // Try parsing last content if available
        if let Some(action) = last_content
            .as_deref()
            .and_then(|c| parse_action_json(c).ok())
        {
            return Ok(action);
        }

        // Otherwise, request final decision with json_object mode
        messages.push(json!({
            "role": "user",
            "content": "All tool investigations are complete. Do not invoke any tools. Synthesize all collected facts and output your final replenishment decision now as a single valid json object with keys: action, reorder_quantity, urgency, risk_level, summary, key_points, confidence. Action must be REORDER, WAIT, or REVIEW."
        }));

        let request = json!({
            "model": self.config.model,
            "temperature": 0.1,
            "messages": messages,
            "response_format": { "type": "json_object" }
        });

        let mut response = self
            .http
            .post(&self.config.endpoint)
            .bearer_auth(key)
            .json(&request)
            .send()
            .await
            .context("LLM final decision request failed")?;

        let mut final_retries = 0;
        while (response.status() == StatusCode::TOO_MANY_REQUESTS
            || response.status() == StatusCode::SERVICE_UNAVAILABLE)
            && final_retries < 4
        {
            final_retries += 1;
            let status = response.status();
            let retry_header = response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_owned());
            let err_body = response.text().await.unwrap_or_default();
            let sleep_secs = parse_retry_delay(retry_header.as_deref(), &err_body, final_retries);
            let hint = err_body
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(150)
                .collect::<String>();
            warn!(
                status = %status,
                sleep_secs,
                final_retries,
                hint = %hint,
                "LLM provider rate limited or unavailable on final decision; backing off"
            );
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            response = self
                .http
                .post(&self.config.endpoint)
                .bearer_auth(key)
                .json(&request)
                .send()
                .await
                .context("LLM final decision retry failed")?;
        }

        if !response.status().is_success() {
            let status = response.status();
            let err_text = response.text().await.unwrap_or_default();
            bail!("LLM final decision failed with HTTP {status}: {err_text}");
        }

        let envelope: ChatCompletionResponse = response
            .json()
            .await
            .context("LLM final decision response JSON failed")?;
        *cumulative_tokens += envelope.usage.as_ref().map(|u| u.total_tokens).unwrap_or(0);
        let content = envelope
            .choices
            .first()
            .and_then(|c| c.message.content.as_deref())
            .context("LLM final response content was empty")?;

        match parse_action_json(content) {
            Ok(action) => Ok(action),
            Err(first_err) => {
                // Retry once with parse error appended (Amendment #1)
                warn!(
                    ?first_err,
                    "LLM JSON parse failed; retrying once with error feedback"
                );
                messages.push(json!({ "role": "assistant", "content": content }));
                messages.push(json!({
                    "role": "user",
                    "content": format!(
                        "Your response could not be parsed: {first_err}. \
                         Return ONLY valid JSON matching this schema: \
                         {{\"action\":\"REORDER\"|\"WAIT\"|\"REVIEW\",\"reorder_quantity\":<int>,\"urgency\":\"LOW\"|\"MEDIUM\"|\"HIGH\"|\"CRITICAL\",\"risk_level\":\"LOW\"|\"MEDIUM\"|\"HIGH\"|\"CRITICAL\",\"summary\":\"...\",\"key_points\":[\"...\"],\"confidence\":<float 0-1>}}"
                    )
                }));

                let retry_request = json!({
                    "model": self.config.model,
                    "temperature": 0.1,
                    "messages": messages,
                    "response_format": { "type": "json_object" }
                });

                let retry_res = self
                    .http
                    .post(&self.config.endpoint)
                    .bearer_auth(key)
                    .json(&retry_request)
                    .send()
                    .await
                    .context("LLM final decision retry failed")?;

                let retry_env: ChatCompletionResponse = retry_res
                    .json()
                    .await
                    .context("LLM final decision retry JSON failed")?;
                *cumulative_tokens += retry_env.usage.as_ref().map(|u| u.total_tokens).unwrap_or(0);
                let retry_content = retry_env
                    .choices
                    .first()
                    .and_then(|c| c.message.content.as_deref())
                    .context("LLM retry response content was empty")?;

                parse_action_json(retry_content)
            }
        }
    }

    async fn circuit_is_open(&self) -> bool {
        let mut circuit = self.circuit.lock().await;
        if let Some(opened_at) = circuit.opened_at {
            if opened_at.elapsed() >= self.config.circuit_cooldown {
                circuit.opened_at = None;
                circuit.consecutive_failures = 0;
                return false;
            }
            return true;
        }
        false
    }

    fn is_open_locked(&self, circuit: &CircuitState) -> bool {
        circuit
            .opened_at
            .is_some_and(|opened_at| opened_at.elapsed() < self.config.circuit_cooldown)
    }

    async fn record_failure(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures += 1;
        if circuit.consecutive_failures >= self.config.circuit_failure_threshold {
            circuit.opened_at = Some(Instant::now());
        }
    }

    async fn record_success(&self) {
        let mut circuit = self.circuit.lock().await;
        circuit.consecutive_failures = 0;
        circuit.opened_at = None;
    }
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ChatUsage {
    #[serde(default)]
    #[allow(dead_code)]
    prompt_tokens: u32,
    #[serde(default)]
    #[allow(dead_code)]
    completion_tokens: u32,
    #[serde(default)]
    total_tokens: u32,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Debug, Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCallMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolCallMessage {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: ToolCallFunction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    extra_content: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolCallFunction {
    name: String,
    arguments: String,
}

pub fn parse_action_json(value: &str) -> Result<LlmActionProposal> {
    let clean = value.trim();
    let json_str = if let Some(stripped) = clean.strip_prefix("```json") {
        stripped.strip_suffix("```").unwrap_or(stripped).trim()
    } else if let Some(stripped) = clean.strip_prefix("```") {
        stripped.strip_suffix("```").unwrap_or(stripped).trim()
    } else {
        clean
    };
    let proposal: LlmActionProposal =
        serde_json::from_str(json_str).context("LLM action JSON parse failed")?;
    if proposal.summary.trim().is_empty()
        || proposal.key_points.is_empty()
        || !(0.0..=1.0).contains(&proposal.confidence)
    {
        bail!("LLM action failed required field validation");
    }
    Ok(proposal)
}

pub fn parse_retry_delay(header_val: Option<&str>, body: &str, retries: u32) -> u64 {
    if let Some(wait) = header_val.and_then(|s| s.parse::<u64>().ok()) {
        return (wait + 2).clamp(3, 60);
    }

    if let Some(idx) = body.find("\"retryDelay\":") {
        let snippet = &body[idx + 13..];
        let digits: String = snippet
            .chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(secs) = digits.parse::<u64>() {
            return (secs + 3).clamp(3, 60);
        }
    }

    if let Some(idx) = body.find("Please retry in ") {
        let snippet = &body[idx + 16..];
        let digits: String = snippet.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(secs) = digits.parse::<u64>() {
            return (secs + 3).clamp(3, 60);
        }
    }

    (2u64.pow(retries) * 4).clamp(4, 45)
}

fn deterministic_reasoning(decision: &ReorderDecision) -> LlmReasoning {
    LlmReasoning {
        summary: format!(
            "Deterministic fallback recommends {} units for SKU {} at {} because policy found {:?} risk.",
            decision.recommended_quantity,
            decision.sku_id,
            decision.location_code,
            decision.risk_level
        ),
        key_points: decision.reason_codes.clone(),
        confidence: 0.80,
        tools_used: Vec::new(),
        tool_calls: Vec::new(),
    }
}

fn parse_risk_level(s: &str) -> RiskLevel {
    match s.trim().to_ascii_uppercase().as_str() {
        "CRITICAL" => RiskLevel::Critical,
        "HIGH" => RiskLevel::High,
        "MEDIUM" => RiskLevel::Medium,
        _ => RiskLevel::Low,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_action_json_accepts_valid_payload() {
        let json = r#"{
            "action": "REORDER",
            "reorder_quantity": 45,
            "urgency": "HIGH",
            "risk_level": "HIGH",
            "summary": "Stock depleted below safety threshold with Sunday spike expected.",
            "key_points": ["DEPLETION", "FESTIVE_SPIKE"],
            "confidence": 0.92
        }"#;

        let parsed = parse_action_json(json).unwrap();
        assert_eq!(parsed.action, "REORDER");
        assert_eq!(parsed.reorder_quantity, 45);
        assert_eq!(parsed.confidence, 0.92);
    }

    #[test]
    fn parse_action_json_accepts_markdown_code_blocks() {
        let json = "```json\n{\n  \"action\": \"REORDER\",\n  \"reorder_quantity\": 500,\n  \"urgency\": \"HIGH\",\n  \"risk_level\": \"CRITICAL\",\n  \"summary\": \"Emergency restock\",\n  \"key_points\": [\"Out of stock\"],\n  \"confidence\": 0.95\n}\n```";
        let parsed = parse_action_json(json).unwrap();
        assert_eq!(parsed.action, "REORDER");
        assert_eq!(parsed.reorder_quantity, 500);
        assert_eq!(parsed.confidence, 0.95);
    }

    #[test]
    fn parse_action_json_rejects_missing_key_points() {
        let json = r#"{
            "action": "REORDER",
            "reorder_quantity": 45,
            "urgency": "HIGH",
            "risk_level": "HIGH",
            "summary": "Reorder needed",
            "key_points": [],
            "confidence": 0.92
        }"#;

        assert!(parse_action_json(json).is_err());
    }

    #[test]
    fn parse_action_json_rejects_invalid_confidence() {
        let json = r#"{
            "action": "REORDER",
            "reorder_quantity": 45,
            "urgency": "HIGH",
            "risk_level": "HIGH",
            "summary": "Reorder needed",
            "key_points": ["SPIKE"],
            "confidence": 1.5
        }"#;

        assert!(parse_action_json(json).is_err());
    }

    #[test]
    fn parse_action_json_rejects_malformed_json() {
        assert!(parse_action_json("not-a-json-string").is_err());
        assert!(parse_action_json(r#"{"action":"REORDER"}"#).is_err());
        assert!(parse_action_json(r#"{"action":"REORDER","reorder_quantity":50}"#).is_err());
    }

    #[test]
    fn tool_error_results_in_review_action_selection() {
        // System prompt directs LLM: if tool returns status "ERROR", choose action "REVIEW" with urgency "HIGH" and confidence <= 0.5
        let response_json = r#"{
            "action": "REVIEW",
            "reorder_quantity": 0,
            "urgency": "HIGH",
            "risk_level": "HIGH",
            "summary": "ClickHouse tool query failed with status ERROR (connection refused). Manual operator review required.",
            "key_points": ["TOOL_ERROR", "WAREHOUSE_UNREACHABLE"],
            "confidence": 0.4
        }"#;

        let proposal = parse_action_json(response_json).unwrap();
        assert_eq!(proposal.action, "REVIEW");
        assert_eq!(proposal.reorder_quantity, 0);
        assert!(proposal.confidence <= 0.5);

        let balance = InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: 10,
            reserved: 0,
            available: 10,
            safety_stock: 20,
            reorder_point: 30,
            max_stock: 100,
            version: 1,
            updated_at: Utc::now(),
        };
        let firewall = SafetyFirewall::new(5000);
        let candidate = LlmDecisionResult {
            action: proposal.action.clone(),
            reorder_quantity: proposal.reorder_quantity,
            urgency: proposal.urgency.clone(),
            risk_level: proposal.risk_level.clone(),
            reasoning: LlmReasoning {
                summary: proposal.summary.clone(),
                key_points: proposal.key_points.clone(),
                confidence: proposal.confidence,
                tools_used: vec!["get_event_calendar".to_owned()],
                tool_calls: Vec::new(),
            },
        };
        let firewall_result = firewall.validate(candidate, &balance);
        assert!(matches!(firewall_result, FirewallOutcome::Passed(_)));
        if let FirewallOutcome::Passed(passed) = firewall_result {
            assert_eq!(passed.action, "REVIEW");
            assert_eq!(passed.reorder_quantity, 0);
        }
    }

    #[tokio::test]
    async fn circuit_breaker_falls_back_when_provider_is_unavailable() {
        let config = LlmConfig {
            provider: LlmProvider::Groq,
            api_key: Some("dummy-key".to_owned()),
            model: "test-model".to_owned(),
            endpoint: "http://127.0.0.1:1/v1/chat/completions".to_owned(),
            timeout: std::time::Duration::from_millis(100),
            circuit_failure_threshold: 1,
            circuit_cooldown: std::time::Duration::from_secs(60),
            max_markdown_percent: 20.0,
            max_order_quantity: 100,
            max_tool_rounds: 6,
            tool_call_timeout: std::time::Duration::from_secs(5),
            max_identical_tool_calls: 1,
            system_prompt: crate::config::default_system_prompt(),
            ..LlmConfig::default()
        };
        let client = LlmClient::new(config);
        let balance = InventoryBalanceState {
            sku_id: 42,
            location_code: "NCR".to_owned(),
            on_hand: 50,
            reserved: 0,
            available: 50,
            safety_stock: 10,
            reorder_point: 20,
            max_stock: 100,
            version: 1,
            updated_at: Utc::now(),
        };
        let decision = ReorderDecision {
            decision_id: uuid::Uuid::new_v4(),
            sku_id: 42,
            location_code: "NCR".to_owned(),
            created_at: Utc::now(),
            forecast_units_per_day: 10.0,
            baseline_units_per_day: 10.0,
            recent_units_per_day: 10.0,
            spike_score: 0.0,
            risk_level: RiskLevel::Low,
            estimated_stockout_at: None,
            inventory_position: 50,
            safety_stock: 10,
            target_inventory: 60,
            recommended_quantity: 10,
            horizon_demand_units: 30.0,
            horizon_forecast: Vec::new(),
            reason_codes: vec!["INVENTORY_POSITION_BELOW_ADAPTIVE_TARGET".to_owned()],
            model_version: "test".to_owned(),
            mode: replenishment_agent::AgentMode::Recommend,
        };
        let warehouse = warehouse::ClickHouseWarehouse::new(warehouse::ClickHouseConfig {
            url: "http://127.0.0.1:1".to_owned(),
            database: "test".to_owned(),
            user: "test".to_owned(),
            password: "test".to_owned(),
        });
        let ctx = ToolExecutionContext {
            warehouse: &warehouse,
            current_balance: &balance,
            current_features: None,
            feature_state: None,
            base_decision: &decision,
            timeout: std::time::Duration::from_millis(100),
            macro_cache: None,
        };

        let result = client.propose_with_tools(&ctx, &balance).await;
        assert_eq!(result.proposed_quantity, 10);
        assert_eq!(
            result.fallback_reason,
            Some(LlmFallbackReason::ProviderError)
        );
        assert!(client.status().await.circuit_open);
    }

    #[test]
    fn parse_retry_delay_prefers_http_retry_after_header() {
        assert_eq!(parse_retry_delay(Some("10"), "{}", 1), 12);
    }

    #[test]
    fn parse_retry_delay_extracts_google_retry_delay_from_body() {
        let body = r#"{"error":{"message":"Quota exceeded. Please retry in 25.64s.","details":[{"@type":"...RetryInfo","retryDelay":"25s"}]}}"#;
        assert_eq!(parse_retry_delay(None, body, 1), 28);
    }

    #[test]
    fn parse_retry_delay_falls_back_to_message_retry_hint() {
        let body = r#"{"error":{"message":"Quota exceeded. Please retry in 20s."}}"#;
        assert_eq!(parse_retry_delay(None, body, 1), 23);
    }

    #[test]
    fn parse_retry_delay_falls_back_to_exponential_backoff() {
        assert_eq!(parse_retry_delay(None, "{}", 1), 8);
        assert_eq!(parse_retry_delay(None, "{}", 2), 16);
    }
}
