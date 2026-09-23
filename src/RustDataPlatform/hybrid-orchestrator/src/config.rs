use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use serde::Serialize;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub amqp_url: String,
    pub queue_name: String,
    pub consumer_tag: String,
    pub bind_addr: SocketAddr,
    pub execution_mode: ExecutionMode,
    pub llm: LlmConfig,
    pub clickhouse: warehouse::ClickHouseConfig,
    pub proposal_ttl: Duration,
    pub cleanup_interval: Duration,
    pub amqp_max_retry_delay: Duration,
}

impl OrchestratorConfig {
    pub fn from_env() -> Self {
        let bind_addr = env("HYBRID_ORCHESTRATOR_BIND")
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000));

        let clickhouse = warehouse::ClickHouseConfig {
            url: env("CLICKHOUSE_URL").unwrap_or_else(|| "http://127.0.0.1:20255".to_owned()),
            database: env("CLICKHOUSE_DATABASE").unwrap_or_else(|| "eshop_analytics".to_owned()),
            user: env("CLICKHOUSE_USER").unwrap_or_else(|| "eshop".to_owned()),
            password: env("CLICKHOUSE_PASSWORD").unwrap_or_else(|| "clickhousepass123".to_owned()),
        };

        Self {
            amqp_url: resolve_amqp_url(),
            queue_name: env("HYBRID_ORCHESTRATOR_QUEUE")
                .unwrap_or_else(|| "eshop.inventory.order_stock_confirmed".to_owned()),
            consumer_tag: env("HYBRID_ORCHESTRATOR_CONSUMER_TAG")
                .unwrap_or_else(|| "hybrid-orchestrator".to_owned()),
            bind_addr,
            execution_mode: ExecutionMode::from_env(),
            llm: LlmConfig::from_env(),
            clickhouse,
            proposal_ttl: Duration::from_secs(env_parse(
                "HYBRID_ORCHESTRATOR_PROPOSAL_TTL_SECONDS",
                3_600,
            )),
            cleanup_interval: Duration::from_secs(env_parse(
                "HYBRID_ORCHESTRATOR_CLEANUP_INTERVAL_SECONDS",
                60,
            )),
            amqp_max_retry_delay: Duration::from_secs(env_parse(
                "HYBRID_ORCHESTRATOR_AMQP_MAX_RETRY_SECONDS",
                30,
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionMode {
    LogOnly,
}

impl ExecutionMode {
    fn from_env() -> Self {
        match env("HYBRID_ORCHESTRATOR_EXECUTION_MODE")
            .unwrap_or_else(|| "LOG_ONLY".to_owned())
            .trim()
            .to_ascii_uppercase()
            .as_str()
        {
            "LOG_ONLY" => Self::LogOnly,
            unsupported => {
                tracing::warn!(
                    unsupported,
                    "unsupported execution mode requested; falling back to LOG_ONLY"
                );
                Self::LogOnly
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub provider: LlmProvider,
    pub api_key: Option<String>,
    pub model: String,
    pub endpoint: String,
    pub timeout: Duration,
    pub circuit_failure_threshold: u32,
    pub circuit_cooldown: Duration,
    #[allow(dead_code)]
    pub max_markdown_percent: f64,
    pub max_order_quantity: i32,
    pub max_tool_rounds: usize,
    pub tool_call_timeout: Duration,
    pub max_identical_tool_calls: usize,
    pub system_prompt: String,
}

impl LlmConfig {
    pub fn from_env() -> Self {
        let groq_key = env("GROQ_API_KEY");
        let openai_key = env("OPENAI_API_KEY");
        let provider = if groq_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
        {
            LlmProvider::Groq
        } else if openai_key
            .as_deref()
            .is_some_and(|key| !key.trim().is_empty())
        {
            LlmProvider::OpenAi
        } else {
            LlmProvider::Disabled
        };
        let api_key = match provider {
            LlmProvider::Groq => groq_key,
            LlmProvider::OpenAi => openai_key,
            LlmProvider::Disabled => None,
        };
        let model = match provider {
            LlmProvider::Groq => env("GROQ_MODEL").unwrap_or_else(|| "qwen/qwen3.8-27b".to_owned()),
            LlmProvider::OpenAi => env("OPENAI_MODEL").unwrap_or_else(|| "gpt-4o-mini".to_owned()),
            LlmProvider::Disabled => "deterministic-fallback".to_owned(),
        };
        let endpoint = match provider {
            LlmProvider::Groq => env("GROQ_BASE_URL")
                .unwrap_or_else(|| "https://api.groq.com/openai/v1/chat/completions".to_owned()),
            LlmProvider::OpenAi => env("OPENAI_BASE_URL")
                .unwrap_or_else(|| "https://api.openai.com/v1/chat/completions".to_owned()),
            LlmProvider::Disabled => String::new(),
        };

        let system_prompt = if let Some(path) = env("LLM_SYSTEM_PROMPT_FILE") {
            std::fs::read_to_string(&path).unwrap_or_else(|_| default_system_prompt())
        } else {
            env("LLM_SYSTEM_PROMPT").unwrap_or_else(default_system_prompt)
        };

        Self {
            provider,
            api_key,
            model,
            endpoint,
            timeout: Duration::from_millis(env_parse("LLM_TIMEOUT_MILLISECONDS", 30_000)),
            circuit_failure_threshold: env_parse("LLM_CIRCUIT_FAILURE_THRESHOLD", 3),
            circuit_cooldown: Duration::from_secs(env_parse("LLM_CIRCUIT_COOLDOWN_SECONDS", 30)),
            max_markdown_percent: env_parse("LLM_MAX_MARKDOWN_PERCENT", 20.0),
            max_order_quantity: env_parse("LLM_MAX_ORDER_QUANTITY", 5_000),
            max_tool_rounds: env_parse("LLM_MAX_TOOL_ROUNDS", 6),
            tool_call_timeout: Duration::from_secs(env_parse("LLM_TOOL_TIMEOUT_SECONDS", 5)),
            max_identical_tool_calls: env_parse("LLM_MAX_IDENTICAL_TOOL_CALLS", 1),
            system_prompt,
        }
    }

    pub fn configured(&self) -> bool {
        self.provider != LlmProvider::Disabled
            && self
                .api_key
                .as_deref()
                .is_some_and(|key| !key.trim().is_empty())
    }
}

pub fn default_system_prompt() -> String {
    r#"You are the Supply Chain Decision Engine for an e-commerce inventory system.

YOUR ROLE:
You decide whether and how much to reorder for a specific SKU at a warehouse location.
You must base your decision strictly on actual data retrieved through your tools.
Never assume, guess, or use hardcoded formulas.

DECISION GUIDELINES:
1. Start by calling `get_inventory_snapshot` and `get_demand_features` to understand stock position and demand velocities.
2. Call `get_demand_forecast` to evaluate projected demand across lead time and review periods.
3. Call `get_event_calendar` to check for upcoming festive spikes, holidays, or promotional events that may boost demand.
4. Call `get_supplier_signals` to check for active supply-chain bottlenecks, delivery delays, or shortages.
5. If analyzing historical trends, call `get_historical_sales` and pass numeric values to `run_statistical_analysis` for deterministic arithmetic (mean, stddev, trend slope). NEVER calculate math freehand.
6. Evaluate potential replenishment quantities using `simulate_reorder` to see capacity, projected stockout units, and coverage.
7. Synthesize all observations into your final decision.

CRITICAL INSTRUCTIONS:
- If any tool returns status "ERROR" (e.g. database unreachable, network failure, or missing critical data), DO NOT GUESS OR INVENT DATA. You must choose action "REVIEW" with urgency "HIGH" and confidence 0.5 or lower, documenting the tool error in your summary.
- NEVER perform arithmetic on large numbers in text. Always use `run_statistical_analysis`.
- Respond with a JSON object matching this exact schema:
  {
    "action": "REORDER" | "WAIT" | "REVIEW",
    "reorder_quantity": <integer >= 0, 0 if action is WAIT or REVIEW>,
    "urgency": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
    "risk_level": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
    "summary": "<clear 1-3 sentence explanation citing tool data>",
    "key_points": ["<point 1>", "<point 2>"],
    "confidence": <float between 0.0 and 1.0>
  }
"#.to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LlmProvider {
    Groq,
    OpenAi,
    Disabled,
}

fn resolve_amqp_url() -> String {
    env("AMQP_URL")
        .or_else(|| env("ConnectionStrings__eventbus"))
        .or_else(|| env("ConnectionStrings__EventBus"))
        .unwrap_or_else(|| "amqp://127.0.0.1:5672/%2f".to_owned())
}

fn read_from_dotenv(name: &str) -> Option<String> {
    let candidate_paths = [
        std::path::PathBuf::from(".env"),
        std::path::PathBuf::from("src/RustDataPlatform/.env"),
        std::path::PathBuf::from("../.env"),
        std::path::PathBuf::from("../../.env"),
    ];
    for path in &candidate_paths {
        if let Ok(content) = std::fs::read_to_string(path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                if let Some((_, v)) = trimmed.split_once('=').filter(|(k, _)| k.trim() == name) {
                    let val = v.trim().trim_matches('"').trim_matches('\'');
                    if !val.is_empty() {
                        return Some(val.to_owned());
                    }
                }
            }
        }
    }
    None
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| read_from_dotenv(name))
}

fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_llm_is_not_configured_without_key() {
        let config = LlmConfig {
            provider: LlmProvider::Disabled,
            api_key: None,
            model: "fallback".to_owned(),
            endpoint: String::new(),
            timeout: Duration::from_millis(100),
            circuit_failure_threshold: 1,
            circuit_cooldown: Duration::from_secs(1),
            max_markdown_percent: 20.0,
            max_order_quantity: 100,
            max_tool_rounds: 6,
            tool_call_timeout: Duration::from_secs(5),
            max_identical_tool_calls: 1,
            system_prompt: default_system_prompt(),
        };
        assert!(!config.configured());
    }

    #[test]
    fn default_system_prompt_is_non_empty_and_mentions_json() {
        let prompt = default_system_prompt();
        assert!(prompt.contains("JSON"));
        assert!(prompt.contains("REORDER"));
        assert!(prompt.contains("REVIEW"));
    }
}
