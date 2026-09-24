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
    pub amqp_prefetch: u16,
    pub max_concurrent_llm: usize,
    pub macro_cache_ttl: Duration,
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
            amqp_prefetch: env_parse("HYBRID_ORCHESTRATOR_AMQP_PREFETCH", 100),
            max_concurrent_llm: env_parse("LLM_MAX_CONCURRENT", 20),
            macro_cache_ttl: Duration::from_secs(env_parse("MACRO_CACHE_TTL_SECONDS", 60)),
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
    pub groq_tpm_budget: u64,
    pub estimate_per_call: u64,
    pub queue_max_depth: usize,
    pub system_prompt: String,
}

impl LlmConfig {
    pub fn from_env() -> Self {
        let requested_provider = env("LLM_PROVIDER").map(|s| s.trim().to_ascii_lowercase());
        let gemini_key = env("GEMINI_API_KEY");
        let groq_key = env("GROQ_API_KEY");
        let openai_key = env("OPENAI_API_KEY");

        let provider = match requested_provider.as_deref() {
            Some("gemini") => LlmProvider::Gemini,
            Some("groq") => LlmProvider::Groq,
            Some("openai") => LlmProvider::OpenAi,
            Some("disabled") => LlmProvider::Disabled,
            _ => {
                if gemini_key.as_deref().is_some_and(|k| !k.trim().is_empty()) {
                    LlmProvider::Gemini
                } else if groq_key.as_deref().is_some_and(|k| !k.trim().is_empty()) {
                    LlmProvider::Groq
                } else if openai_key.as_deref().is_some_and(|k| !k.trim().is_empty()) {
                    LlmProvider::OpenAi
                } else {
                    LlmProvider::Disabled
                }
            }
        };

        let api_key = match provider {
            LlmProvider::Gemini => gemini_key,
            LlmProvider::Groq => groq_key,
            LlmProvider::OpenAi => openai_key,
            LlmProvider::Disabled => None,
        };

        let model = match provider {
            LlmProvider::Gemini => env("GEMINI_MODEL")
                .map(|m| {
                    let trimmed = m.trim();
                    if trimmed == "gemini-2.5-flash" || trimmed == "models/gemini-2.5-flash" {
                        "gemini-3.6-flash".to_owned()
                    } else if trimmed == "gemini-2.5-flash-lite"
                        || trimmed == "models/gemini-2.5-flash-lite"
                    {
                        "gemini-3.1-flash-lite".to_owned()
                    } else {
                        trimmed.to_owned()
                    }
                })
                .unwrap_or_else(|| "gemini-3.6-flash".to_owned()),
            LlmProvider::Groq => {
                env("GROQ_MODEL").unwrap_or_else(|| "openai/gpt-oss-20b".to_owned())
            }
            LlmProvider::OpenAi => env("OPENAI_MODEL").unwrap_or_else(|| "gpt-4o-mini".to_owned()),
            LlmProvider::Disabled => "deterministic-fallback".to_owned(),
        };

        let endpoint = match provider {
            LlmProvider::Gemini => env("GEMINI_BASE_URL").unwrap_or_else(|| {
                "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
                    .to_owned()
            }),
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
            groq_tpm_budget: env_parse("GROQ_TPM_BUDGET", 8_000),
            estimate_per_call: env_parse("GROQ_TPM_ESTIMATE_PER_CALL", 3_500),
            queue_max_depth: env_parse("LLM_QUEUE_MAX_DEPTH", 200),
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

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            provider: LlmProvider::Disabled,
            api_key: None,
            model: "fallback".to_owned(),
            endpoint: String::new(),
            timeout: Duration::from_millis(30_000),
            circuit_failure_threshold: 3,
            circuit_cooldown: Duration::from_secs(30),
            max_markdown_percent: 20.0,
            max_order_quantity: 5_000,
            max_tool_rounds: 6,
            tool_call_timeout: Duration::from_secs(5),
            max_identical_tool_calls: 1,
            groq_tpm_budget: 8_000,
            estimate_per_call: 3_500,
            queue_max_depth: 200,
            system_prompt: default_system_prompt(),
        }
    }
}

pub fn default_system_prompt() -> String {
    r#"You are the Supply Chain Decision Engine for an e-commerce inventory system.

YOUR ROLE:
You decide whether and how much to reorder for a specific SKU at a warehouse location.
You must base your decision strictly on actual data retrieved through your tools.
Never assume, guess, or use hardcoded formulas.

MANDATORY INVESTIGATION SEQUENCE:
Before synthesizing your final decision, you MUST query the following core intelligence sources:
1. `get_inventory_snapshot` and `get_demand_features`: Check current stock position (on_hand, reserved, available, safety_stock) and velocity.
2. `get_demand_forecast`: Evaluate projected baseline and trend demand across lead time and review periods.
3. `get_event_calendar`: REQUIRED. Check for upcoming Indian festivals (e.g. Diwali, Dussehra, Ganesh Chaturthi, Great Indian Festival) or regional sales spikes.
4. `get_supplier_signals`: REQUIRED. Check for active supply-chain bottlenecks, freight transit delays, port congestion, or factory shortages.
5. (Optional) `simulate_reorder`: Test candidate replenishment quantities against warehouse capacity and projected stockout units.
6. (Optional) If analyzing raw historical data, call `run_statistical_analysis`. NEVER perform mental math on large numbers.

DO NOT emit your final JSON decision until you have queried both `get_event_calendar` AND `get_supplier_signals`.

RISK LEVEL AND URGENCY ASSIGNMENT RULES:
- ACTIVE STOCKOUT / DEPLETION: If `on_hand <= 0` or `available <= 0`, risk_level MUST NEVER be 'LOW'. A stockout is an active operational failure and MUST be classified as at least 'MEDIUM', 'HIGH', or 'CRITICAL'.
- FESTIVE SURGE OR SUPPLIER BOTTLENECK: If upcoming high/medium-impact festive events or active supplier disruptions are detected, set risk_level to 'HIGH' or 'CRITICAL' with urgency 'HIGH'.
- LOW RISK: Can ONLY be assigned when on_hand stock is healthy (well above reorder_point and safety_stock) and no festive spikes or supplier bottlenecks are active.

CRITICAL INSTRUCTIONS:
- If any tool returns status "ERROR" (e.g. database unreachable, network failure, or missing critical data), DO NOT GUESS OR INVENT DATA. You must choose action "REVIEW" with urgency "HIGH" and confidence 0.5 or lower, documenting the tool error in your summary.
- NEVER perform arithmetic on large numbers in text. Always use `run_statistical_analysis`.
- Respond with a JSON object matching this exact schema:
  {
    "action": "REORDER" | "WAIT" | "REVIEW",
    "reorder_quantity": <integer >= 0, 0 if action is WAIT or REVIEW>,
    "urgency": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
    "risk_level": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
    "summary": "<clear 1-3 sentence explanation citing tool data including event calendar and supplier signals>",
    "key_points": ["<point 1>", "<point 2>"],
    "confidence": <float between 0.0 and 1.0>
  }
"#.to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LlmProvider {
    Gemini,
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
            ..LlmConfig::default()
        };
        assert!(!config.configured());
    }

    #[test]
    fn default_system_prompt_is_non_empty_and_mentions_json() {
        let prompt = default_system_prompt();
        assert!(prompt.contains("JSON"));
        assert!(prompt.contains("REORDER"));
        assert!(prompt.contains("REVIEW"));
        assert!(prompt.contains("get_event_calendar"));
        assert!(prompt.contains("get_supplier_signals"));
        assert!(prompt.contains("ACTIVE STOCKOUT"));
    }

    #[test]
    fn gemini_provider_is_configured_with_key() {
        let config = LlmConfig {
            provider: LlmProvider::Gemini,
            api_key: Some("dummy-gemini-key".to_owned()),
            model: "gemini-3.6-flash".to_owned(),
            endpoint: "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
                .to_owned(),
            timeout: Duration::from_secs(10),
            circuit_failure_threshold: 3,
            circuit_cooldown: Duration::from_secs(30),
            max_markdown_percent: 20.0,
            max_order_quantity: 500,
            max_tool_rounds: 6,
            tool_call_timeout: Duration::from_secs(5),
            max_identical_tool_calls: 1,
            system_prompt: default_system_prompt(),
            ..LlmConfig::default()
        };
        assert!(config.configured());
        assert_eq!(config.provider, LlmProvider::Gemini);
        assert_eq!(config.model, "gemini-3.6-flash");
    }

    #[test]
    fn groq_provider_is_configured_with_key() {
        let config = LlmConfig {
            provider: LlmProvider::Groq,
            api_key: Some("dummy-groq-key".to_owned()),
            model: "openai/gpt-oss-20b".to_owned(),
            endpoint: "https://api.groq.com/openai/v1/chat/completions".to_owned(),
            timeout: Duration::from_secs(10),
            circuit_failure_threshold: 3,
            circuit_cooldown: Duration::from_secs(30),
            max_markdown_percent: 20.0,
            max_order_quantity: 500,
            max_tool_rounds: 6,
            tool_call_timeout: Duration::from_secs(5),
            max_identical_tool_calls: 1,
            system_prompt: default_system_prompt(),
            ..LlmConfig::default()
        };
        assert!(config.configured());
        assert_eq!(config.provider, LlmProvider::Groq);
        assert_eq!(config.model, "openai/gpt-oss-20b");
    }

    #[test]
    fn from_env_loads_groq_when_set() {
        let config = LlmConfig::from_env();
        assert_eq!(config.provider, LlmProvider::Groq);
        assert_eq!(config.model, "openai/gpt-oss-20b");
        assert!(config.configured());
    }
}
