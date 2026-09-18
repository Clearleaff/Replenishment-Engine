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
    pub proposal_ttl: Duration,
    pub cleanup_interval: Duration,
    pub amqp_max_retry_delay: Duration,
}

impl OrchestratorConfig {
    pub fn from_env() -> Self {
        let bind_addr = env("HYBRID_ORCHESTRATOR_BIND")
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000));

        Self {
            amqp_url: resolve_amqp_url(),
            queue_name: env("HYBRID_ORCHESTRATOR_QUEUE")
                .unwrap_or_else(|| "eshop.inventory.order_stock_confirmed".to_owned()),
            consumer_tag: env("HYBRID_ORCHESTRATOR_CONSUMER_TAG")
                .unwrap_or_else(|| "hybrid-orchestrator".to_owned()),
            bind_addr,
            execution_mode: ExecutionMode::from_env(),
            llm: LlmConfig::from_env(),
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
    pub max_markdown_percent: f64,
    pub max_order_quantity: i32,
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
            LlmProvider::Groq => {
                env("GROQ_MODEL").unwrap_or_else(|| "llama-3.3-70b-versatile".to_owned())
            }
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

        Self {
            provider,
            api_key,
            model,
            endpoint,
            timeout: Duration::from_millis(env_parse("LLM_TIMEOUT_MILLISECONDS", 3_500)),
            circuit_failure_threshold: env_parse("LLM_CIRCUIT_FAILURE_THRESHOLD", 3),
            circuit_cooldown: Duration::from_secs(env_parse("LLM_CIRCUIT_COOLDOWN_SECONDS", 30)),
            max_markdown_percent: env_parse("LLM_MAX_MARKDOWN_PERCENT", 20.0),
            max_order_quantity: env_parse("LLM_MAX_ORDER_QUANTITY", 5_000),
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

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
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
        };
        assert!(!config.configured());
    }
}
