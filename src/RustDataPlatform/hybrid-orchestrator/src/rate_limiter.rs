use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimiterError {
    QueueFull,
}

impl std::fmt::Display for RateLimiterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "LLM rate limiter queue is full"),
        }
    }
}

impl std::error::Error for RateLimiterError {}

#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    pub tpm_budget: u64,
    pub estimate_per_call: u64,
    pub max_queue_depth: usize,
    pub window: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            tpm_budget: 8_000,
            estimate_per_call: 3_500,
            max_queue_depth: 200,
            window: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RateLimiterStatus {
    pub tpm_budget: u64,
    pub current_tpm_usage: u64,
    pub queue_depth: usize,
    pub queue_max_depth: usize,
    pub estimate_per_call: u64,
}

#[derive(Debug)]
struct LimiterState {
    history: VecDeque<(Instant, u64)>,
    waiting: usize,
}

#[derive(Debug)]
pub struct TokenBucketRateLimiter {
    config: RateLimiterConfig,
    state: Arc<Mutex<LimiterState>>,
}

impl TokenBucketRateLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(LimiterState {
                history: VecDeque::new(),
                waiting: 0,
            })),
        }
    }

    pub fn config(&self) -> &RateLimiterConfig {
        &self.config
    }

    pub async fn status(&self) -> RateLimiterStatus {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        Self::prune_expired(&mut state.history, now, self.config.window);
        let current_tpm_usage: u64 = state.history.iter().map(|(_, t)| *t).sum();
        RateLimiterStatus {
            tpm_budget: self.config.tpm_budget,
            current_tpm_usage,
            queue_depth: state.waiting,
            queue_max_depth: self.config.max_queue_depth,
            estimate_per_call: self.config.estimate_per_call,
        }
    }

    /// Acquires permission to consume estimated tokens for an LLM call.
    ///
    /// If budget is available and no callers are waiting, proceeds immediately.
    /// If budget is exhausted, waits asynchronously until tokens expire from the rolling window.
    /// If the wait queue depth exceeds `max_queue_depth`, returns `Err(RateLimiterError::QueueFull)`.
    pub async fn acquire(&self) -> Result<(), RateLimiterError> {
        // If budget is 0 (unlimited / bypass mode), admit immediately
        if self.config.tpm_budget == 0 {
            return Ok(());
        }

        let cost = self.config.estimate_per_call.min(self.config.tpm_budget);
        let mut was_waiting = false;

        loop {
            let sleep_duration = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                Self::prune_expired(&mut state.history, now, self.config.window);
                let current_usage: u64 = state.history.iter().map(|(_, t)| *t).sum();

                // If budget is available and this task is either first in line or the only waiter
                if current_usage + cost <= self.config.tpm_budget && state.waiting == 0 {
                // If budget is available:
                // - A fresh caller can acquire immediately only if no other tasks are waiting (preserves fairness).
                // - A caller that has already waited (was_waiting == true) can acquire as soon as budget is free.
                if current_usage + cost <= self.config.tpm_budget && (state.waiting == 0 || was_waiting) {
                    state.history.push_back((now, cost));
                    return Ok(());
                }

                // If we need to wait, check queue depth first
                if state.waiting >= self.config.max_queue_depth {
                // If we need to wait, check queue depth first (only for fresh tasks joining the queue)
                if !was_waiting && state.waiting >= self.config.max_queue_depth {
                    return Err(RateLimiterError::QueueFull);
                }

                // Calculate required wait time until sufficient tokens expire
                let needed_reduction = (current_usage + cost).saturating_sub(self.config.tpm_budget);
                let mut accumulated = 0u64;
                let mut expire_target = now + self.config.window;

                for (ts, tokens) in state.history.iter() {
                    accumulated += tokens;
                    if accumulated >= needed_reduction {
                        expire_target = *ts + self.config.window;
                        break;
                    }
                }

                let wait = expire_target.saturating_duration_since(now);
                state.waiting += 1;
                wait.max(Duration::from_millis(1))
            };

            was_waiting = true;
            debug!(?sleep_duration, "rate limiter waiting for token budget to free up");
            tokio::time::sleep(sleep_duration).await;

            // Decrement waiting count after sleep
            {
                let mut state = self.state.lock().await;
                state.waiting = state.waiting.saturating_sub(1);
            }
        }
    }

    fn prune_expired(history: &mut VecDeque<(Instant, u64)>, now: Instant, window: Duration) {
        while let Some((ts, _)) = history.front() {
            if now.saturating_duration_since(*ts) >= window {
                history.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limiter_permits_immediately_when_budget_available() {
        let config = RateLimiterConfig {
            tpm_budget: 8_000,
            estimate_per_call: 3_500,
            max_queue_depth: 10,
            window: Duration::from_secs(60),
        };
        let limiter = TokenBucketRateLimiter::new(config);

        let t0 = Instant::now();
        limiter.acquire().await.unwrap();
        limiter.acquire().await.unwrap();
        assert!(t0.elapsed() < Duration::from_millis(50));

        let status = limiter.status().await;
        assert_eq!(status.current_tpm_usage, 7_000);
        assert_eq!(status.queue_depth, 0);
    }

    #[tokio::test]
    async fn limiter_blocks_when_budget_exhausted_and_resumes_after_window() {
        let config = RateLimiterConfig {
            tpm_budget: 8_000,
            estimate_per_call: 5_000,
            max_queue_depth: 10,
            window: Duration::from_millis(60), // Scaled down for fast test execution
        };
        let limiter = TokenBucketRateLimiter::new(config);

        // Call 1 consumes 5,000 tokens
        limiter.acquire().await.unwrap();
        assert_eq!(limiter.status().await.current_tpm_usage, 5_000);

        // Call 2 needs 5,000 tokens: 5,000 + 5,000 = 10,000 > 8,000 -> must wait ~60ms
        let t0 = Instant::now();
        limiter.acquire().await.unwrap();
        let elapsed = t0.elapsed();

        assert!(
            elapsed >= Duration::from_millis(50),
            "expected limiter to wait at least ~50ms, elapsed: {:?}",
            elapsed
        );
        assert_eq!(limiter.status().await.current_tpm_usage, 5_000);
    }

    #[tokio::test]
    async fn queue_overflow_drops_event_when_queue_depth_exceeded() {
        let config = RateLimiterConfig {
            tpm_budget: 8_000,
            estimate_per_call: 5_000,
            max_queue_depth: 1, // Only 1 waiter allowed
            window: Duration::from_secs(10),
        };
        let limiter = Arc::new(TokenBucketRateLimiter::new(config));

        // Call 1 uses 5,000 tokens
        limiter.acquire().await.unwrap();

        // Spawn Call 2 which will wait (queue depth = 1)
        let limiter2 = limiter.clone();
        let handle = tokio::spawn(async move {
            limiter2.acquire().await
        });

        // Give Call 2 a few millis to enter wait state
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(limiter.status().await.queue_depth, 1);

        // Call 3 arrives while queue depth is at max (1) -> must be rejected immediately
        let res = limiter.acquire().await;
        assert_eq!(res, Err(RateLimiterError::QueueFull));

        // Clean up spawned task
        handle.abort();
    }

    #[tokio::test]
    async fn multiple_waiters_progress_sequentially_without_livelock() {
        let config = RateLimiterConfig {
            tpm_budget: 8_000,
            estimate_per_call: 5_000,
            max_queue_depth: 5,
            window: Duration::from_millis(60), // Scaled down for test
        };
        let limiter = Arc::new(TokenBucketRateLimiter::new(config));

        // Call 1 consumes 5,000 tokens immediately
        limiter.acquire().await.unwrap();

        // Spawn Call 2 and Call 3, both will queue up
        let l2 = limiter.clone();
        let h2 = tokio::spawn(async move { l2.acquire().await });

        let l3 = limiter.clone();
        let h3 = tokio::spawn(async move { l3.acquire().await });

        // Both handles must complete successfully without livelocking
        let res2 = tokio::time::timeout(Duration::from_millis(500), h2).await;
        let res3 = tokio::time::timeout(Duration::from_millis(500), h3).await;

        assert!(res2.is_ok(), "Call 2 should complete without timing out");
        assert!(res2.unwrap().unwrap().is_ok());
        assert!(res3.is_ok(), "Call 3 should complete without timing out");
        assert!(res3.unwrap().unwrap().is_ok());
    }
}

