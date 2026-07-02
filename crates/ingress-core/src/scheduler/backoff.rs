//! Retry backoff math. Pure and deterministic — no jitter (single client,
//! no thundering herd; deterministic tests).

use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    pub base: Duration,
    pub max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(30),
            max: Duration::from_secs(3600),
        }
    }
}

/// Delay before the `n`th retry (n = retry_count after the bump, 1-based):
/// `min(base × 2^(n−1), max)`.
pub fn delay(config: &BackoffConfig, n: i64) -> Duration {
    let exp = (n - 1).clamp(0, 62) as u32;
    config
        .base
        .checked_mul(1u32 << exp.min(31))
        .unwrap_or(config.max)
        .min(config.max)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: wrong backoff math either hammers iCloud on persistent
    // failures or stalls recovery for hours after a blip.
    // Should: double per retry from base and clamp at max.
    #[test]
    fn exponential_with_cap() {
        let cfg = BackoffConfig {
            base: Duration::from_secs(30),
            max: Duration::from_secs(3600),
        };
        assert_eq!(delay(&cfg, 1), Duration::from_secs(30));
        assert_eq!(delay(&cfg, 2), Duration::from_secs(60));
        assert_eq!(delay(&cfg, 3), Duration::from_secs(120));
        assert_eq!(delay(&cfg, 7), Duration::from_secs(1920));
        assert_eq!(delay(&cfg, 8), Duration::from_secs(3600)); // clamped
        assert_eq!(delay(&cfg, 60), Duration::from_secs(3600));
    }
}
