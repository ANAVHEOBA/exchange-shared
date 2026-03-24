use statrs::distribution::{Continuous, ContinuousCDF, LogNormal};
use std::time::Duration;

/// Mathematical strategy for optimal polling based on QCD (Quickest Change Detection)
/// and Hazard Rate modeling.
///
/// This implementation follows the optimal control law:
/// τ ≈ sqrt( 2 * Cp / (Cd * λ(t)) )
/// Where:
/// - Cp is Polling Cost
/// - Cd is Delay Cost
/// - λ(t) is the Hazard Rate of the completion distribution
pub struct PollingStrategy {
    /// Cost of a single API poll (normalized)
    pub cost_per_poll: f64,
    /// Cost of information delay (normalized per second)
    pub cost_per_delay_sec: f64,
}

impl PollingStrategy {
    pub fn new(cost_per_poll: f64, cost_per_delay_sec: f64) -> Self {
        Self {
            cost_per_poll,
            cost_per_delay_sec,
        }
    }

    /// Calculate the next optimal polling interval using the Hazard Rate.
    /// Crypto swaps completion times are best modeled by a LogNormal distribution
    /// due to non-zero minimum confirmation times and a long tail.
    pub fn calculate_next_interval(&self, elapsed_secs: u64) -> Duration {
        // Typical swap parameters: Median ~10m (600s), Moderate variance
        // mu = ln(median), sigma affects the "fatness" of the tail
        let mu = 6.4;
        let sigma = 0.5;

        let dist = LogNormal::new(mu, sigma).unwrap();

        // Ensure t is at least 1.0 to avoid log(0) or div by zero in PDF/CDF
        let t = (elapsed_secs as f64).max(1.0);

        // 1. PDF f(t)
        let pdf = dist.pdf(t);
        // 2. CDF F(t)
        let cdf = dist.cdf(t);

        // 3. Hazard Rate λ(t) = f(t) / (1 - F(t))
        // Represents the "urgency" of polling right now.
        let survivor = 1.0 - cdf;
        let hazard_rate = if survivor < 1e-7 {
            // If we are deep in the tail, the probability of finishing
            // in the next second is low but non-zero.
            1e-7
        } else {
            pdf / survivor
        };

        // 4. Solve Optimal Control Equation:
        // τ = sqrt( (2 * Cp) / (Cd * λ(t)) )
        let optimal_tau =
            ((2.0 * self.cost_per_poll) / (self.cost_per_delay_sec * hazard_rate)).sqrt();

        // 5. Constraints and Clamping
        // Min 5s (API protection), Max 1 hour (Safety bound)
        let final_secs = optimal_tau.clamp(5.0, 3600.0) as u64;

        Duration::from_secs(final_secs)
    }

    /// Estimate the posterior probability that the swap has finished
    /// given it had not finished at time t.
    pub fn probability_due(&self, elapsed_secs: u64, window_secs: u64) -> f64 {
        let mu = 6.4;
        let sigma = 0.5;
        let dist = LogNormal::new(mu, sigma).unwrap();

        let t = elapsed_secs as f64;
        let t_next = t + window_secs as f64;

        let f_t = dist.cdf(t);
        let f_next = dist.cdf(t_next);

        if f_t >= 1.0 {
            return 0.0;
        }

        // P(T < t+Δ | T > t) = [F(t+Δ) - F(t)] / [1 - F(t)]
        (f_next - f_t) / (1.0 - f_t)
    }
}
