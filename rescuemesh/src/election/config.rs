//! Settings shared by the election components.
//!
//! Defaults are initial values for experiments, not measured tuning results.
//! Configure before constructing the state machines; changing a configuration
//! value afterwards does not change a running machine's timers.

use std::fmt;
use std::time::Duration;

/// Configuration for both election components at application startup.
///
/// ```
/// use rescuemesh::election::ElectionConfig;
/// use std::time::Duration;
///
/// let mut config = ElectionConfig::default();
/// config.worker_selection.bid_timeout = Duration::from_millis(300);
/// config.validate()?;
/// # Ok::<(), rescuemesh::election::ElectionConfigError>(())
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ElectionConfig {
    pub coordinator: CoordinatorConfig,
    pub worker_selection: WorkerSelectionConfig,
}

impl ElectionConfig {
    pub fn validate(&self) -> Result<(), ElectionConfigError> {
        self.coordinator
            .validate()
            .map_err(ElectionConfigError::Coordinator)?;
        self.worker_selection
            .validate()
            .map_err(ElectionConfigError::WorkerSelection)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CoordinatorConfig {
    /// Interval between requests to send leader heartbeats.
    pub heartbeat_interval: Duration,
    /// Lower inclusive bound for randomized election timeouts.
    pub election_timeout_min: Duration,
    /// Upper exclusive bound for randomized election timeouts.
    pub election_timeout_max: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_millis(1000),
        }
    }
}

impl CoordinatorConfig {
    pub fn validate(&self) -> Result<(), CoordinatorConfigError> {
        if self.heartbeat_interval.is_zero()
            || self.heartbeat_interval >= self.election_timeout_min
            || self.election_timeout_min >= self.election_timeout_max
        {
            return Err(CoordinatorConfigError(
                "require 0 < heartbeat < minimum timeout < maximum timeout",
            ));
        }
        // The coordinator samples timeout jitter using u64 nanoseconds.
        if self.election_timeout_max.as_nanos() > u64::MAX as u128 {
            return Err(CoordinatorConfigError("election timeout is too large"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerSelectionConfig {
    /// Maximum time to collect bids for a selection round.
    pub bid_timeout: Duration,
    /// Completed round IDs are retained for deduplication until the term changes.
    /// At capacity, new request IDs are refused rather than forgetting old rounds.
    pub max_tracked_requests: usize,
}

impl Default for WorkerSelectionConfig {
    fn default() -> Self {
        Self {
            bid_timeout: Duration::from_millis(200),
            max_tracked_requests: 100_000,
        }
    }
}

impl WorkerSelectionConfig {
    pub fn validate(&self) -> Result<(), WorkerSelectionConfigError> {
        if self.bid_timeout.is_zero() || self.max_tracked_requests == 0 {
            return Err(WorkerSelectionConfigError(
                "bid timeout and request capacity must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorConfigError(pub &'static str);

impl fmt::Display for CoordinatorConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for CoordinatorConfigError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSelectionConfigError(pub &'static str);

impl fmt::Display for WorkerSelectionConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for WorkerSelectionConfigError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionConfigError {
    Coordinator(CoordinatorConfigError),
    WorkerSelection(WorkerSelectionConfigError),
}

impl fmt::Display for ElectionConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordinator(error) => write!(f, "coordinator configuration: {error}"),
            Self::WorkerSelection(error) => write!(f, "worker selection configuration: {error}"),
        }
    }
}

impl std::error::Error for ElectionConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Coordinator(error) => Some(error),
            Self::WorkerSelection(error) => Some(error),
        }
    }
}
