//! AppPulse: metrics domain logic for Orbit.
//!
//! Orbit samples the OS (via procfs, WMI, host_statistics — platform layer, gated by
//! the `process_metrics` grant) and feeds `Sample`s into this crate. This crate never
//! reads `/proc`, never invents numbers, and its alert logic is deterministic and
//! fully unit-tested.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppPulseError {
    #[error("buffer capacity must be >= 1")]
    ZeroCapacity,
    #[error("alpha must be in (0, 1], got {0}")]
    BadAlpha(u16),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Milliseconds since Unix epoch (host-provided).
    pub at_ms: u64,
    /// Total CPU usage 0..=100 (synthetic values are fine as inputs, never displayed as real).
    pub cpu_percent: f64,
    /// Total RAM usage 0..=100.
    pub ram_percent: f64,
    pub process_count: u32,
}

/// Capped ring buffer: oldest samples are discarded when full.
#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    capacity: usize,
    buf: VecDeque<T>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Result<Self, AppPulseError> {
        if capacity == 0 {
            return Err(AppPulseError::ZeroCapacity);
        }
        Ok(Self { capacity, buf: VecDeque::with_capacity(capacity) })
    }

    pub fn push(&mut self, value: T) {
        if self.buf.len() == self.capacity {
            self.buf.pop_front();
        }
        self.buf.push_back(value);
    }

    pub fn latest(&self) -> Option<&T> {
        self.buf.back()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.buf.iter()
    }
}

/// Exponential moving average with fixed-point-ish alpha (0,1].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ema {
    alpha: f64,
    value: Option<f64>,
}

impl Ema {
    pub fn new(alpha: f64) -> Result<Self, AppPulseError> {
        if !(alpha > 0.0 && alpha <= 1.0) || alpha.is_nan() {
            return Err(AppPulseError::BadAlpha((alpha * 1000.0) as u16));
        }
        Ok(Self { alpha, value: None })
    }

    pub fn update(&mut self, sample: f64) -> f64 {
        let next = match self.value {
            Some(v) => self.alpha * sample + (1.0 - self.alpha) * v,
            None => sample,
        };
        self.value = Some(next);
        next
    }

    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Alert {
    CpuHigh,
    RamHigh,
}

/// Alert rule: metric must stay above threshold for `consecutive` consecutive samples.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct AlertRule {
    pub cpu_above: f64,
    pub ram_above: f64,
    pub consecutive: u32,
}

impl Default for AlertRule {
    fn default() -> Self {
        Self { cpu_above: 90.0, ram_above: 90.0, consecutive: 3 }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AlertTracker {
    cpu_streak: u32,
    ram_streak: u32,
}

impl AlertTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one sample; returns the alerts that newly fired on this sample.
    pub fn observe(&mut self, rule: &AlertRule, sample: &Sample) -> Vec<Alert> {
        let mut fired = Vec::new();

        self.cpu_streak = if sample.cpu_percent > rule.cpu_above { self.cpu_streak + 1 } else { 0 };
        if self.cpu_streak == rule.consecutive {
            fired.push(Alert::CpuHigh);
        }

        self.ram_streak = if sample.ram_percent > rule.ram_above { self.ram_streak + 1 } else { 0 };
        if self.ram_streak == rule.consecutive {
            fired.push(Alert::RamHigh);
        }

        fired
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(cpu: f64, ram: f64, t: u64) -> Sample {
        Sample { at_ms: t, cpu_percent: cpu, ram_percent: ram, process_count: 100 }
    }

    #[test]
    fn ring_buffer_caps_and_evicts_oldest() {
        let mut rb = RingBuffer::new(3).unwrap();
        for i in 0..5 {
            rb.push(i);
        }
        assert_eq!(rb.len(), 3);
        let got: Vec<_> = rb.iter().copied().collect();
        assert_eq!(got, vec![2, 3, 4]);
    }

    #[test]
    fn ring_buffer_rejects_zero_capacity() {
        assert_eq!(RingBuffer::<u8>::new(0).unwrap_err(), AppPulseError::ZeroCapacity);
    }

    #[test]
    fn ema_smooths_toward_samples() {
        let mut ema = Ema::new(0.5).unwrap();
        assert_eq!(ema.update(100.0), 100.0);
        assert_eq!(ema.update(0.0), 50.0);
        assert_eq!(ema.update(0.0), 25.0);
    }

    #[test]
    fn ema_rejects_bad_alpha() {
        assert!(Ema::new(0.0).is_err());
        assert!(Ema::new(1.5).is_err());
    }

    #[test]
    fn alert_requires_consecutive_streak() {
        let rule = AlertRule { cpu_above: 80.0, ram_above: 90.0, consecutive: 3 };
        let mut tracker = AlertTracker::new();
        assert!(tracker.observe(&rule, &sample(95.0, 10.0, 0)).is_empty());
        assert!(tracker.observe(&rule, &sample(95.0, 10.0, 1)).is_empty());
        assert_eq!(tracker.observe(&rule, &sample(95.0, 10.0, 2)), vec![Alert::CpuHigh]);
        // Fourth sample: no duplicate alert.
        assert!(tracker.observe(&rule, &sample(95.0, 10.0, 3)).is_empty());
    }

    #[test]
    fn dip_resets_streak() {
        let rule = AlertRule { cpu_above: 80.0, ram_above: 90.0, consecutive: 2 };
        let mut tracker = AlertTracker::new();
        tracker.observe(&rule, &sample(95.0, 0.0, 0));
        tracker.observe(&rule, &sample(10.0, 0.0, 1)); // dip breaks streak
        assert!(tracker.observe(&rule, &sample(95.0, 0.0, 2)).is_empty());
    }

    #[test]
    fn ram_alert_independent_of_cpu() {
        let rule = AlertRule { cpu_above: 80.0, ram_above: 80.0, consecutive: 2 };
        let mut tracker = AlertTracker::new();
        tracker.observe(&rule, &sample(0.0, 95.0, 0));
        assert_eq!(tracker.observe(&rule, &sample(0.0, 95.0, 1)), vec![Alert::RamHigh]);
    }
}
