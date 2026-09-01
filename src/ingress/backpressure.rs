#[derive(Clone, Copy, Debug)]
pub(crate) struct BackpressurePolicy {
    pause_high_percent: u8,
    resume_low_percent: u8,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BudgetUsage {
    queue_used: usize,
    queue_total: usize,
    bytes_used: usize,
    bytes_total: usize,
}

impl BackpressurePolicy {
    pub(crate) fn new(pause_high_percent: u8, resume_low_percent: u8) -> Self {
        Self {
            pause_high_percent,
            resume_low_percent,
        }
    }

    pub(crate) fn should_pause(self, usage: BudgetUsage) -> bool {
        reached_percent(usage.queue_used, usage.queue_total, self.pause_high_percent)
            || reached_percent(usage.bytes_used, usage.bytes_total, self.pause_high_percent)
    }

    pub(crate) fn should_resume(self, usage: BudgetUsage) -> bool {
        at_or_below_percent(usage.queue_used, usage.queue_total, self.resume_low_percent)
            && at_or_below_percent(usage.bytes_used, usage.bytes_total, self.resume_low_percent)
    }
}

impl BudgetUsage {
    pub(crate) fn new(
        queue_used: usize,
        queue_total: usize,
        bytes_used: usize,
        bytes_total: usize,
    ) -> Self {
        Self {
            queue_used,
            queue_total,
            bytes_used,
            bytes_total,
        }
    }

    pub(crate) fn queue_used(self) -> usize {
        self.queue_used
    }

    pub(crate) fn queue_total(self) -> usize {
        self.queue_total
    }

    pub(crate) fn bytes_used(self) -> usize {
        self.bytes_used
    }

    pub(crate) fn bytes_total(self) -> usize {
        self.bytes_total
    }
}

fn reached_percent(used: usize, total: usize, percent: u8) -> bool {
    (used as u128) * 100 >= (total as u128) * u128::from(percent)
}

fn at_or_below_percent(used: usize, total: usize, percent: u8) -> bool {
    (used as u128) * 100 <= (total as u128) * u128::from(percent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_high_low_hysteresis_across_both_budgets() {
        let policy = BackpressurePolicy::new(80, 50);

        assert!(policy.should_pause(BudgetUsage::new(8, 10, 1, 10)));
        assert!(policy.should_pause(BudgetUsage::new(1, 10, 8, 10)));
        assert!(!policy.should_resume(BudgetUsage::new(5, 10, 6, 10)));
        assert!(policy.should_resume(BudgetUsage::new(5, 10, 5, 10)));
    }
}
