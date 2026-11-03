//! Summary statistics for the stress harness: percentiles, sample mean and
//! standard deviation, Student-t 95 % confidence intervals over repeated
//! runs, and Jain's fairness index. NaN marks "no value" (an empty set, or a
//! metric that does not apply to a run) and is skipped by `ci95`.

/// Nearest-rank percentile of an ascending slice, `p` in [0, 1]: index
/// round((n − 1)·p), the same rule as the client's run summary. NaN if empty.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)).round() as usize]
}

/// NaN if empty.
pub fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        f64::NAN
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// Sample standard deviation (n − 1 denominator); 0 for fewer than 2 values.
pub fn std_dev(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let m = mean(xs);
    (xs.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
}

/// Two-sided 95 % Student-t critical value for `df` degrees of freedom. Past
/// 30 it uses the next lower tabulated df, so the interval errs wide.
pub fn t95(df: usize) -> f64 {
    const T: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    match df {
        0 => f64::NAN,
        1..=30 => T[df - 1],
        31..=39 => 2.042,
        40..=59 => 2.021,
        60..=119 => 2.000,
        _ => 1.980,
    }
}

/// Mean of repeated measurements with the half-width of its 95 % confidence
/// interval (Student t, n − 1 df). `half` is NaN when n < 2.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Ci {
    pub mean: f64,
    pub half: f64,
    pub n: usize,
}

/// Skips NaN values (runs where the metric does not apply).
pub fn ci95(xs: &[f64]) -> Ci {
    let v: Vec<f64> = xs.iter().copied().filter(|x| !x.is_nan()).collect();
    let n = v.len();
    let half = if n < 2 {
        f64::NAN
    } else {
        t95(n - 1) * std_dev(&v) / (n as f64).sqrt()
    };
    Ci {
        mean: mean(&v),
        half,
        n,
    }
}

/// Jain's fairness index (Σx)² / (n·Σx²): 1 when all shares are equal, 1/n
/// when one gets everything. NaN if empty or all zero.
pub fn jain(xs: &[f64]) -> f64 {
    let sum: f64 = xs.iter().sum();
    let sq: f64 = xs.iter().map(|x| x * x).sum();
    if xs.is_empty() || sq == 0.0 {
        f64::NAN
    } else {
        sum * sum / (xs.len() as f64 * sq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_nearest_rank() {
        let xs: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&xs, 0.0), 1.0);
        assert_eq!(percentile(&xs, 0.5), 51.0); // round(99 · 0.5) = 50 → 51
        assert_eq!(percentile(&xs, 0.99), 99.0);
        assert_eq!(percentile(&xs, 1.0), 100.0);
        assert!(percentile(&[], 0.5).is_nan());
    }

    #[test]
    fn mean_and_sample_std_dev() {
        let xs = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        assert_eq!(mean(&xs), 5.0);
        assert!((std_dev(&xs) - 2.138_089_935).abs() < 1e-9);
        assert_eq!(std_dev(&[3.0]), 0.0);
        assert!(mean(&[]).is_nan());
    }

    #[test]
    fn ci_matches_hand_computation_and_skips_nan() {
        // n = 5, mean 10, s = sqrt(2.5): half = 2.776 · 1.5811 / 2.2361 = 1.963
        let ci = ci95(&[8.0, 9.0, 10.0, 11.0, 12.0, f64::NAN]);
        assert_eq!(ci.n, 5);
        assert_eq!(ci.mean, 10.0);
        assert!((ci.half - 1.963).abs() < 1e-3, "{}", ci.half);
        let one = ci95(&[4.0]);
        assert_eq!((one.mean, one.n), (4.0, 1));
        assert!(one.half.is_nan());
        assert!(ci95(&[f64::NAN]).mean.is_nan());
    }

    #[test]
    fn t_values_shrink_with_df_and_err_wide() {
        assert_eq!(t95(1), 12.706);
        assert_eq!(t95(4), 2.776);
        assert_eq!(t95(30), 2.042);
        assert_eq!(t95(35), 2.042);
        assert_eq!(t95(1000), 1.980);
        assert!(t95(0).is_nan());
        for df in 1..200 {
            assert!(t95(df + 1) <= t95(df));
        }
    }

    #[test]
    fn jain_is_one_when_equal_and_one_over_n_when_one_takes_all() {
        assert!((jain(&[5.0, 5.0, 5.0]) - 1.0).abs() < 1e-12);
        assert!((jain(&[9.0, 0.0, 0.0]) - 1.0 / 3.0).abs() < 1e-12);
        assert!(jain(&[]).is_nan());
        assert!(jain(&[0.0, 0.0]).is_nan());
    }
}
