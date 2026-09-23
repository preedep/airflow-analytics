/// Linear-interpolated percentile over an ascending slice; `q` in 0..=1.
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    match sorted.len() {
        0 => 0.0,
        1 => sorted[0],
        n => {
            let rank = q.clamp(0.0, 1.0) * (n - 1) as f64;
            let lower = rank.floor() as usize;
            let upper = rank.ceil() as usize;
            sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
        }
    }
}

pub fn sorted_ascending(values: impl IntoIterator<Item = f64>) -> Vec<f64> {
    let mut values: Vec<f64> = values.into_iter().collect();
    values.sort_by(f64::total_cmp);
    values
}

pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_interpolates() {
        let v = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(percentile(&v, 0.0), 1.0);
        assert_eq!(percentile(&v, 0.5), 2.5);
        assert_eq!(percentile(&v, 1.0), 4.0);
        assert_eq!(percentile(&[], 0.5), 0.0);
    }
}
