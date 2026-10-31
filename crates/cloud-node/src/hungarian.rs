//! Hungarian algorithm (Kuhn–Munkres with potentials), O(n²·m), for the
//! rectangular minimum-cost assignment problem with n rows ≤ m columns.

const INF: i64 = i64::MAX / 4;

/// Assigns every row to a distinct column minimizing the total cost.
/// Returns `(col_of_row, total)`. Panics if rows > columns or rows are ragged.
pub fn solve(cost: &[Vec<i64>]) -> (Vec<usize>, i64) {
    let n = cost.len();
    if n == 0 {
        return (Vec::new(), 0);
    }
    let m = cost[0].len();
    assert!(n <= m, "hungarian: {n} rows > {m} columns");
    assert!(
        cost.iter().all(|r| r.len() == m),
        "hungarian: ragged matrix"
    );

    // 1-based: u[i] row potentials, v[j] column potentials, p[j] = row matched
    // to column j (0 = none), way[j] = previous column on the augmenting path.
    let mut u = vec![0i64; n + 1];
    let mut v = vec![0i64; m + 1];
    let mut p = vec![0usize; m + 1];
    let mut way = vec![0usize; m + 1];

    for i in 1..=n {
        p[0] = i;
        let mut j0 = 0usize;
        let mut minv = vec![INF; m + 1];
        let mut used = vec![false; m + 1];
        loop {
            used[j0] = true;
            let i0 = p[j0];
            let mut delta = INF;
            let mut j1 = 0usize;
            for j in 1..=m {
                if used[j] {
                    continue;
                }
                let cur = cost[i0 - 1][j - 1] - u[i0] - v[j];
                if cur < minv[j] {
                    minv[j] = cur;
                    way[j] = j0;
                }
                if minv[j] < delta {
                    delta = minv[j];
                    j1 = j;
                }
            }
            for j in 0..=m {
                if used[j] {
                    u[p[j]] += delta;
                    v[j] -= delta;
                } else {
                    minv[j] -= delta;
                }
            }
            j0 = j1;
            if p[j0] == 0 {
                break;
            }
        }
        // Flip the augmenting path.
        loop {
            let j1 = way[j0];
            p[j0] = p[j1];
            j0 = j1;
            if j0 == 0 {
                break;
            }
        }
    }

    let mut col_of_row = vec![0usize; n];
    for j in 1..=m {
        if p[j] != 0 {
            col_of_row[p[j] - 1] = j - 1;
        }
    }
    let total = col_of_row
        .iter()
        .enumerate()
        .map(|(i, &j)| cost[i][j])
        .sum();
    (col_of_row, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::rng::Rng;

    /// Minimum over every injective row → column map.
    fn brute_force(cost: &[Vec<i64>]) -> i64 {
        fn go(cost: &[Vec<i64>], row: usize, used: &mut Vec<bool>) -> i64 {
            if row == cost.len() {
                return 0;
            }
            let mut best = INF;
            for j in 0..used.len() {
                if !used[j] {
                    used[j] = true;
                    best = best.min(cost[row][j] + go(cost, row + 1, used));
                    used[j] = false;
                }
            }
            best
        }
        let m = cost.first().map_or(0, |r| r.len());
        go(cost, 0, &mut vec![false; m])
    }

    #[test]
    fn matches_brute_force_on_small_inputs() {
        let mut rng = Rng::new(2024);
        for _ in 0..2000 {
            let n = rng.range(1, 5) as usize;
            let m = rng.range(n as u64, 6) as usize;
            let cost: Vec<Vec<i64>> = (0..n)
                .map(|_| (0..m).map(|_| rng.range(0, 40) as i64 - 10).collect())
                .collect();
            let (cols, total) = solve(&cost);
            let mut seen = cols.clone();
            seen.sort();
            seen.dedup();
            assert_eq!(seen.len(), n, "columns must be distinct: {cols:?}");
            assert_eq!(total, brute_force(&cost), "matrix {cost:?}");
        }
    }

    #[test]
    fn known_square_case() {
        let cost = vec![vec![4, 1, 3], vec![2, 0, 5], vec![3, 2, 2]];
        assert_eq!(solve(&cost), (vec![1, 0, 2], 5));
    }

    #[test]
    fn empty() {
        assert_eq!(solve(&[]), (vec![], 0));
    }
}
