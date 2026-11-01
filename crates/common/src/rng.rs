//! Small seeded PRNG (SplitMix64). Every node builds the same synthetic campus
//! from the same seed, so the generator must be ours and stable across
//! platforms and library versions.

#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `per_mille / 1000`.
    pub fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}

/// SplitMix64 finalizer: a fast, well-distributed 64-bit hash.
pub fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a over bytes; used for order-independent table digests.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let a: Vec<u64> = { let mut r = Rng::new(42); (0..8).map(|_| r.next_u64()).collect() };
        let b: Vec<u64> = { let mut r = Rng::new(42); (0..8).map(|_| r.next_u64()).collect() };
        assert_eq!(a, b);
    }

    #[test]
    fn range_is_inclusive_and_bounded() {
        let mut r = Rng::new(1);
        let mut seen = [false; 3];
        for _ in 0..1000 {
            let v = r.range(5, 7);
            assert!((5..=7).contains(&v));
            seen[(v - 5) as usize] = true;
        }
        assert!(seen.iter().all(|s| *s));
    }
}
