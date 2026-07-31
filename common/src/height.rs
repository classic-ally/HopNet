//! Consensus-height ⇄ SQLite mapping.
//!
//! Heights are u64 end-to-end (matching the consensus engine); SQLite's
//! INTEGER is a signed i64. The mapping is a bit cast in both directions:
//! lossless over the FULL 64-bit range, never bounds-checked, never
//! panicking. Heights at or above 2^63 are stored as negative INTEGERs —
//! roundtrip is exact, but SQL ordering/comparison on height columns is
//! only order-preserving below 2^63 (unreachable in practice; see the
//! ceiling analysis in docs/specs/regenesis.md).

/// u64 height → SQLite INTEGER (bit cast, lossless).
#[inline]
pub fn height_to_db(h: u64) -> i64 {
    h as i64
}

/// SQLite INTEGER → u64 height (bit cast, lossless).
#[inline]
pub fn height_from_db(v: i64) -> u64 {
    v as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: roundtrip every u64 height through the SQLite i64 mapping
    // losslessly, including values above i64::MAX.
    #[test]
    fn height_db_mapping_roundtrips_full_range() {
        for h in [
            0u64,
            1,
            12345,
            u32::MAX as u64 + 1,
            i64::MAX as u64,
            i64::MAX as u64 + 1,
            u64::MAX,
        ] {
            assert_eq!(height_from_db(height_to_db(h)), h);
        }
    }

    // Should: map heights above i64::MAX onto negative INTEGERs rather
    // than saturating or wrapping to an unrelated value.
    #[test]
    fn height_above_signed_max_is_negative_in_db() {
        assert_eq!(height_to_db(u64::MAX), -1);
        assert_eq!(height_to_db(i64::MAX as u64 + 1), i64::MIN);
    }
}
