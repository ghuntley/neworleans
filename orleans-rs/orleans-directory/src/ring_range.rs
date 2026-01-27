//! Ring range representation for consistent hashing.
//!
//! A ring range represents a portion of the 32-bit hash space used in
//! consistent hashing. Ranges are half-open intervals (start, end] that
//! can wrap around the ring (when start > end).

use std::fmt;

/// A segment of the consistent hash ring.
///
/// The segment represents a half-open interval (start, end] in the hash space.
/// When start > end, the segment wraps around the ring (covers both tails).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RingSegment {
    /// Start of the range (exclusive).
    pub start: u32,
    /// End of the range (inclusive).
    pub end: u32,
}

impl RingSegment {
    /// Creates a new ring segment.
    pub fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }

    /// Checks if the segment contains the given hash value.
    ///
    /// The segment is a half-open interval (start, end].
    /// If start > end, the segment wraps around the ring.
    pub fn contains(&self, hash: u32) -> bool {
        if self.start < self.end {
            // Normal case: (start, end]
            hash > self.start && hash <= self.end
        } else if self.start > self.end {
            // Wrap-around case: (start, MAX] ∪ [0, end]
            hash > self.start || hash <= self.end
        } else {
            // start == end means the entire ring
            true
        }
    }

    /// Returns true if this segment wraps around the ring.
    pub fn is_wrapping(&self) -> bool {
        self.start > self.end
    }

    /// Returns true if this segment covers the entire ring.
    pub fn is_full_ring(&self) -> bool {
        self.start == self.end
    }

    /// Returns the size of this segment in the hash space.
    pub fn size(&self) -> u64 {
        if self.start < self.end {
            (self.end - self.start) as u64
        } else if self.start > self.end {
            // Wrapping: size is (MAX - start) + (end + 1)
            (u32::MAX - self.start) as u64 + self.end as u64 + 2
        } else {
            // Full ring
            u32::MAX as u64 + 1
        }
    }
}

impl fmt::Debug for RingSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_wrapping() {
            write!(f, "({:#x}, {:#x}]wrap", self.start, self.end)
        } else if self.is_full_ring() {
            write!(f, "[full ring]")
        } else {
            write!(f, "({:#x}, {:#x}]", self.start, self.end)
        }
    }
}

impl fmt::Display for RingSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A collection of ring segments, potentially discontinuous.
///
/// This represents the hash ranges owned by a silo, which may consist
/// of multiple non-contiguous segments when using virtual buckets.
#[derive(Clone, Debug, Default)]
pub struct RingRange {
    segments: Vec<RingSegment>,
}

impl RingRange {
    /// Creates an empty ring range.
    pub fn empty() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Creates a ring range from a single segment.
    pub fn single(start: u32, end: u32) -> Self {
        Self {
            segments: vec![RingSegment::new(start, end)],
        }
    }

    /// Creates a ring range covering the entire ring.
    pub fn full() -> Self {
        Self {
            segments: vec![RingSegment::new(0, 0)],
        }
    }

    /// Creates a ring range from a list of bucket hash values.
    ///
    /// Each bucket represents a point in the ring, and this function
    /// computes the ranges preceding each bucket (the ranges owned
    /// by the silo that owns those buckets).
    pub fn from_buckets(bucket_hashes: &[u32], all_buckets_sorted: &[u32]) -> Self {
        if bucket_hashes.is_empty() || all_buckets_sorted.is_empty() {
            return Self::empty();
        }

        let mut segments = Vec::with_capacity(bucket_hashes.len());

        for &bucket_hash in bucket_hashes {
            // Find this bucket in the sorted list
            let bucket_idx = match all_buckets_sorted.binary_search(&bucket_hash) {
                Ok(idx) => idx,
                Err(_) => continue, // Bucket not found
            };

            // Find the previous bucket (predecessor)
            let prev_bucket = if bucket_idx == 0 {
                *all_buckets_sorted.last().unwrap()
            } else {
                all_buckets_sorted[bucket_idx - 1]
            };

            // The range owned by this bucket is (prev_bucket, bucket_hash]
            segments.push(RingSegment::new(prev_bucket, bucket_hash));
        }

        Self { segments }
    }

    /// Returns true if this range is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Checks if the range contains the given hash value.
    pub fn contains(&self, hash: u32) -> bool {
        self.segments.iter().any(|s| s.contains(hash))
    }

    /// Returns the segments in this range.
    pub fn segments(&self) -> &[RingSegment] {
        &self.segments
    }

    /// Returns the total size of all segments.
    pub fn total_size(&self) -> u64 {
        self.segments.iter().map(|s| s.size()).sum()
    }

    /// Adds a segment to this range.
    pub fn add_segment(&mut self, start: u32, end: u32) {
        self.segments.push(RingSegment::new(start, end));
    }

    /// Merges another range into this one.
    pub fn merge(&mut self, other: &RingRange) {
        self.segments.extend_from_slice(&other.segments);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_contains_normal() {
        let segment = RingSegment::new(100, 200);

        // Inside range
        assert!(segment.contains(150));
        assert!(segment.contains(101));
        assert!(segment.contains(200)); // End is inclusive

        // Outside range
        assert!(!segment.contains(100)); // Start is exclusive
        assert!(!segment.contains(50));
        assert!(!segment.contains(250));
    }

    #[test]
    fn test_segment_contains_wrapping() {
        // Wrapping segment: (0xFFFFFFF0, 0x10]
        let segment = RingSegment::new(0xFFFFFFF0, 0x10);

        // Inside range (high end)
        assert!(segment.contains(0xFFFFFFFF));
        assert!(segment.contains(0xFFFFFFF1));

        // Inside range (low end)
        assert!(segment.contains(0x0));
        assert!(segment.contains(0x10)); // End is inclusive

        // Outside range
        assert!(!segment.contains(0xFFFFFFF0)); // Start is exclusive
        assert!(!segment.contains(0x11));
        assert!(!segment.contains(0x80000000));
    }

    #[test]
    fn test_segment_full_ring() {
        let segment = RingSegment::new(100, 100);

        // Full ring contains everything
        assert!(segment.contains(0));
        assert!(segment.contains(100));
        assert!(segment.contains(u32::MAX));
        assert!(segment.is_full_ring());
    }

    #[test]
    fn test_segment_size() {
        // Normal segment
        let segment = RingSegment::new(100, 200);
        assert_eq!(segment.size(), 100);

        // Wrapping segment
        let wrap = RingSegment::new(u32::MAX - 10, 10);
        // Size should be 11 (from MAX-10 to MAX) + 11 (from 0 to 10)
        assert_eq!(wrap.size(), 22);
    }

    #[test]
    fn test_range_empty() {
        let range = RingRange::empty();
        assert!(range.is_empty());
        assert!(!range.contains(0));
        assert!(!range.contains(u32::MAX));
    }

    #[test]
    fn test_range_single() {
        let range = RingRange::single(100, 200);
        assert!(!range.is_empty());
        assert!(range.contains(150));
        assert!(!range.contains(50));
    }

    #[test]
    fn test_range_full() {
        let range = RingRange::full();
        assert!(range.contains(0));
        assert!(range.contains(u32::MAX));
        assert!(range.contains(0x80000000));
    }

    #[test]
    fn test_range_from_buckets() {
        // Simulate 3 silos with 2 buckets each
        // Sorted buckets: [100, 200, 300, 400, 500, 600]
        let all_buckets = vec![100, 200, 300, 400, 500, 600];

        // Silo 1 owns buckets at 100 and 400
        let silo1_buckets = vec![100, 400];
        let silo1_range = RingRange::from_buckets(&silo1_buckets, &all_buckets);

        // Silo 1 should own (600, 100] and (300, 400]
        assert_eq!(silo1_range.segments().len(), 2);
        assert!(silo1_range.contains(50)); // In (600, 100] wrapping
        assert!(silo1_range.contains(100)); // End is inclusive
        assert!(silo1_range.contains(350)); // In (300, 400]
        assert!(!silo1_range.contains(200)); // Owned by another silo
    }

    #[test]
    fn test_range_merge() {
        let mut range1 = RingRange::single(100, 200);
        let range2 = RingRange::single(300, 400);

        range1.merge(&range2);

        assert_eq!(range1.segments().len(), 2);
        assert!(range1.contains(150));
        assert!(range1.contains(350));
        assert!(!range1.contains(250));
    }
}
