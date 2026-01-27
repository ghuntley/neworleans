//! SiloAddress - Network address for a silo
//!
//! `SiloAddress` identifies a specific silo instance in the cluster by combining:
//! - Network endpoint (IP address and port)
//! - Generation number (epoch) to distinguish restarts at the same address

use crate::OrleansError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use xxhash_rust::xxh32;

/// Network address for an Orleans silo.
///
/// A `SiloAddress` uniquely identifies a silo instance in the cluster by combining:
/// - **Endpoint**: The network address (IP:port) where the silo listens
/// - **Generation**: A monotonically increasing number (typically timestamp) that
///   distinguishes different silo instances at the same address
///
/// The generation number is crucial for:
/// - Detecting silo restarts (same IP:port but different generation)
/// - Invalidating stale grain directory entries
/// - Preventing message delivery to zombie silos
///
/// # String Format
///
/// `SiloAddress` serializes to/from: `{ip}:{port}@{generation}`
///
/// Example: `192.168.1.10:11111@12345678`
///
/// # Examples
///
/// ```
/// use orleans_core::SiloAddress;
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "192.168.1.10:11111".parse().unwrap();
/// let silo = SiloAddress::new(addr, 12345678);
///
/// assert_eq!(silo.endpoint(), &addr);
/// assert_eq!(silo.generation(), 12345678);
/// ```
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SiloAddress {
    /// Network endpoint (IP:port)
    endpoint: SocketAddr,
    /// Generation number (epoch) - typically a timestamp
    generation: i64,
}

impl SiloAddress {
    /// Creates a new `SiloAddress`.
    ///
    /// # Arguments
    /// * `endpoint` - The network address (IP:port)
    /// * `generation` - The generation number (typically Unix timestamp in ticks)
    pub fn new(endpoint: SocketAddr, generation: i64) -> Self {
        Self {
            endpoint,
            generation,
        }
    }

    /// Creates a `SiloAddress` with the current timestamp as generation.
    pub fn new_with_current_generation(endpoint: SocketAddr) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let generation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        Self::new(endpoint, generation)
    }

    /// Creates a `SiloAddress` from string parts.
    ///
    /// # Arguments
    /// * `ip` - The IP address as a string
    /// * `port` - The port number
    /// * `generation` - The generation number
    pub fn from_parts(ip: &str, port: u16, generation: i64) -> Result<Self, OrleansError> {
        let ip_addr: std::net::IpAddr = ip
            .parse()
            .map_err(|e| OrleansError::InvalidSiloAddress(format!("Invalid IP: {}", e)))?;
        Ok(Self::new(SocketAddr::new(ip_addr, port), generation))
    }

    /// Returns the default (zero) silo address.
    pub fn zero() -> Self {
        Self {
            endpoint: SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0),
            generation: 0,
        }
    }

    /// Returns true if this is the zero (unspecified) address.
    pub fn is_zero(&self) -> bool {
        self.generation == 0 && self.endpoint.port() == 0
    }

    /// Returns the network endpoint.
    pub fn endpoint(&self) -> &SocketAddr {
        &self.endpoint
    }

    /// Returns the IP address.
    pub fn ip(&self) -> std::net::IpAddr {
        self.endpoint.ip()
    }

    /// Returns the port number.
    pub fn port(&self) -> u16 {
        self.endpoint.port()
    }

    /// Returns the generation number.
    ///
    /// The generation distinguishes different silo instances at the same address.
    /// When a silo restarts, it gets a new (higher) generation number.
    pub fn generation(&self) -> i64 {
        self.generation
    }

    /// Returns the hash code for this address.
    ///
    /// Used for consistent hashing and hash map lookups.
    pub fn get_hash_code(&self) -> u32 {
        // Combine endpoint and generation into a hash
        let mut data = Vec::with_capacity(24);
        match self.endpoint.ip() {
            std::net::IpAddr::V4(ip) => {
                data.extend_from_slice(&ip.octets());
            }
            std::net::IpAddr::V6(ip) => {
                data.extend_from_slice(&ip.octets());
            }
        }
        data.extend_from_slice(&self.endpoint.port().to_le_bytes());
        data.extend_from_slice(&self.generation.to_le_bytes());
        xxh32::xxh32(&data, 0)
    }

    /// Returns the uniform hash code (same as `get_hash_code`).
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.get_hash_code()
    }

    /// Checks if this silo address matches another, ignoring generation.
    ///
    /// Useful for checking if two addresses refer to the same physical silo
    /// (which might have restarted with a different generation).
    pub fn matches_endpoint(&self, other: &SiloAddress) -> bool {
        self.endpoint == other.endpoint
    }

    /// Checks if this is a newer generation than another address at the same endpoint.
    pub fn is_newer_than(&self, other: &SiloAddress) -> bool {
        self.endpoint == other.endpoint && self.generation > other.generation
    }

    /// Parses a `SiloAddress` from its string representation.
    ///
    /// Format: `{ip}:{port}@{generation}`
    pub fn parse(s: &str) -> Result<Self, OrleansError> {
        let at_pos = s
            .rfind('@')
            .ok_or_else(|| OrleansError::InvalidSiloAddress(format!("missing @ in: {}", s)))?;

        let endpoint_str = &s[..at_pos];
        let gen_str = &s[at_pos + 1..];

        let endpoint: SocketAddr = endpoint_str
            .parse()
            .map_err(|e| OrleansError::InvalidSiloAddress(format!("Invalid endpoint: {}", e)))?;

        let generation: i64 = gen_str
            .parse()
            .map_err(|e| OrleansError::InvalidSiloAddress(format!("Invalid generation: {}", e)))?;

        Ok(Self::new(endpoint, generation))
    }
}

impl Default for SiloAddress {
    fn default() -> Self {
        Self::zero()
    }
}

impl FromStr for SiloAddress {
    type Err = OrleansError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Debug for SiloAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SiloAddress({}@{})", self.endpoint, self.generation)
    }
}

impl fmt::Display for SiloAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.endpoint, self.generation)
    }
}

impl PartialOrd for SiloAddress {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SiloAddress {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by endpoint first, then by generation
        match self.endpoint.to_string().cmp(&other.endpoint.to_string()) {
            std::cmp::Ordering::Equal => self.generation.cmp(&other.generation),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_new() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo = SiloAddress::new(addr, 12345678);

        assert_eq!(silo.endpoint(), &addr);
        assert_eq!(silo.generation(), 12345678);
        assert_eq!(silo.ip(), IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)));
        assert_eq!(silo.port(), 11111);
    }

    #[test]
    fn test_from_parts() {
        let silo = SiloAddress::from_parts("10.0.0.1", 22222, 99999).unwrap();
        assert_eq!(silo.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(silo.port(), 22222);
        assert_eq!(silo.generation(), 99999);
    }

    #[test]
    fn test_from_parts_invalid_ip() {
        let result = SiloAddress::from_parts("not-an-ip", 1234, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero() {
        let silo = SiloAddress::zero();
        assert!(silo.is_zero());
        assert_eq!(silo.generation(), 0);
        assert_eq!(silo.port(), 0);
    }

    #[test]
    fn test_not_zero() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111);
        let silo = SiloAddress::new(addr, 1);
        assert!(!silo.is_zero());
    }

    #[test]
    fn test_equality() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo1 = SiloAddress::new(addr, 12345);
        let silo2 = SiloAddress::new(addr, 12345);
        let silo3 = SiloAddress::new(addr, 99999); // Different generation

        assert_eq!(silo1, silo2);
        assert_ne!(silo1, silo3);
    }

    #[test]
    fn test_matches_endpoint() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo1 = SiloAddress::new(addr, 12345);
        let silo2 = SiloAddress::new(addr, 99999); // Different generation

        assert!(silo1.matches_endpoint(&silo2));

        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)), 11111);
        let silo3 = SiloAddress::new(addr2, 12345);
        assert!(!silo1.matches_endpoint(&silo3));
    }

    #[test]
    fn test_is_newer_than() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let older = SiloAddress::new(addr, 1000);
        let newer = SiloAddress::new(addr, 2000);

        assert!(newer.is_newer_than(&older));
        assert!(!older.is_newer_than(&newer));
        assert!(!older.is_newer_than(&older)); // Same generation

        // Different endpoint - should be false
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)), 11111);
        let different = SiloAddress::new(addr2, 3000);
        assert!(!different.is_newer_than(&older));
    }

    #[test]
    fn test_hash_code_stable() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo1 = SiloAddress::new(addr, 12345);
        let silo2 = SiloAddress::new(addr, 12345);

        assert_eq!(silo1.get_hash_code(), silo2.get_hash_code());
        assert_eq!(silo1.get_uniform_hash_code(), silo2.get_uniform_hash_code());
    }

    #[test]
    fn test_hash_code_different() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo1 = SiloAddress::new(addr, 12345);
        let silo2 = SiloAddress::new(addr, 99999);

        // Different generations should (usually) have different hashes
        assert_ne!(silo1.get_hash_code(), silo2.get_hash_code());
    }

    #[test]
    fn test_parse() {
        let silo = SiloAddress::parse("192.168.1.10:11111@12345678").unwrap();
        assert_eq!(silo.ip(), IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)));
        assert_eq!(silo.port(), 11111);
        assert_eq!(silo.generation(), 12345678);
    }

    #[test]
    fn test_parse_ipv6() {
        let silo = SiloAddress::parse("[::1]:11111@12345").unwrap();
        assert_eq!(silo.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(silo.port(), 11111);
        assert_eq!(silo.generation(), 12345);
    }

    #[test]
    fn test_parse_invalid_no_at() {
        let result = SiloAddress::parse("192.168.1.10:11111");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_endpoint() {
        let result = SiloAddress::parse("not-an-endpoint@12345");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_generation() {
        let result = SiloAddress::parse("192.168.1.10:11111@not-a-number");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_str() {
        let silo: SiloAddress = "10.0.0.1:22222@99999".parse().unwrap();
        assert_eq!(silo.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(silo.port(), 22222);
        assert_eq!(silo.generation(), 99999);
    }

    #[test]
    fn test_display_roundtrip() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo = SiloAddress::new(addr, 12345678);
        let display = format!("{}", silo);
        let parsed: SiloAddress = display.parse().unwrap();
        assert_eq!(silo, parsed);
    }

    #[test]
    fn test_debug() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo = SiloAddress::new(addr, 12345);
        let debug = format!("{:?}", silo);
        assert!(debug.contains("192.168.1.10"));
        assert!(debug.contains("11111"));
        assert!(debug.contains("12345"));
    }

    #[test]
    fn test_ordering() {
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 11111);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 11111);

        let silo1 = SiloAddress::new(addr1, 100);
        let silo2 = SiloAddress::new(addr1, 200);
        let silo3 = SiloAddress::new(addr2, 50);

        // Same endpoint: order by generation
        assert!(silo1 < silo2);

        // Different endpoints: order by endpoint string
        assert!(silo1 < silo3);
    }

    #[test]
    fn test_ipv6_address() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 11111);
        let silo = SiloAddress::new(addr, 12345);

        assert_eq!(silo.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(silo.port(), 11111);

        // Hash should work for IPv6
        let hash = silo.get_hash_code();
        assert_ne!(hash, 0);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_parse_display_roundtrip(
            ip0 in 0u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            generation in any::<i64>()
        ) {
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                port
            );
            let silo = SiloAddress::new(addr, generation);
            let display = format!("{}", silo);
            let parsed: Result<SiloAddress, _> = display.parse();
            prop_assert!(parsed.is_ok());
            prop_assert_eq!(silo, parsed.unwrap());
        }

        #[test]
        fn prop_hash_stable(
            ip0 in 0u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            generation in any::<i64>()
        ) {
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                port
            );
            let silo1 = SiloAddress::new(addr, generation);
            let silo2 = SiloAddress::new(addr, generation);
            prop_assert_eq!(silo1.get_hash_code(), silo2.get_hash_code());
        }

        #[test]
        fn prop_newer_generation_is_newer(
            ip0 in 0u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            gen1 in 0i64..i64::MAX - 1,
        ) {
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                port
            );
            let older = SiloAddress::new(addr, gen1);
            let newer = SiloAddress::new(addr, gen1 + 1);
            prop_assert!(newer.is_newer_than(&older));
            prop_assert!(!older.is_newer_than(&newer));
        }
    }
}
