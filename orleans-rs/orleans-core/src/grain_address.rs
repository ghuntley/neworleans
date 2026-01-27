//! GrainAddress - Complete location of a grain activation
//!
//! `GrainAddress` represents the complete location of a grain activation,
//! combining `GrainId`, `ActivationId`, and `SiloAddress`.

use crate::{ActivationId, GrainId, OrleansError, SiloAddress};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Complete location of a grain activation in the cluster.
///
/// A `GrainAddress` fully specifies where a grain activation lives:
/// - **GrainId**: Which grain (type + key)
/// - **ActivationId**: Which specific activation of that grain
/// - **SiloAddress**: Which silo hosts the activation
///
/// A complete address has all three components filled in. Incomplete addresses
/// are used during grain activation and directory lookup.
///
/// # Completeness
///
/// An address is "complete" when:
/// - `grain_id` is not default
/// - `activation_id` is not default
/// - `silo_address` is present
///
/// # Examples
///
/// ```
/// use orleans_core::{GrainAddress, GrainId, ActivationId, SiloAddress};
/// use std::net::SocketAddr;
///
/// let grain_id = GrainId::create("MyApp.UserGrain", "user-123");
/// let activation_id = ActivationId::new();
/// let silo: SocketAddr = "192.168.1.10:11111".parse().unwrap();
/// let silo_address = SiloAddress::new(silo, 12345678);
///
/// let address = GrainAddress::new(grain_id.clone(), activation_id, Some(silo_address));
/// assert!(address.is_complete());
/// assert_eq!(address.grain_id(), &grain_id);
/// ```
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct GrainAddress {
    /// The grain's identity
    grain_id: GrainId,
    /// The specific activation's identity
    activation_id: ActivationId,
    /// The silo hosting this activation (None if unknown)
    silo_address: Option<SiloAddress>,
}

impl GrainAddress {
    /// Creates a new `GrainAddress`.
    ///
    /// # Arguments
    /// * `grain_id` - The grain identity
    /// * `activation_id` - The activation identity
    /// * `silo_address` - The silo address (optional)
    pub fn new(
        grain_id: GrainId,
        activation_id: ActivationId,
        silo_address: Option<SiloAddress>,
    ) -> Self {
        Self {
            grain_id,
            activation_id,
            silo_address,
        }
    }

    /// Creates a `GrainAddress` with only the grain ID (incomplete).
    ///
    /// Used when looking up a grain in the directory.
    pub fn for_grain(grain_id: GrainId) -> Self {
        Self {
            grain_id,
            activation_id: ActivationId::default(),
            silo_address: None,
        }
    }

    /// Creates a complete `GrainAddress` from all components.
    pub fn complete(
        grain_id: GrainId,
        activation_id: ActivationId,
        silo_address: SiloAddress,
    ) -> Self {
        Self {
            grain_id,
            activation_id,
            silo_address: Some(silo_address),
        }
    }

    /// Returns the default (empty) grain address.
    pub fn default_address() -> Self {
        Self {
            grain_id: GrainId::default(),
            activation_id: ActivationId::default(),
            silo_address: None,
        }
    }

    /// Returns true if this is the default (empty) address.
    pub fn is_default(&self) -> bool {
        self.grain_id.is_default()
            && self.activation_id.is_default()
            && self.silo_address.is_none()
    }

    /// Returns true if this address is complete.
    ///
    /// A complete address has:
    /// - Non-default grain ID
    /// - Non-default activation ID
    /// - Present silo address
    pub fn is_complete(&self) -> bool {
        !self.grain_id.is_default()
            && !self.activation_id.is_default()
            && self.silo_address.is_some()
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Returns the activation ID.
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    /// Returns the silo address (if present).
    pub fn silo_address(&self) -> Option<&SiloAddress> {
        self.silo_address.as_ref()
    }

    /// Returns the silo address, panicking if not present.
    ///
    /// # Panics
    /// Panics if the silo address is not set.
    pub fn silo_address_unwrap(&self) -> &SiloAddress {
        self.silo_address
            .as_ref()
            .expect("GrainAddress has no silo_address")
    }

    /// Returns true if this address refers to the same grain as another.
    pub fn same_grain(&self, other: &GrainAddress) -> bool {
        self.grain_id == other.grain_id
    }

    /// Returns true if this address refers to the same activation as another.
    pub fn same_activation(&self, other: &GrainAddress) -> bool {
        self.grain_id == other.grain_id && self.activation_id == other.activation_id
    }

    /// Creates a copy with a new silo address.
    pub fn with_silo(&self, silo_address: SiloAddress) -> Self {
        Self {
            grain_id: self.grain_id.clone(),
            activation_id: self.activation_id,
            silo_address: Some(silo_address),
        }
    }

    /// Creates a copy with a new activation ID.
    pub fn with_activation(&self, activation_id: ActivationId) -> Self {
        Self {
            grain_id: self.grain_id.clone(),
            activation_id,
            silo_address: self.silo_address,
        }
    }

    /// Returns the hash code for consistent hashing.
    ///
    /// Uses only the grain ID hash (not activation or silo) since
    /// directory lookups are by grain ID.
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.grain_id.get_uniform_hash_code()
    }

    /// Checks if this address matches a target (grain ID and optional activation ID).
    pub fn matches(&self, grain_id: &GrainId, activation_id: Option<&ActivationId>) -> bool {
        if &self.grain_id != grain_id {
            return false;
        }
        match activation_id {
            Some(act_id) => &self.activation_id == act_id,
            None => true,
        }
    }

    /// Parses a `GrainAddress` from its string representation.
    /// Format: `{grain_id}#{activation_id}@{silo_address}` or `{grain_id}#{activation_id}`
    pub fn parse(s: &str) -> Result<Self, OrleansError> {
        // First find the # to separate grain_id from activation_id
        let hash_pos = s
            .find("#")
            .ok_or_else(|| OrleansError::ParseError(format!("missing # in: {}", s)))?;

        let grain_str = &s[..hash_pos];
        let rest = &s[hash_pos + 1..];

        // ActivationId is a UUID which is exactly 36 characters (with dashes)
        // Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        // After the UUID, if there is more content, it starts with @ followed by silo address
        let (act_str, silo_part) = if rest.len() > 36 && rest.chars().nth(36) == Some("@".chars().next().unwrap()) {
            (&rest[..36], Some(&rest[37..]))
        } else {
            (rest, None)
        };

        let grain_id = GrainId::parse(grain_str)?;
        let activation_id = ActivationId::parse(act_str)
            .map_err(|e| OrleansError::ParseError(format!("Invalid activation ID: {}", e)))?;

        let silo_address = match silo_part {
            Some(s) => Some(SiloAddress::parse(s)?),
            None => None,
        };

        Ok(Self {
            grain_id,
            activation_id,
            silo_address,
        })
    }
}

impl Default for GrainAddress {
    fn default() -> Self {
        Self::default_address()
    }
}

impl fmt::Debug for GrainAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrainAddress")
            .field("grain_id", &self.grain_id)
            .field("activation_id", &self.activation_id)
            .field("silo_address", &self.silo_address)
            .finish()
    }
}

impl fmt::Display for GrainAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.grain_id, self.activation_id)?;
        if let Some(silo) = &self.silo_address {
            write!(f, "@{}", silo)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_silo() -> SiloAddress {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        SiloAddress::new(addr, 12345678)
    }

    #[test]
    fn test_new() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();

        let addr = GrainAddress::new(grain_id.clone(), activation_id, Some(silo));

        assert_eq!(addr.grain_id(), &grain_id);
        assert_eq!(addr.activation_id(), &activation_id);
        assert_eq!(addr.silo_address(), Some(&silo));
        assert!(addr.is_complete());
    }

    #[test]
    fn test_for_grain() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let addr = GrainAddress::for_grain(grain_id.clone());

        assert_eq!(addr.grain_id(), &grain_id);
        assert!(addr.activation_id().is_default());
        assert!(addr.silo_address().is_none());
        assert!(!addr.is_complete());
    }

    #[test]
    fn test_complete() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();

        let addr = GrainAddress::complete(grain_id.clone(), activation_id, silo);

        assert!(addr.is_complete());
        assert_eq!(addr.silo_address_unwrap(), &silo);
    }

    #[test]
    fn test_default() {
        let addr = GrainAddress::default();
        assert!(addr.is_default());
        assert!(!addr.is_complete());
    }

    #[test]
    fn test_is_complete() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();

        // Complete address
        let complete = GrainAddress::complete(grain_id.clone(), activation_id, silo);
        assert!(complete.is_complete());

        // Missing silo
        let no_silo = GrainAddress::new(grain_id.clone(), activation_id, None);
        assert!(!no_silo.is_complete());

        // Default activation ID
        let default_act =
            GrainAddress::new(grain_id.clone(), ActivationId::default(), Some(silo));
        assert!(!default_act.is_complete());

        // Default grain ID
        let default_grain = GrainAddress::new(GrainId::default(), activation_id, Some(silo));
        assert!(!default_grain.is_complete());
    }

    #[test]
    fn test_same_grain() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let addr1 = GrainAddress::complete(grain_id.clone(), ActivationId::new(), test_silo());
        let addr2 = GrainAddress::complete(grain_id.clone(), ActivationId::new(), test_silo());

        assert!(addr1.same_grain(&addr2));

        let different = GrainAddress::for_grain(GrainId::create("OtherGrain", "other-key"));
        assert!(!addr1.same_grain(&different));
    }

    #[test]
    fn test_same_activation() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let addr1 = GrainAddress::complete(grain_id.clone(), activation_id, test_silo());
        let addr2 = GrainAddress::new(grain_id.clone(), activation_id, None);

        assert!(addr1.same_activation(&addr2));

        let different_act =
            GrainAddress::complete(grain_id.clone(), ActivationId::new(), test_silo());
        assert!(!addr1.same_activation(&different_act));
    }

    #[test]
    fn test_with_silo() {
        let addr = GrainAddress::for_grain(GrainId::create("TestGrain", "test-key"));
        let silo = test_silo();

        let with_silo = addr.with_silo(silo);
        assert_eq!(with_silo.silo_address(), Some(&silo));
        assert_eq!(with_silo.grain_id(), addr.grain_id());
    }

    #[test]
    fn test_with_activation() {
        let addr = GrainAddress::for_grain(GrainId::create("TestGrain", "test-key"));
        let activation_id = ActivationId::new();

        let with_act = addr.with_activation(activation_id);
        assert_eq!(with_act.activation_id(), &activation_id);
        assert_eq!(with_act.grain_id(), addr.grain_id());
    }

    #[test]
    fn test_hash_code() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let addr1 = GrainAddress::complete(grain_id.clone(), ActivationId::new(), test_silo());
        let addr2 = GrainAddress::complete(grain_id.clone(), ActivationId::new(), test_silo());

        // Hash based on grain ID only
        assert_eq!(addr1.get_uniform_hash_code(), addr2.get_uniform_hash_code());
        assert_eq!(
            addr1.get_uniform_hash_code(),
            grain_id.get_uniform_hash_code()
        );
    }

    #[test]
    fn test_matches() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let addr = GrainAddress::complete(grain_id.clone(), activation_id, test_silo());

        // Match by grain ID only
        assert!(addr.matches(&grain_id, None));

        // Match by grain ID and activation ID
        assert!(addr.matches(&grain_id, Some(&activation_id)));

        // No match - different grain ID
        assert!(!addr.matches(&GrainId::create("OtherGrain", "key"), None));

        // No match - different activation ID
        assert!(!addr.matches(&grain_id, Some(&ActivationId::new())));
    }

    #[test]
    fn test_parse_complete() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();
        let addr = GrainAddress::complete(grain_id.clone(), activation_id, silo);

        let display = format!("{}", addr);
        let parsed = GrainAddress::parse(&display).unwrap();

        assert_eq!(addr, parsed);
    }

    #[test]
    fn test_parse_without_silo() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let addr = GrainAddress::new(grain_id.clone(), activation_id, None);

        let display = format!("{}", addr);
        let parsed = GrainAddress::parse(&display).unwrap();

        assert_eq!(addr.grain_id(), parsed.grain_id());
        assert_eq!(addr.activation_id(), parsed.activation_id());
        assert!(parsed.silo_address().is_none());
    }

    #[test]
    fn test_parse_invalid() {
        // Missing #
        let result = GrainAddress::parse("TestGrain/key");
        assert!(result.is_err());

        // Invalid grain ID (missing /)
        let result = GrainAddress::parse("TestGrain#00000000-0000-0000-0000-000000000000");
        assert!(result.is_err());
    }

    #[test]
    fn test_display() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();

        let addr = GrainAddress::complete(grain_id, activation_id, silo);
        let display = format!("{}", addr);

        assert!(display.contains("TestGrain"));
        assert!(display.contains("test-key"));
        assert!(display.contains("#"));
        assert!(display.contains("@"));
        assert!(display.contains("192.168.1.10"));
    }

    #[test]
    fn test_debug() {
        let addr = GrainAddress::for_grain(GrainId::create("TestGrain", "test-key"));
        let debug = format!("{:?}", addr);

        assert!(debug.contains("GrainAddress"));
        assert!(debug.contains("grain_id"));
        assert!(debug.contains("activation_id"));
        assert!(debug.contains("silo_address"));
    }

    #[test]
    fn test_equality() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let activation_id = ActivationId::new();
        let silo = test_silo();

        let addr1 = GrainAddress::complete(grain_id.clone(), activation_id, silo);
        let addr2 = GrainAddress::complete(grain_id.clone(), activation_id, silo);
        let addr3 = GrainAddress::complete(grain_id.clone(), ActivationId::new(), silo);

        assert_eq!(addr1, addr2);
        assert_ne!(addr1, addr3);
    }

    #[test]
    #[should_panic(expected = "has no silo_address")]
    fn test_silo_address_unwrap_panics() {
        let addr = GrainAddress::for_grain(GrainId::create("TestGrain", "test-key"));
        let _ = addr.silo_address_unwrap();
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    proptest! {
        #[test]
        fn prop_parse_display_roundtrip(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
            key in "[a-zA-Z0-9_-]{1,30}",
            ip0 in 1u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            generation in 1i64..i64::MAX
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let activation_id = ActivationId::new();
            let silo_addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                port
            );
            let silo = SiloAddress::new(silo_addr, generation);

            let addr = GrainAddress::complete(grain_id, activation_id, silo);
            let display = format!("{}", addr);
            let parsed = GrainAddress::parse(&display);

            prop_assert!(parsed.is_ok());
            prop_assert_eq!(addr, parsed.unwrap());
        }

        #[test]
        fn prop_complete_is_complete(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
            key in "[a-zA-Z0-9_-]{1,30}"
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let activation_id = ActivationId::new();
            let silo_addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)),
                11111
            );
            let silo = SiloAddress::new(silo_addr, 12345);

            let addr = GrainAddress::complete(grain_id, activation_id, silo);
            prop_assert!(addr.is_complete());
        }

        #[test]
        fn prop_hash_matches_grain_id_hash(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
            key in "[a-zA-Z0-9_-]{1,30}"
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let addr = GrainAddress::for_grain(grain_id.clone());
            prop_assert_eq!(addr.get_uniform_hash_code(), grain_id.get_uniform_hash_code());
        }
    }
}
