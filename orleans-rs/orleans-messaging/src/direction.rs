//! Message direction enum.
//!
//! Indicates whether a message is a request, response, or one-way message.

use std::fmt;

/// Direction of a message in the Orleans messaging system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Direction {
    /// A request message expecting a response.
    Request = 0,
    /// A response to a previous request.
    Response = 1,
    /// A one-way message that doesn't expect a response.
    OneWay = 2,
}

impl Direction {
    /// Returns true if this is a request that expects a response.
    pub fn is_request(&self) -> bool {
        matches!(self, Direction::Request)
    }

    /// Returns true if this is a response message.
    pub fn is_response(&self) -> bool {
        matches!(self, Direction::Response)
    }

    /// Returns true if this is a one-way message.
    pub fn is_one_way(&self) -> bool {
        matches!(self, Direction::OneWay)
    }

    /// Converts from a u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Direction::Request),
            1 => Some(Direction::Response),
            2 => Some(Direction::OneWay),
            _ => None,
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Request => write!(f, "Request"),
            Direction::Response => write!(f, "Response"),
            Direction::OneWay => write!(f, "OneWay"),
        }
    }
}

impl Default for Direction {
    fn default() -> Self {
        Direction::Request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direction_predicates() {
        assert!(Direction::Request.is_request());
        assert!(!Direction::Request.is_response());
        assert!(!Direction::Request.is_one_way());

        assert!(!Direction::Response.is_request());
        assert!(Direction::Response.is_response());
        assert!(!Direction::Response.is_one_way());

        assert!(!Direction::OneWay.is_request());
        assert!(!Direction::OneWay.is_response());
        assert!(Direction::OneWay.is_one_way());
    }

    #[test]
    fn test_from_u8() {
        assert_eq!(Direction::from_u8(0), Some(Direction::Request));
        assert_eq!(Direction::from_u8(1), Some(Direction::Response));
        assert_eq!(Direction::from_u8(2), Some(Direction::OneWay));
        assert_eq!(Direction::from_u8(3), None);
        assert_eq!(Direction::from_u8(255), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(Direction::Request.to_string(), "Request");
        assert_eq!(Direction::Response.to_string(), "Response");
        assert_eq!(Direction::OneWay.to_string(), "OneWay");
    }
}
