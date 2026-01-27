//! Orleans Telemetry - Structured logging and diagnostics for Orleans Rust port.
//!
//! This crate provides comprehensive structured logging infrastructure using the `tracing` crate.
//! It offers configurable output formats, Orleans-specific span macros, and logging initialization.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use orleans_telemetry::{init_logging, LogFormat, LogLevel};
//!
//! // Initialize logging with compact format at info level
//! init_logging(LogFormat::Compact, LogLevel::Info);
//! ```
//!
//! # Log Formats
//!
//! - [`LogFormat::Compact`] - Single-line output, good for development
//! - [`LogFormat::Pretty`] - Multi-line colorized output, good for debugging
//! - [`LogFormat::Json`] - JSON output, good for production log aggregation
//!
//! # Structured Logging
//!
//! Use the standard tracing macros with structured fields:
//!
//! ```rust,ignore
//! use tracing::{info, debug, warn, error, instrument};
//!
//! #[instrument(skip(self), fields(grain_id = %grain_id))]
//! async fn activate_grain(&self, grain_id: &GrainId) {
//!     info!(silo = %self.silo_address, "Activating grain");
//! }
//! ```

use parking_lot::Once;
use tracing::Level;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Static initialization guard to prevent double-init.
static INIT: Once = Once::new();

/// Output format for log messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// Compact single-line format, good for development.
    /// Example: `2024-01-15T10:30:45.123Z INFO orleans_host::silo: Starting silo silo=127.0.0.1:11111`
    #[default]
    Compact,

    /// Pretty multi-line format with colors, good for debugging.
    /// Shows full span context with indentation.
    Pretty,

    /// JSON format, good for production log aggregation (ELK, Datadog, etc.).
    /// Each log line is a valid JSON object.
    Json,
}

/// Log level filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    /// Trace level - very verbose, includes all details.
    Trace,

    /// Debug level - detailed information for debugging.
    Debug,

    /// Info level - general operational information.
    #[default]
    Info,

    /// Warn level - warning conditions that should be addressed.
    Warn,

    /// Error level - error conditions.
    Error,
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl From<LogLevel> for tracing_subscriber::filter::LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing_subscriber::filter::LevelFilter::TRACE,
            LogLevel::Debug => tracing_subscriber::filter::LevelFilter::DEBUG,
            LogLevel::Info => tracing_subscriber::filter::LevelFilter::INFO,
            LogLevel::Warn => tracing_subscriber::filter::LevelFilter::WARN,
            LogLevel::Error => tracing_subscriber::filter::LevelFilter::ERROR,
        }
    }
}

/// Configuration for logging initialization.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Output format for log messages.
    pub format: LogFormat,

    /// Default log level.
    pub level: LogLevel,

    /// Whether to include source code location in logs.
    pub include_location: bool,

    /// Whether to include thread IDs in logs.
    pub include_thread_ids: bool,

    /// Whether to include target (module path) in logs.
    pub include_target: bool,

    /// Whether to include span events (new/close).
    pub include_span_events: bool,

    /// Custom filter directive (overrides level if set).
    /// Example: "orleans_host=debug,orleans_runtime=trace"
    pub filter_directive: Option<String>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::Compact,
            level: LogLevel::Info,
            include_location: false,
            include_thread_ids: false,
            include_target: true,
            include_span_events: false,
            filter_directive: None,
        }
    }
}

impl LogConfig {
    /// Create a new LogConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the output format.
    pub fn with_format(mut self, format: LogFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the default log level.
    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.level = level;
        self
    }

    /// Include source code location in logs.
    pub fn with_location(mut self, include: bool) -> Self {
        self.include_location = include;
        self
    }

    /// Include thread IDs in logs.
    pub fn with_thread_ids(mut self, include: bool) -> Self {
        self.include_thread_ids = include;
        self
    }

    /// Include target (module path) in logs.
    pub fn with_target(mut self, include: bool) -> Self {
        self.include_target = include;
        self
    }

    /// Include span events (new/close) in logs.
    pub fn with_span_events(mut self, include: bool) -> Self {
        self.include_span_events = include;
        self
    }

    /// Set a custom filter directive.
    pub fn with_filter(mut self, directive: impl Into<String>) -> Self {
        self.filter_directive = Some(directive.into());
        self
    }

    /// Create a configuration preset for development.
    pub fn development() -> Self {
        Self {
            format: LogFormat::Pretty,
            level: LogLevel::Debug,
            include_location: true,
            include_thread_ids: false,
            include_target: true,
            include_span_events: false,
            filter_directive: None,
        }
    }

    /// Create a configuration preset for production.
    pub fn production() -> Self {
        Self {
            format: LogFormat::Json,
            level: LogLevel::Info,
            include_location: false,
            include_thread_ids: true,
            include_target: true,
            include_span_events: false,
            filter_directive: None,
        }
    }

    /// Create a configuration preset for testing.
    pub fn testing() -> Self {
        Self {
            format: LogFormat::Compact,
            level: LogLevel::Warn,
            include_location: false,
            include_thread_ids: false,
            include_target: false,
            include_span_events: false,
            filter_directive: None,
        }
    }
}

/// Initialize logging with the given format and level.
///
/// This is a convenience function that creates a default config with the
/// specified format and level.
///
/// # Example
///
/// ```rust,no_run
/// use orleans_telemetry::{init_logging, LogFormat, LogLevel};
///
/// init_logging(LogFormat::Compact, LogLevel::Info);
/// ```
///
/// # Note
///
/// This function can only be called once. Subsequent calls will be ignored.
pub fn init_logging(format: LogFormat, level: LogLevel) {
    let config = LogConfig::new()
        .with_format(format)
        .with_level(level);
    init_logging_with_config(config);
}

/// Initialize logging with a custom configuration.
///
/// # Example
///
/// ```rust,no_run
/// use orleans_telemetry::{init_logging_with_config, LogConfig, LogFormat, LogLevel};
///
/// let config = LogConfig::new()
///     .with_format(LogFormat::Json)
///     .with_level(LogLevel::Debug)
///     .with_filter("orleans_host=trace,orleans_runtime=debug");
///
/// init_logging_with_config(config);
/// ```
///
/// # Note
///
/// This function can only be called once. Subsequent calls will be ignored.
pub fn init_logging_with_config(config: LogConfig) {
    INIT.call_once(|| {
        init_logging_internal(config);
    });
}

/// Internal initialization function.
fn init_logging_internal(config: LogConfig) {
    let filter = if let Some(directive) = &config.filter_directive {
        EnvFilter::try_new(directive)
            .unwrap_or_else(|_| EnvFilter::new(format!("{}", Level::from(config.level))))
    } else {
        EnvFilter::from_default_env()
            .add_directive(tracing_subscriber::filter::LevelFilter::from(config.level).into())
    };

    let span_events = if config.include_span_events {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    match config.format {
        LogFormat::Compact => {
            let fmt_layer = fmt::layer()
                .with_target(config.include_target)
                .with_thread_ids(config.include_thread_ids)
                .with_file(config.include_location)
                .with_line_number(config.include_location)
                .with_span_events(span_events)
                .compact();

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();
        }
        LogFormat::Pretty => {
            let fmt_layer = fmt::layer()
                .with_target(config.include_target)
                .with_thread_ids(config.include_thread_ids)
                .with_file(config.include_location)
                .with_line_number(config.include_location)
                .with_span_events(span_events)
                .pretty();

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();
        }
        LogFormat::Json => {
            let fmt_layer = fmt::layer()
                .with_target(config.include_target)
                .with_thread_ids(config.include_thread_ids)
                .with_file(config.include_location)
                .with_line_number(config.include_location)
                .with_span_events(span_events)
                .json();

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();
        }
    }
}

/// Re-export commonly used tracing macros for convenience.
pub use tracing::{
    debug, error, info, trace, warn,
    debug_span, error_span, info_span, trace_span, warn_span,
    instrument, span, Level as TracingLevel, Span,
    event,
};

/// Orleans-specific field names for consistent structured logging.
pub mod fields {
    /// Field name for silo address.
    pub const SILO: &str = "silo";

    /// Field name for grain ID.
    pub const GRAIN_ID: &str = "grain_id";

    /// Field name for grain type.
    pub const GRAIN_TYPE: &str = "grain_type";

    /// Field name for activation ID.
    pub const ACTIVATION_ID: &str = "activation_id";

    /// Field name for correlation ID.
    pub const CORRELATION_ID: &str = "correlation_id";

    /// Field name for interface type.
    pub const INTERFACE_TYPE: &str = "interface_type";

    /// Field name for method ID.
    pub const METHOD_ID: &str = "method_id";

    /// Field name for error details.
    pub const ERROR: &str = "error";

    /// Field name for duration in milliseconds.
    pub const DURATION_MS: &str = "duration_ms";

    /// Field name for message direction.
    pub const DIRECTION: &str = "direction";

    /// Field name for target silo.
    pub const TARGET_SILO: &str = "target_silo";

    /// Field name for source silo.
    pub const SOURCE_SILO: &str = "source_silo";

    /// Field name for status.
    pub const STATUS: &str = "status";

    /// Field name for count/quantity.
    pub const COUNT: &str = "count";

    /// Field name for membership version.
    pub const VERSION: &str = "version";

    /// Field name for attempt number (retries).
    pub const ATTEMPT: &str = "attempt";
}

/// Span target names for Orleans components.
pub mod targets {
    /// Target for silo host operations.
    pub const SILO: &str = "orleans_host::silo";

    /// Target for grain catalog operations.
    pub const CATALOG: &str = "orleans_runtime::catalog";

    /// Target for message dispatcher operations.
    pub const DISPATCHER: &str = "orleans_runtime::dispatcher";

    /// Target for messaging operations.
    pub const MESSAGING: &str = "orleans_messaging";

    /// Target for connection management.
    pub const CONNECTION: &str = "orleans_messaging::connection";

    /// Target for membership operations.
    pub const MEMBERSHIP: &str = "orleans_clustering::membership";

    /// Target for directory operations.
    pub const DIRECTORY: &str = "orleans_directory";

    /// Target for serialization operations.
    pub const SERIALIZATION: &str = "orleans_serialization";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_conversion() {
        assert_eq!(Level::from(LogLevel::Trace), Level::TRACE);
        assert_eq!(Level::from(LogLevel::Debug), Level::DEBUG);
        assert_eq!(Level::from(LogLevel::Info), Level::INFO);
        assert_eq!(Level::from(LogLevel::Warn), Level::WARN);
        assert_eq!(Level::from(LogLevel::Error), Level::ERROR);
    }

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert_eq!(config.format, LogFormat::Compact);
        assert_eq!(config.level, LogLevel::Info);
        assert!(!config.include_location);
        assert!(!config.include_thread_ids);
        assert!(config.include_target);
        assert!(!config.include_span_events);
        assert!(config.filter_directive.is_none());
    }

    #[test]
    fn test_log_config_builder() {
        let config = LogConfig::new()
            .with_format(LogFormat::Json)
            .with_level(LogLevel::Debug)
            .with_location(true)
            .with_thread_ids(true)
            .with_target(false)
            .with_span_events(true)
            .with_filter("orleans=trace");

        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.level, LogLevel::Debug);
        assert!(config.include_location);
        assert!(config.include_thread_ids);
        assert!(!config.include_target);
        assert!(config.include_span_events);
        assert_eq!(config.filter_directive, Some("orleans=trace".to_string()));
    }

    #[test]
    fn test_log_config_development() {
        let config = LogConfig::development();
        assert_eq!(config.format, LogFormat::Pretty);
        assert_eq!(config.level, LogLevel::Debug);
        assert!(config.include_location);
    }

    #[test]
    fn test_log_config_production() {
        let config = LogConfig::production();
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.level, LogLevel::Info);
        assert!(config.include_thread_ids);
    }

    #[test]
    fn test_log_config_testing() {
        let config = LogConfig::testing();
        assert_eq!(config.format, LogFormat::Compact);
        assert_eq!(config.level, LogLevel::Warn);
        assert!(!config.include_target);
    }

    #[test]
    fn test_log_format_default() {
        assert_eq!(LogFormat::default(), LogFormat::Compact);
    }

    #[test]
    fn test_log_level_default() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
    }

    #[test]
    fn test_fields_constants() {
        // Verify field names are valid and non-empty
        assert!(!fields::SILO.is_empty());
        assert!(!fields::GRAIN_ID.is_empty());
        assert!(!fields::GRAIN_TYPE.is_empty());
        assert!(!fields::ACTIVATION_ID.is_empty());
        assert!(!fields::CORRELATION_ID.is_empty());
        assert!(!fields::INTERFACE_TYPE.is_empty());
        assert!(!fields::METHOD_ID.is_empty());
        assert!(!fields::ERROR.is_empty());
        assert!(!fields::DURATION_MS.is_empty());
        assert!(!fields::DIRECTION.is_empty());
        assert!(!fields::TARGET_SILO.is_empty());
        assert!(!fields::SOURCE_SILO.is_empty());
        assert!(!fields::STATUS.is_empty());
        assert!(!fields::COUNT.is_empty());
        assert!(!fields::VERSION.is_empty());
        assert!(!fields::ATTEMPT.is_empty());
    }

    #[test]
    fn test_targets_constants() {
        // Verify target names are valid module paths
        assert!(targets::SILO.contains("::"));
        assert!(targets::CATALOG.contains("::"));
        assert!(targets::DISPATCHER.contains("::"));
        assert!(!targets::MESSAGING.is_empty());
        assert!(targets::CONNECTION.contains("::"));
        assert!(targets::MEMBERSHIP.contains("::"));
        assert!(!targets::DIRECTORY.is_empty());
        assert!(!targets::SERIALIZATION.is_empty());
    }

    #[test]
    fn test_log_level_ordering() {
        // Verify that level conversions maintain proper ordering
        use tracing_subscriber::filter::LevelFilter;

        let trace_filter = LevelFilter::from(LogLevel::Trace);
        let debug_filter = LevelFilter::from(LogLevel::Debug);
        let info_filter = LevelFilter::from(LogLevel::Info);
        let warn_filter = LevelFilter::from(LogLevel::Warn);
        let error_filter = LevelFilter::from(LogLevel::Error);

        // More verbose levels should be "greater" (allow more logs)
        assert!(trace_filter >= debug_filter);
        assert!(debug_filter >= info_filter);
        assert!(info_filter >= warn_filter);
        assert!(warn_filter >= error_filter);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Test that tracing macros work correctly with structured fields.
    #[test]
    fn test_tracing_macros_compile() {
        // This test verifies that the re-exported macros work
        let grain_id = "test-grain-123";
        let silo = "127.0.0.1:11111";

        // These should all compile without errors
        trace!(grain_id = %grain_id, "Trace message");
        debug!(silo = %silo, grain_id = %grain_id, "Debug message");
        info!(count = 42, "Info message");
        warn!(error = "test error", "Warning message");
        error!(duration_ms = 100, "Error message");
    }

    /// Test that spans work correctly.
    #[test]
    fn test_spans_compile() {
        let span = info_span!("test_operation", grain_id = "test-123");
        let _guard = span.enter();

        // Nested span
        let inner_span = debug_span!("inner_operation", method_id = 42);
        let _inner_guard = inner_span.enter();

        info!("Inside nested spans");
    }

    /// Test instrument attribute usage pattern (compile check).
    fn _example_instrumented_function(grain_id: &str) {
        info!(grain_id = %grain_id, "Processing grain");
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Test that LogConfig builder methods are idempotent.
        #[test]
        fn prop_log_config_builder_idempotent(
            include_location in any::<bool>(),
            include_threads in any::<bool>(),
            include_target in any::<bool>(),
            include_spans in any::<bool>(),
        ) {
            let config1 = LogConfig::new()
                .with_location(include_location)
                .with_thread_ids(include_threads)
                .with_target(include_target)
                .with_span_events(include_spans);

            let config2 = LogConfig::new()
                .with_location(include_location)
                .with_thread_ids(include_threads)
                .with_target(include_target)
                .with_span_events(include_spans);

            prop_assert_eq!(config1.include_location, config2.include_location);
            prop_assert_eq!(config1.include_thread_ids, config2.include_thread_ids);
            prop_assert_eq!(config1.include_target, config2.include_target);
            prop_assert_eq!(config1.include_span_events, config2.include_span_events);
        }

        /// Test that filter directives are preserved.
        #[test]
        fn prop_filter_directive_preserved(filter in "[a-z_]+=[a-z]+") {
            let config = LogConfig::new().with_filter(filter.clone());
            prop_assert_eq!(config.filter_directive, Some(filter));
        }

        /// Test that log level conversions are consistent.
        #[test]
        fn prop_log_level_consistent(level_idx in 0usize..5) {
            let levels = [
                LogLevel::Trace,
                LogLevel::Debug,
                LogLevel::Info,
                LogLevel::Warn,
                LogLevel::Error,
            ];
            let level = levels[level_idx];

            // Test conversion to tracing Level
            let tracing_level = Level::from(level);
            let expected = match level {
                LogLevel::Trace => Level::TRACE,
                LogLevel::Debug => Level::DEBUG,
                LogLevel::Info => Level::INFO,
                LogLevel::Warn => Level::WARN,
                LogLevel::Error => Level::ERROR,
            };
            prop_assert_eq!(tracing_level, expected);
        }

        /// Test that format enum values are distinct.
        #[test]
        fn prop_log_format_distinct(format_idx in 0usize..3) {
            let formats = [
                LogFormat::Compact,
                LogFormat::Pretty,
                LogFormat::Json,
            ];
            let format = formats[format_idx];

            // Each format should not equal any other format
            for (i, other) in formats.iter().enumerate() {
                if i == format_idx {
                    prop_assert_eq!(&format, other);
                } else {
                    prop_assert_ne!(&format, other);
                }
            }
        }
    }

    /// Test that preset configurations have expected properties.
    #[test]
    fn test_preset_properties() {
        // Development preset should have debug level and pretty format
        let dev = LogConfig::development();
        assert_eq!(dev.level, LogLevel::Debug);
        assert_eq!(dev.format, LogFormat::Pretty);
        assert!(dev.include_location);

        // Production preset should have info level and JSON format
        let prod = LogConfig::production();
        assert_eq!(prod.level, LogLevel::Info);
        assert_eq!(prod.format, LogFormat::Json);
        assert!(prod.include_thread_ids);

        // Testing preset should have warn level
        let test = LogConfig::testing();
        assert_eq!(test.level, LogLevel::Warn);
        assert_eq!(test.format, LogFormat::Compact);
    }

    /// Test that all field names are valid identifiers.
    #[test]
    fn test_field_names_valid() {
        let field_names = [
            fields::SILO,
            fields::GRAIN_ID,
            fields::GRAIN_TYPE,
            fields::ACTIVATION_ID,
            fields::CORRELATION_ID,
            fields::INTERFACE_TYPE,
            fields::METHOD_ID,
            fields::ERROR,
            fields::DURATION_MS,
            fields::DIRECTION,
            fields::TARGET_SILO,
            fields::SOURCE_SILO,
            fields::STATUS,
            fields::COUNT,
            fields::VERSION,
            fields::ATTEMPT,
        ];

        for name in field_names {
            // Field names should be non-empty
            assert!(!name.is_empty(), "Field name should not be empty");
            // Field names should be valid identifiers (alphanumeric + underscore)
            assert!(
                name.chars().all(|c| c.is_alphanumeric() || c == '_'),
                "Field name '{}' contains invalid characters",
                name
            );
            // Field names should start with a letter or underscore
            assert!(
                name.chars().next().map(|c| c.is_alphabetic() || c == '_').unwrap_or(false),
                "Field name '{}' should start with letter or underscore",
                name
            );
        }
    }

    /// Test that target names follow Rust module path conventions.
    #[test]
    fn test_target_names_valid() {
        let target_names = [
            targets::SILO,
            targets::CATALOG,
            targets::DISPATCHER,
            targets::MESSAGING,
            targets::CONNECTION,
            targets::MEMBERSHIP,
            targets::DIRECTORY,
            targets::SERIALIZATION,
        ];

        for target in target_names {
            // Target names should be non-empty
            assert!(!target.is_empty(), "Target name should not be empty");
            // Target names should be valid Rust module paths
            for segment in target.split("::") {
                assert!(!segment.is_empty(), "Target '{}' has empty segment", target);
                assert!(
                    segment.chars().all(|c| c.is_alphanumeric() || c == '_'),
                    "Target '{}' segment '{}' has invalid chars",
                    target,
                    segment
                );
            }
        }
    }
}
