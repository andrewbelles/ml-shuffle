//! 
//! src/logging.rs  Andrew Belles  Sept 13th, 2025 
//!
//! Initializes logger, includes methods for calling logger 
//! and ensuring that crawler gives informative outputs while running 
//!
//!

use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use tracing_error::ErrorLayer; 
use tracing_appender::non_blocking; 

use crate::config::LoggingConfig;

pub struct LoggingGuard(tracing_appender::non_blocking::WorkerGuard);

pub fn init_logging(cfg: &LoggingConfig) -> 
    Result<LoggingGuard, crate::errors::CrawlerError> {
    
    let (writer, guard) = non_blocking(std::io::stdout());
    let filter = std::env::var("RUST_LOG")
        .ok()
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::new(cfg.filter_directives.clone()));

    let time = tracing_subscriber::fmt::time::UtcTime::rfc_3339();
    let fmt_layer = fmt::layer()
        .with_writer(writer)
        .with_timer(time)
        .with_target(cfg.include_target)
        .with_file(cfg.include_file_line)
        .with_line_number(cfg.include_file_line)
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(true);

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(ErrorLayer::default());

    registry.init();
    Ok( LoggingGuard(guard) )
}
