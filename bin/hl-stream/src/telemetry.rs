//! Logs and Prometheus metrics.

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use hyperliquid_stream::metrics::{LATENCY_BUCKETS, PREFIX};
use metrics::Gauge;
use metrics_derive::Metrics;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use metrics_util::layers::{Layer, PrefixLayer};
use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Metrics describing the process itself.
#[derive(Metrics)]
#[metrics(scope = "process")]
struct ProcessMetrics {
    /// Start time of the process, in Unix seconds.
    start_time_seconds: Gauge,
    /// Always 1; the `version` label carries the release.
    #[metric(labels = [("version", env!("CARGO_PKG_VERSION"))])]
    build_info: Gauge,
}

/// Buckets, in seconds, for the time to compress one WebSocket message.
const COMPRESSION_BUCKETS: &[f64] = &[
    0.000_02, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05,
];

const DEFAULT_LOG_FILTER: &str = "warn,hyperliquid_stream=info,hl_stream=info";

#[derive(Debug, Args)]
pub(crate) struct TelemetryArgs {
    /// Serve Prometheus metrics at http://ADDR/metrics, for example 0.0.0.0:9100.
    #[arg(long, global = true, env = "HL_METRICS_ADDR", value_name = "ADDR")]
    metrics_addr: Option<SocketAddr>,
    /// Log format on stderr; the level is set with RUST_LOG.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum LogFormat {
    /// Human-readable lines.
    Text,
    /// One JSON object per line, for log collectors such as Loki.
    Json,
}

/// Installs the log subscriber and, if requested, the metrics exporter.
pub(crate) fn init(args: &TelemetryArgs) -> Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let logs = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match args.log_format {
        LogFormat::Text => logs.init(),
        LogFormat::Json => logs.json().flatten_event(true).init(),
    }

    if let Some(addr) = args.metrics_addr {
        let (recorder, exporter) = PrometheusBuilder::new()
            .with_http_listener(addr)
            .set_buckets_for_metric(Matcher::Suffix("lag_seconds".to_owned()), LATENCY_BUCKETS)
            .and_then(|builder| {
                builder.set_buckets_for_metric(
                    Matcher::Suffix("inclusion_delay_seconds".to_owned()),
                    LATENCY_BUCKETS,
                )
            })
            .and_then(|builder| {
                builder.set_buckets_for_metric(
                    Matcher::Suffix("compression_duration_seconds".to_owned()),
                    COMPRESSION_BUCKETS,
                )
            })
            .context("configuring metric buckets")?
            .build()
            .with_context(|| format!("serving metrics on {addr}"))?;
        tokio::spawn(async move {
            if let Err(error) = exporter.await {
                warn!(?error, "the metrics endpoint stopped");
            }
        });
        // Every metric is named `hl_stream_<scope>_<name>`.
        metrics::set_global_recorder(PrefixLayer::new(PREFIX).layer(recorder))
            .context("installing the metrics recorder")?;
        let process = ProcessMetrics::default();
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |elapsed| elapsed.as_secs_f64());
        process.start_time_seconds.set(started);
        process.build_info.set(1.0);
        info!(%addr, "serving Prometheus metrics at /metrics");
    }
    Ok(())
}
