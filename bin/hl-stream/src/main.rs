//! `hl-stream`: stream the Hyperliquid mempool and blocks as JSON lines or over WebSocket.

mod output;
mod records;
mod serve;
mod telemetry;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use hyperliquid_stream::{
    CallbackConfig, Client, Config, MAINNET_INFO_URL, connection::STREAM_PORT,
};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

#[derive(Debug, Parser)]
#[command(name = "hl-stream", version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    network: NetworkArgs,
    #[command(flatten)]
    telemetry: telemetry::TelemetryArgs,
    #[command(subcommand)]
    command: Option<Command>,
    /// Options of `stream`, the default command, given without it.
    #[command(flatten)]
    stream: StreamArgs,
}

impl Cli {
    /// The command to run: `stream` with the top-level options when no
    /// subcommand is given.
    fn run_command(command: Option<Command>, stream: StreamArgs) -> Result<Command, clap::Error> {
        match command {
            None => Ok(Command::Stream(stream)),
            Some(_) if stream != StreamArgs::default() => {
                use clap::CommandFactory;
                Err(<Self as CommandFactory>::command().error(
                    clap::error::ErrorKind::ArgumentConflict,
                    "stream options go after `stream`, or without a subcommand",
                ))
            }
            Some(command) => Ok(command),
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the mempool, and optionally blocks, to stdout as JSON lines (default).
    Stream(StreamArgs),
    /// Serve the mempool and blocks to WebSocket subscribers.
    Serve(serve::ServeArgs),
}

#[derive(Debug, Args)]
struct NetworkArgs {
    /// Peer to try first, as HOST or HOST:PORT (repeatable; port 4001 by default).
    #[arg(long = "peer", value_name = "HOST[:PORT]", global = true)]
    peers: Vec<String>,
    /// Use only the given and cached peers.
    #[arg(long, global = true)]
    no_discovery: bool,
    /// Info API endpoint used to find root peers.
    #[arg(long, global = true, default_value = MAINNET_INFO_URL)]
    info_url: String,
    /// Address on which peers reach this host on TCP ports 4001 and 4002.
    #[arg(long, global = true, env = "HL_CALLBACK_IP", default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    callback_ip: IpAddr,
    /// Most concurrent peer sessions, used while looking for relays.
    #[arg(long, global = true, default_value_t = 4)]
    max_peers: usize,
    /// Try to keep at least this many peers that relay the mempool; peers that
    /// relay only blocks are replaced until then (0 disables the search).
    #[arg(long, global = true, env = "HL_MIN_RELAY_PEERS", default_value_t = 2)]
    min_relay_peers: usize,
    /// Replace peers whose blocks arrive later than this (seconds).
    #[arg(long, global = true, default_value_t = 30)]
    max_lag: u64,
    /// File storing good peers between runs.
    #[arg(long, global = true, env = "HL_PEERS_CACHE")]
    peers_cache: Option<PathBuf>,
    /// Interval between statistics (seconds).
    #[arg(long, global = true, default_value_t = 30)]
    stats_interval: u64,
    /// How long transaction hashes are remembered (seconds).
    #[arg(long, global = true, default_value_t = 900)]
    retention: u64,
}

#[derive(Debug, Default, PartialEq, Eq, Args)]
struct StreamArgs {
    /// Record format.
    #[arg(long, value_enum, default_value_t = Format::Actions)]
    format: Format,
    /// Only actions of these kinds, such as `order,cancel,evmRawTx`.
    #[arg(long, value_delimiter = ',', value_name = "KIND,...")]
    kinds: Vec<String>,
    /// Also write a record per block.
    #[arg(long)]
    blocks: bool,
    /// Also write periodic statistics records.
    #[arg(long)]
    stats: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum Format {
    /// One record per action.
    #[default]
    Actions,
    /// One record per transaction, with all its actions.
    Transactions,
    /// One record per transaction, with its action count per kind.
    Summary,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let command = Cli::run_command(cli.command, cli.stream).unwrap_or_else(|error| error.exit());
    telemetry::init(&cli.telemetry)?;
    let config = cli.network.config().await?;
    let mut client = Client::connect(config)
        .await
        .context("starting the stream")?;
    let result = match command {
        Command::Stream(args) => output::stream(&mut client, &args).await,
        Command::Serve(args) => serve::serve(&mut client, &args).await,
    };
    client.shutdown().await;
    result
}

/// Completes on Ctrl-C or, on Unix, SIGTERM.
pub(crate) async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "listening for Ctrl-C failed");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "listening for SIGTERM failed");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
    tracing::info!("shutting down");
}

impl NetworkArgs {
    async fn config(&self) -> Result<Config> {
        let mut peers = Vec::new();
        let mut unique = HashSet::new();
        for peer in &self.peers {
            for addr in resolve(peer).await? {
                if unique.insert(addr) {
                    peers.push(addr);
                }
            }
        }
        Ok(Config {
            peers,
            discovery: !self.no_discovery,
            info_url: self.info_url.clone(),
            max_peers: self.max_peers,
            min_relay_peers: self.min_relay_peers,
            max_lag: Duration::from_secs(self.max_lag),
            callbacks: Some(CallbackConfig {
                ip: self.callback_ip,
                ..CallbackConfig::default()
            }),
            peers_cache: self.peers_cache.clone(),
            stats_interval: Duration::from_secs(self.stats_interval),
            retention: Duration::from_secs(self.retention),
            ..Config::default()
        })
    }
}

/// Resolves `HOST` or `HOST:PORT`, with port 4001 by default.
async fn resolve(peer: &str) -> Result<Vec<SocketAddr>> {
    if let Ok(ip) = peer.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, STREAM_PORT)]);
    }
    let target = if peer.contains(':') {
        peer.to_owned()
    } else {
        format!("{peer}:{STREAM_PORT}")
    };
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(&target)
        .await
        .with_context(|| format!("resolving peer {peer}"))?
        .collect();
    anyhow::ensure!(!addrs.is_empty(), "peer {peer} has no address");
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_is_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    fn command(args: &[&str]) -> Result<Command, clap::Error> {
        let cli = Cli::try_parse_from(std::iter::once("hl-stream").chain(args.iter().copied()))?;
        Cli::run_command(cli.command, cli.stream)
    }

    #[test]
    fn stream_options_work_with_or_without_the_subcommand() {
        let expected = StreamArgs {
            kinds: vec!["evmRawTx".to_owned()],
            ..StreamArgs::default()
        };
        for args in [
            &["--peers-cache", "p.json", "--kinds", "evmRawTx"][..],
            &["--peers-cache", "p.json", "stream", "--kinds", "evmRawTx"],
            &["stream", "--kinds", "evmRawTx", "--peers-cache", "p.json"],
        ] {
            let Ok(Command::Stream(stream)) = command(args) else {
                panic!("{args:?} should stream");
            };
            assert_eq!(stream, expected, "{args:?}");
        }
        assert!(matches!(
            command(&["--peers-cache", "p.json", "serve"]),
            Ok(Command::Serve(_))
        ));
        assert!(command(&["--kinds", "evmRawTx", "serve"]).is_err());
    }

    #[tokio::test]
    async fn resolves_peers_with_default_port() {
        assert_eq!(
            resolve("16.76.120.9").await.unwrap(),
            ["16.76.120.9:4001".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            resolve("127.0.0.1:4101").await.unwrap(),
            ["127.0.0.1:4101".parse::<SocketAddr>().unwrap()]
        );
    }
}
