use akula::{
    akula_tracing::{self, Component},
    binutil::AkulaDataDir,
    consensus::{engine_factory, Consensus, ForkChoiceMode},
    kv::tables::CHAINDATA_TABLES,
    models::*,
    p2p::node::NodeBuilder,
    rpc::{
        debug::DebugApiServerImpl, erigon::ErigonApiServerImpl, eth::EthApiServerImpl,
        net::NetApiServerImpl, otterscan::OtterscanApiServerImpl, parity::ParityApiServerImpl,
        trace::TraceApiServerImpl, web3::Web3ApiServerImpl,
    },
    snapshot::Snapshotter,
    stagedsync,
    stages::{stage_util::IndexParams, *},
    version_string,
};
use anyhow::Context;
use clap::Parser;
use ethereum_jsonrpc::{
    ErigonApiServer, EthApiServer, NetApiServer, OtterscanApiServer, ParityApiServer,
    TraceApiServer, Web3ApiServer,
};
use expanded_pathbuf::ExpandedPathBuf;
use http::Uri;
use jsonrpsee::{core::server::rpc_module::Methods, server::ServerBuilder};
use signal_hook::{
    consts::*,
    flag,
    iterator::{exfiltrator::SignalOnly, SignalsInfo},
};
use std::{
    collections::HashSet,
    fs::OpenOptions,
    future::pending,
    io::Write,
    net::SocketAddr,
    panic,
    path::PathBuf,
    process::{exit, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Mutex as AsyncMutex, time::sleep};
use tracing::*;
use tracing_subscriber::prelude::*;
use transmission_rpc::{
    types::{TorrentAddArgs, TorrentAddedOrDuplicate, TorrentSetArgs, TrackerList},
    TransClient,
};
use url::Url;

const TRACKERS: &[&str] = &[
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://9.rarbg.com:2810/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "https://opentracker.i2p.rocks:443/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://explodie.org:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://chouchou.top:8080/announce",
    "udp://bt.oiyo.tk:6969/announce",
    "https://tracker.nanoha.org:443/announce",
    // "https://tracker.lilithraws.org:443/announce",
    "http://tracker3.ctix.cn:8080/announce",
    "http://tracker.mywaifu.best:6969/announce",
];

#[derive(Parser)]
#[clap(name = "Akula", about = "Next-generation Ethereum implementation.")]
pub struct Opt {
    /// Path to database directory.
    #[clap(long, help = "Database directory path", default_value_t)]
    pub datadir: AkulaDataDir,

    #[clap(long, default_value = "transmission-daemon")]
    pub transmission_daemon_path: ExpandedPathBuf,

    /// Path to snapshot directory.
    #[clap(long = "snapshotdir")]
    pub snapshot_dir: Option<ExpandedPathBuf>,

    /// Name of the network to join
    #[clap(long)]
    pub chain: Option<String>,

    /// Chain specification file to use
    #[clap(long)]
    pub chain_spec_file: Option<ExpandedPathBuf>,

    #[clap(long, default_value = "30091")]
    pub transmission_daemon_port: u16,

    #[clap(long, default_value = "42069")]
    pub transmission_peer_port: u16,

    /// Sentry GRPC service URL
    #[clap(long, help = "Sentry GRPC service URLs as 'http://host:port'")]
    pub sentry_api_addr: Option<String>,

    #[clap(flatten)]
    pub sentry_opts: akula::sentry::Opts,

    /// Last block where to sync to.
    #[clap(long)]
    pub max_block: Option<BlockNumber>,

    /// Start with unwinding to this block.
    #[clap(long)]
    pub start_with_unwind: Option<BlockNumber>,

    /// Turn on pruning.
    #[clap(long)]
    pub prune: bool,

    /// Use incremental staged sync.
    #[clap(long)]
    pub increment: Option<BlockNumber>,

    /// Sender recovery batch size (blocks)
    #[clap(long, default_value = "500000")]
    pub sender_recovery_batch_size: u64,

    /// Execution batch size (Ggas).
    #[clap(long, default_value = "5000")]
    pub execution_batch_size: u64,

    /// Execution history batch size (Ggas).
    #[clap(long, default_value = "250")]
    pub execution_history_batch_size: u64,

    /// Exit execution stage after batch.
    #[clap(long)]
    pub execution_exit_after_batch: bool,

    /// Skip commitment (state root) verification.
    #[clap(long)]
    pub skip_commitment: bool,

    /// Exit Akula after sync is complete and there's no progress.
    #[clap(long)]
    pub exit_after_sync: bool,

    /// Delay applied at the terminating stage.
    #[clap(long, default_value = "0")]
    pub delay_after_sync: u64,

    /// Disable JSONRPC.
    #[clap(long)]
    pub no_rpc: bool,

    /// Enable API options
    #[clap(long)]
    pub enable_api: Option<String>,

    /// Enable JSONRPC at this IP address and port.
    #[clap(long, default_value = "127.0.0.1:8545")]
    pub rpc_listen_address: String,

    /// Enable gRPC at this IP address and port.
    #[clap(long, default_value = "127.0.0.1:7545")]
    pub grpc_listen_address: SocketAddr,

    /// Enable CL engine RPC at this IP address and port.
    #[clap(long, default_value = "127.0.0.1:8551")]
    pub engine_listen_address: SocketAddr,

    /// Path to JWT secret file.
    #[clap(long)]
    pub jwt_secret_path: Option<ExpandedPathBuf>,
}

#[allow(unreachable_code)]
fn main() -> anyhow::Result<()> {
    // Make sure double CTRL+C and similar kills
    let term_now = Arc::new(AtomicBool::new(false));
    let mut sigs = vec![SIGHUP];
    sigs.extend(TERM_SIGNALS);

    for sig in &sigs {
        flag::register(*sig, Arc::clone(&term_now))?;
    }

    let mut signals = SignalsInfo::<SignalOnly>::new(&sigs)?;

    // Subscribe to all these signals with information about where they come from. We use the
    // extra info only for logging in this example (it is not available on all the OSes or at
    // all the occasions anyway, it may return `Unknown`).

    let mut opt: Opt = Opt::parse();
    fdlimit::raise_fd_limit();

    akula_tracing::build_subscriber(Component::Core).init();

    std::thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(128 * 1024 * 1024)
                .build()?;

            rt.block_on(async move {
                info!("Starting Akula ({})", version_string());

                let mut bundled_chain_spec = false;
                let chain_config = if let Some(chain) = opt.chain {
                    bundled_chain_spec = true;
                    Some(ChainSpec::load_builtin(&chain)?)
                } else if let Some(path) = opt.chain_spec_file {
                    Some(ChainSpec::load_from_file(path)?)
                } else {
                    None
                };

                std::fs::create_dir_all(&opt.datadir.0)?;
                let akula_chain_data_dir = opt.datadir.chain_data_dir();
                let etl_temp_path = opt.datadir.etl_temp_dir();
                let _ = std::fs::remove_dir_all(&etl_temp_path);
                std::fs::create_dir_all(&etl_temp_path)?;
                let etl_temp_dir = Arc::new(
                    tempfile::tempdir_in(&etl_temp_path)
                        .context("failed to create ETL temp dir")?,
                );
                let db = Arc::new(akula::kv::new_database(
                    &CHAINDATA_TABLES,
                    &akula_chain_data_dir,
                )?);

                akula::database_version::migrate_database(&db)?;

                if opt.prune {
                    warn!("Pruning is temporarily unavailable");
                    opt.prune = false;
                }

                let transmission_daemon_addr: Url = format!(
                    "http://localhost:{}/transmission/rpc",
                    opt.transmission_daemon_port
                )
                .parse()?;

                {
                    let mut btclient = TransClient::new(transmission_daemon_addr.clone());

                    if btclient.session_close().await.is_ok() {
                        info!("Waiting for existing transmission-daemon instance to close");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }

                let snapshot_dir = opt
                    .snapshot_dir
                    .map(|v| v.0)
                    .unwrap_or_else(|| opt.datadir.snapshot());

                let tmpconfig = tempfile::tempdir().unwrap();
                let btdaemon = Command::new(opt.transmission_daemon_path)
                    .args(&[
                        "-f",
                        "--no-auth",
                        "--config-dir",
                        &tmpconfig.path().to_string_lossy(),
                        "--port",
                        &opt.transmission_daemon_port.to_string(),
                        "--peerport",
                        &opt.transmission_peer_port.to_string(),
                        "--dht",
                        "--lpd",
                        "--utp",
                        "--rpc-bind-address",
                        "0.0.0.0",
                        "--allowed",
                        "127.0.0.1,::1",
                        "-et",
                    ])
                    .stdin(Stdio::piped())
                    .spawn()
                    .context("Failed to start transmission-daemon")?;

                let shutting_down = Arc::new(AtomicBool::new(false));

                std::thread::spawn({
                    let shutting_down = shutting_down.clone();
                    let transmission_daemon_addr = transmission_daemon_addr.clone();
                    move || {
                        for sig in &mut signals {
                            // Will print info about signal + where it comes from.
                            info!("Received signal {:?}", sig);

                            shutting_down.store(true, Ordering::SeqCst);

                            tokio::runtime::Runtime::new().unwrap().block_on({
                                let transmission_daemon_addr = transmission_daemon_addr.clone();
                                async move {
                                    let _ = tokio::time::timeout(
                                        Duration::from_secs(5),
                                        TransClient::new(transmission_daemon_addr.clone())
                                            .session_close(),
                                    )
                                    .await;

                                    exit(0)
                                }
                            });
                        }
                    }
                });

                std::thread::spawn(move || {
                    let res = btdaemon.wait_with_output();

                    if !shutting_down.load(Ordering::SeqCst) {
                        panic!("transmission-daemon unexpectedly exited: {res:?}, shutting down");
                    }
                });

                tokio::time::sleep(Duration::from_secs(5)).await;

                let (torrent_tx, mut torrent_rx) = tokio::sync::mpsc::channel(1);
                tokio::spawn({
                    let snapshot_base_path = snapshot_dir.clone();
                    async move {
                        while let Some((snapshot_name, snapshot_path)) = torrent_rx.recv().await {
                            let torrent_file_name = format!("{snapshot_name}.torrent");
                            let torrent_path = snapshot_base_path.join(&torrent_file_name);

                            let snapshot_path: PathBuf = snapshot_path;

                            let _ = TRACKERS;

                            let rsp = TransClient::new(transmission_daemon_addr.clone())
                                .torrent_add(TorrentAddArgs {
                                    filename: Some(torrent_path.to_string_lossy().to_string()),
                                    download_dir: Some(snapshot_path.to_string_lossy().to_string()),
                                    ..Default::default()
                                })
                                .await
                                .unwrap();

                            assert!(rsp.is_ok());

                            if let TorrentAddedOrDuplicate::TorrentAdded(handle) = rsp.arguments {
                                assert!(TransClient::new(transmission_daemon_addr.clone())
                                    .torrent_set(
                                        TorrentSetArgs {
                                            tracker_list: Some(TrackerList(
                                                TRACKERS.iter().map(|s| s.to_string()).collect(),
                                            )),
                                            ..Default::default()
                                        },
                                        None
                                    )
                                    .await
                                    .unwrap()
                                    .is_ok());

                                info!("Started torrent: {:?}: {:?}", handle.id, handle.name);
                            }
                        }
                    }
                });

                let header_snapshotter =
                    Arc::new(AsyncMutex::new(Snapshotter::new(&snapshot_dir,  &db.begin()?)?));
                let body_snapshotter = Arc::new(AsyncMutex::new(Snapshotter::new(&snapshot_dir, &db.begin()?)?));
                let sender_snapshotter =
                    Arc::new(AsyncMutex::new(Snapshotter::new(&snapshot_dir, &db.begin()?)?));

                {
                    let header_snapshotter = header_snapshotter.lock().await;
                    let body_snapshotter = body_snapshotter.lock().await;
                    let sender_snapshotter = sender_snapshotter.lock().await;

                    let mut snapshots_count = 0;
                    for snapshot_paths in [
                        header_snapshotter.snapshot_paths(),
                        body_snapshotter.snapshot_paths(),
                        sender_snapshotter.snapshot_paths(),
                    ] {
                        for (snapshot_name, _) in snapshot_paths {
                            torrent_tx
                                .send((snapshot_name, snapshot_dir.clone()))
                                .await
                                .unwrap();

                            snapshots_count += 1;
                        }
                    }

                    info!("Added {snapshots_count} snapshots to torrent engine");
                }

                let chainspec = {
                    let span = span!(Level::INFO, "", " Genesis initialization ");
                    let _g = span.enter();
                    let txn = db.begin_mutable()?;
                    let (chainspec, initialized) = akula::genesis::initialize_genesis(
                        &txn,
                        &etl_temp_dir,
                        bundled_chain_spec,
                        chain_config,
                    )?;
                    if initialized {
                        txn.commit()?;
                    }

                    chainspec
                };

                info!("Current network: {}", chainspec.name);

                let jwt_secret_path = opt
                    .jwt_secret_path
                    .map(|v| v.0)
                    .unwrap_or_else(|| opt.datadir.0.join("jwt.hex"));
                if let Ok(mut file) = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(jwt_secret_path)
                {
                    file.write_all(
                        hex::encode(
                            std::iter::repeat_with(rand::random)
                                .take(32)
                                .collect::<Vec<_>>(),
                        )
                        .as_bytes(),
                    )?;
                    file.flush()?;
                }

                let consensus: Arc<dyn Consensus> = engine_factory(
                    Some(db.clone()),
                    chainspec.clone(),
                    Some(opt.engine_listen_address),
                )?
                .into();

                let network_id = chainspec.params.network_id;

                let chain_config = ChainConfig::from(chainspec);

                if !opt.no_rpc {
                    tokio::spawn({
                        let db = db.clone();
                        async move {
                            let jsonrpc_server = ServerBuilder::default()
                                .build(&opt.rpc_listen_address)
                                .await
                                .unwrap();

                            let mut api = Methods::new();

                            let api_options = opt
                                .enable_api
                                .map(|v| {
                                    v.split(',')
                                        .into_iter()
                                        .map(|s| s.to_lowercase())
                                        .collect::<HashSet<String>>()
                                })
                                .unwrap_or_default();

                            if api_options.is_empty() || api_options.contains("eth") {
                                api.merge(
                                    EthApiServerImpl {
                                        db: db.clone(),
                                        call_gas_limit: 100_000_000,
                                    }
                                    .into_rpc(),
                                )
                                .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("net") {
                                api.merge(NetApiServerImpl { network_id }.into_rpc())
                                    .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("erigon") {
                                api.merge(ErigonApiServerImpl { db: db.clone() }.into_rpc())
                                    .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("otterscan") {
                                api.merge(OtterscanApiServerImpl { db: db.clone() }.into_rpc())
                                    .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("parity") {
                                api.merge(ParityApiServerImpl { db: db.clone() }.into_rpc())
                                    .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("trace") {
                                api.merge(
                                    TraceApiServerImpl {
                                        db: db.clone(),
                                        call_gas_limit: 100_000_000,
                                    }
                                    .into_rpc(),
                                )
                                .unwrap();
                            }

                            if api_options.is_empty() || api_options.contains("web3") {
                                api.merge(Web3ApiServerImpl.into_rpc()).unwrap();
                            }

                            let _jsonrpc_server_handle = jsonrpc_server.start(api.clone()).unwrap();
                            info!("JSONRPC server listening on {}", opt.rpc_listen_address);

                            pending::<()>().await
                        }
                    });

                    tokio::spawn({
                        let db = db.clone();
                        async move {
                            info!("Starting gRPC server on {}", opt.grpc_listen_address);
                            let mut builder = tonic::transport::Server::builder();

                            #[cfg(feature = "grpc-reflection")]
                            builder.add_service(
                                tonic_reflection::server::Builder::configure()
                                    .register_encoded_file_descriptor_set(
                                        ethereum_interfaces::FILE_DESCRIPTOR_SET,
                                    )
                                    .build()
                                    .unwrap(),
                            );

                            builder.add_service(
                                ethereum_interfaces::web3::debug_api_server::DebugApiServer::new(
                                    DebugApiServerImpl {
                                        db: db.clone(),
                                    }
                                )
                            )
                            .add_service(
                                ethereum_interfaces::web3::trace_api_server::TraceApiServer::new(
                                    TraceApiServerImpl {
                                        db,
                                        call_gas_limit: 100_000_000,
                                    },
                                ),
                            )
                            .serve(opt.grpc_listen_address)
                            .await
                            .unwrap();
                        }
                    });
                }

                let increment = opt.increment.or({
                    if opt.prune {
                        Some(BlockNumber(90_000))
                    } else {
                        None
                    }
                });

                // staged sync setup
                let mut staged_sync = stagedsync::StagedSync::new();
                staged_sync.set_min_progress_to_commit_after_stage(if opt.prune {
                    u64::MAX
                } else {
                    1024
                });
                if opt.prune {
                    staged_sync.set_pruning_interval(90_000);
                }
                staged_sync.set_max_block(opt.max_block);
                staged_sync.start_with_unwind(opt.start_with_unwind);
                staged_sync.set_exit_after_sync(opt.exit_after_sync);

                if opt.delay_after_sync > 0 {
                    staged_sync
                        .set_delay_after_sync(Some(Duration::from_millis(opt.delay_after_sync)));
                }

                let sentries = if let Some(raw_str) = opt.sentry_api_addr {
                    raw_str
                        .split(',')
                        .filter_map(|s| s.parse::<Uri>().ok())
                        .collect::<Vec<_>>()
                } else {
                    let max_peers = opt.sentry_opts.max_peers;
                    let sentry_api_addr = opt.sentry_opts.sentry_addr;
                    let swarm = akula::sentry::run(
                        opt.sentry_opts,
                        opt.datadir,
                        chain_config.chain_spec.p2p.clone(),
                    )
                    .await?;

                    let current_stage = staged_sync.current_stage();

                    tokio::spawn(async move {
                        loop {
                            if let Some(stage) = *current_stage.borrow() {
                                if stage == HEADERS || stage == BODIES {
                                    info!(
                                        "P2P node peer info: {} active (+{} dialing) / {} max.",
                                        swarm.connected_peers(),
                                        swarm.dialing(),
                                        max_peers
                                    );
                                }
                            }

                            sleep(Duration::from_secs(5)).await;
                        }
                    });

                    vec![format!("http://{sentry_api_addr}").parse()?]
                };

                let mut builder = NodeBuilder::new(chain_config.clone()).set_stash(db.clone());
                for sentry_api_addr in sentries {
                    builder = builder.add_sentry(sentry_api_addr);
                }

                let node = Arc::new(builder.build()?);
                let tip_discovery =
                    !matches!(consensus.fork_choice_mode(), ForkChoiceMode::External(_));

                tokio::spawn({
                    let node = node.clone();
                    async move {
                        node.start_sync(tip_discovery).await.unwrap();
                    }
                });

                staged_sync.push(
                    HeaderDownload {
                        node: node.clone(),
                        consensus: consensus.clone(),
                        max_block: opt.max_block.unwrap_or_else(|| u64::MAX.into()),
                        increment,
                    },
                    false,
                );
                staged_sync.push(TotalGasIndex, false);
                staged_sync.push(
                    BlockHashes {
                        temp_dir: etl_temp_dir.clone(),
                    },
                    false,
                );
                staged_sync.push(
                    HeaderSnapshot {
                        snapshotter: header_snapshotter,
                        bt_sender: torrent_tx.clone(),
                    },
                    false,
                );
                staged_sync.push(BodyDownload { node, consensus }, false);
                staged_sync.push(TotalTxIndex, false);
                staged_sync.push(
                    BodySnapshot {
                        snapshotter: body_snapshotter,
                        bt_sender: torrent_tx.clone(),
                    },
                    false,
                );
                staged_sync.push(
                    SenderRecovery {
                        batch_size: opt.sender_recovery_batch_size.try_into().unwrap(),
                    },
                    false,
                );
                staged_sync.push(
                    SenderSnapshot {
                        snapshotter: sender_snapshotter,
                        bt_sender: torrent_tx,
                    },
                    false,
                );
                staged_sync.push(
                    Execution {
                        max_block: opt.max_block,
                        batch_size: opt.execution_batch_size.saturating_mul(1_000_000_000_u64),
                        history_batch_size: opt
                            .execution_history_batch_size
                            .saturating_mul(1_000_000_000_u64),
                        exit_after_batch: opt.execution_exit_after_batch,
                        batch_until: None,
                        commit_every: None,
                    },
                    false,
                );
                if !opt.skip_commitment {
                    staged_sync.push(HashState::new(etl_temp_dir.clone(), None), !opt.prune);
                    staged_sync.push_with_unwind_priority(
                        Interhashes::new(etl_temp_dir.clone(), None),
                        !opt.prune,
                        1,
                    );
                }
                let index_params = IndexParams {
                    temp_dir: etl_temp_dir.clone(),
                    flush_interval: 50_000,
                };
                staged_sync.push(AccountHistoryIndex(index_params.clone()), !opt.prune);
                staged_sync.push(StorageHistoryIndex(index_params.clone()), !opt.prune);
                staged_sync.push(LogTopicIndex(index_params.clone()), !opt.prune);
                staged_sync.push(LogAddressIndex(index_params.clone()), !opt.prune);
                staged_sync.push(
                    TxLookup {
                        temp_dir: etl_temp_dir.clone(),
                    },
                    !opt.prune,
                );
                staged_sync.push(
                    CallTraceIndex {
                        temp_dir: etl_temp_dir.clone(),
                        flush_interval: 50_000,
                    },
                    !opt.prune,
                );
                staged_sync.push(Finish, !opt.prune);

                info!("Running staged sync");
                staged_sync.run(&db).await?;

                if opt.exit_after_sync {
                    Ok(())
                } else {
                    pending().await
                }
            })
        })?
        .join()
        .unwrap_or_else(|e| panic::resume_unwind(e))
}
