use akula::{
    akula_tracing::{self, Component},
    binutil::AkulaDataDir,
    consensus::{engine_factory, Consensus, ForkChoiceMode},
    kv::{
        tables::{self, CHAINDATA_TABLES},
        traits::Table,
    },
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
use anyhow::{bail, Context};
use bytesize::ByteSize;
use clap::Parser;
use ethereum_jsonrpc::{
    ErigonApiServer, EthApiServer, NetApiServer, OtterscanApiServer, ParityApiServer,
    TraceApiServer, Web3ApiServer,
};
use expanded_pathbuf::ExpandedPathBuf;
use futures::{future::BoxFuture, stream::FuturesUnordered};
use http::Uri;
use itertools::Itertools;
use jsonrpsee::{core::server::rpc_module::Methods, server::ServerBuilder};
use parking_lot::Mutex;
use signal_hook::{
    consts::*,
    flag,
    iterator::{exfiltrator::SignalOnly, SignalsInfo},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::OpenOptions,
    future::pending,
    io::Write,
    net::SocketAddr,
    panic,
    process::{exit, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot::Sender as OneshotSender, Mutex as AsyncMutex},
    time::sleep,
};
use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::prelude::*;
use transmission_rpc::{
    types::{TorrentAddArgs, TorrentAddedOrDuplicate, TorrentGetField, TorrentSetArgs},
    TransClient,
};
use url::Url;

const TORRENT_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

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

                // Start iroh
                let mut config = iroh_one::config::Config::default();
                config.store.path = opt.datadir.snapshotdb();

                let (store_recv, store_sender) = iroh_rpc_types::Addr::new_mem();
                config.rpc_client.store_addr = Some(store_sender);
                let _store_rpc = iroh_one::mem_store::start(store_recv, config.clone().store).await?;

                let (p2p_recv, p2p_sender) = iroh_rpc_types::Addr::new_mem();
                config.rpc_client.p2p_addr = Some(p2p_sender);
                let _p2p_rpc = iroh_one::mem_p2p::start(p2p_recv, config.clone().p2p).await?;

                let (gateway_recv, gateway_sender) = iroh_rpc_types::Addr::new_mem();

                config.rpc_client.gateway_addr = Some(gateway_sender);
                config.synchronize_subconfigs();

                let content_loader = iroh_resolver::racing::RacingLoader::new(
                    iroh_rpc_client::Client::new(config.rpc_client.clone()).await?,
                    config.gateway.http_resolvers.clone().unwrap_or_default(),
                );
                let shared_state = iroh_gateway::core::Core::make_state(
                    Arc::new(config.clone()),
                    Arc::new(None),
                    content_loader,
                )
                .await?;

                let _handler = iroh_gateway::core::Core::new_with_state(gateway_recv, Arc::clone(&shared_state)).await?;

                let rpc_client = iroh_rpc_client::Client::new(config.rpc_client.clone()).await?;

                // Start Transmission

                let transmission_daemon_addr: Url = format!(
                    "http://localhost:{}/transmission/rpc",
                    opt.transmission_daemon_port
                )
                .parse()?;

                {
                    let mut btclient = TransClient::new(transmission_daemon_addr.clone());

                    if btclient.session_close().await.is_ok() {
                        info!("Waiting for existing transmission-daemon instance to close");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }

                let snapshot_dir = opt
                    .snapshot_dir
                    .map(|v| v.0)
                    .unwrap_or_else(|| opt.datadir.snapshot());

                let tmpconfig = tempfile::tempdir().unwrap();
                let logfile = opt.datadir.0.join("transmission.log");
                let _ = std::fs::remove_file(&logfile);
                std::fs::write(tmpconfig.path().join("settings.json").to_string_lossy().into_owned(), serde_json::json!({
                    "encryption": 0,
                    "lpd-enabled": false,
                    "download-queue-enabled": false,
                    "peer-limit-global": 2500,
                    "peer-limit-per-torrent": 25,
                    "rename-partial-files": false,
                }).to_string())?;
                let btdaemon = Command::new(opt.transmission_daemon_path)
                    .args(&[
                        "-f",
                        "--no-auth",
                        "-e",
                        &logfile.to_string_lossy(),
                        "--log-debug",
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
                        error!("transmission-daemon unexpectedly exited: {res:?}, shutting down");

                        std::process::exit(1);
                    }
                });

                tokio::time::sleep(Duration::from_secs(5)).await;

                // For torrents added before staged sync
                let (torrent_tx, mut torrent_rx) =
                    tokio::sync::mpsc::channel::<(H160, TorrentData, Option<OneshotSender<i64>>, Option<Weak<Mutex<Option<transmission_rpc::types::Torrent>>>>)>(1);

                // For new torrents added at snapshot stages
                let (stage_snapshot_tx, mut stage_snapshot_rx) = tokio::sync::mpsc::channel::<(H160, Vec<u8>)>(1);
                tokio::spawn({
                    let snapshot_dir = snapshot_dir.clone();
                    async move {
                        let mut client = TransClient::new(transmission_daemon_addr.clone());
                        while let (Some((info_hash, torrent_data, id_sender, status_to_update)), _, _) = futures::future::select_all([Box::pin(torrent_rx.recv()) as BoxFuture<'_, Option<(H160, TorrentData, Option<OneshotSender<i64>>, Option<Weak<Mutex<Option<transmission_rpc::types::Torrent>>>>)>>, Box::pin(futures::FutureExt::map(stage_snapshot_rx.recv(), |opt| {
                            opt.map(|(info_hash, torrent_data)| (info_hash, TorrentData::Downloaded(torrent_data), None, None))
                        }))]).await {
                            let mut torrent_add_args = TorrentAddArgs {
                                download_dir: Some(snapshot_dir.to_string_lossy().to_string()),
                                ..Default::default()
                            };

                            let mut send_torrent_data_tx: Option<tokio::sync::oneshot::Sender<Vec<u8>>> = None;

                            match torrent_data {
                                TorrentData::Downloaded(torrent_bytes) => {
                                    torrent_add_args.metainfo = Some(base64::encode(&torrent_bytes));
                                }
                                TorrentData::NotDownloaded(tx) => {
                                    torrent_add_args.filename =
                                    Some(format!("magnet:?xt=urn:btih:{}", hex::encode(info_hash)));
                                    send_torrent_data_tx = Some(tx);
                                }
                            }

                            debug!("{torrent_add_args:?}");

                            let rsp = client
                                .torrent_add(torrent_add_args)
                                .await
                                .unwrap();

                            assert!(rsp.is_ok());

                            if let TorrentAddedOrDuplicate::TorrentAdded(handle) = rsp.arguments {
                                client
                                    .torrent_set(
                                        TorrentSetArgs {
                                            tracker_add: Some(
                                                TRACKERS.iter().map(|s| s.to_string()).collect(),
                                            ),
                                            ..Default::default()
                                        },
                                        None
                                    )
                                    .await
                                    .unwrap();

                                info!("Started torrent: {:?}: {:?}", handle.id, handle.name);

                                let id = handle.id.unwrap();

                                if let Some(status_to_update) = status_to_update {
                                    tokio::spawn({
                                        let transmission_daemon_addr = transmission_daemon_addr.clone();
                                        async move {
                                            let mut client = TransClient::new(transmission_daemon_addr);
                                            while let Some(status_to_update) = status_to_update.upgrade() {
                                                let v = client.torrent_get(Some(vec![TorrentGetField::Status, TorrentGetField::SizeWhenDone, TorrentGetField::PercentDone]), Some(vec![transmission_rpc::types::Id::Id(id)])).await.unwrap().arguments.torrents[0].clone();

                                                *status_to_update.lock() = Some(v);

                                                tokio::time::sleep(TORRENT_STATUS_REFRESH_INTERVAL).await;
                                            }
                                        }
                                    });
                                }

                                if let Some(send_torrent_data_tx) = send_torrent_data_tx {
                                    let transmission_daemon_addr = transmission_daemon_addr.clone();
                                    tokio::spawn(async move {
                                        let mut client = TransClient::new(transmission_daemon_addr);
                                        loop {
                                            let res = client.torrent_get(Some(vec![TorrentGetField::MetadataPercentComplete, TorrentGetField::TorrentFile]), Some(vec![transmission_rpc::types::Id::Id(id)])).await.unwrap().arguments.torrents[0].clone();

                                            if res.metadata_percent_complete.unwrap() == 1.0 {
                                                send_torrent_data_tx.send(std::fs::read(res.torrent_file.unwrap()).unwrap()).unwrap();
                                                break;
                                            }

                                            tokio::time::sleep(TORRENT_STATUS_REFRESH_INTERVAL).await;
                                        }
                                    });
                                }

                                if let Some(id_sender) = id_sender {
                                    let _ = id_sender.send(id);
                                }
                            } else {
                                unreachable!("duplicate torrent");
                            }
                        }
                    }
                });

                {
                    let mut tx = db.begin_mutable()?;

                    let mut snapshots_count = 0;
                    let mut new_torrent_data = FuturesUnordered::<BoxFuture<'static, (H160, Vec<u8>)>>::new();
                    let mut all_snapshots = HashMap::<i64, Arc<Mutex<Option<transmission_rpc::types::Torrent>>>>::new();

                    for (table, preverified_snapshots) in [
                        (
                            Arc::new(tables::HeaderSnapshot)
                                as Arc<dyn Table<Key = u64, Value = H160, SeekKey = u64>>,
                            &chainspec.snapshots.headers,
                        ),
                        (Arc::new(tables::BodySnapshot), &chainspec.snapshots.bodies),
                        (
                            Arc::new(tables::SenderSnapshot),
                            &chainspec.snapshots.senders,
                        ),
                    ] {
                        let mut new_snapshots = BTreeMap::new();

                        let walker = tx.cursor(table.clone())?.walk(None);
                        for it in
                            walker.zip_longest(preverified_snapshots.iter().copied().enumerate())
                        {
                            let (id_sender_tx, id_sender_rx) = tokio::sync::oneshot::channel();
                            let status_to_update = Arc::new(Mutex::new(None));
                            match it {
                                itertools::EitherOrBoth::Both(
                                    downloaded_snapshot,
                                    (_, preverified_info_hash),
                                ) => {
                                    let (idx, info_hash) = downloaded_snapshot?;

                                    if info_hash != preverified_info_hash {
                                        bail!("Downloaded infohash #{idx}/{info_hash:?} does not match preverified one ({preverified_info_hash:?}");
                                    }

                                    torrent_tx.send((info_hash, if let Some(torrent_bytes) = tx.get(tables::Torrents, info_hash)? {
                                        TorrentData::Downloaded(torrent_bytes)
                                    } else {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        new_torrent_data.push(Box::pin(futures::FutureExt::map(rx, move |data| (info_hash, data.unwrap()))));

                                        TorrentData::NotDownloaded(tx)
                                    }, Some(id_sender_tx), Some(Arc::downgrade(&status_to_update)))).await.unwrap();
                                }
                                itertools::EitherOrBoth::Left(downloaded_snapshot) => {
                                    // We're past embedded snapshots
                                    let (_, info_hash) = downloaded_snapshot?;

                                    torrent_tx.send((info_hash, if let Some(torrent_bytes) = tx.get(tables::Torrents, info_hash)? {
                                        TorrentData::Downloaded(torrent_bytes)
                                    } else {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        new_torrent_data.push(Box::pin(futures::FutureExt::map(rx, move |data: Result<Vec<u8>, _>| (info_hash, data.unwrap()))));

                                        TorrentData::NotDownloaded(tx)
                                    }, Some(id_sender_tx), Some(Arc::downgrade(&status_to_update)))).await.unwrap();
                                },
                                itertools::EitherOrBoth::Right((idx, preverified_info_hash)) => {
                                    // We have more snapshots to download according to chainspec

                                    new_snapshots.insert(idx, preverified_info_hash);
                                    torrent_tx.send((preverified_info_hash, {
                                        let (tx, rx) = tokio::sync::oneshot::channel();
                                        new_torrent_data.push(Box::pin(futures::FutureExt::map(rx, move |data| (preverified_info_hash, data.unwrap()))));

                                        TorrentData::NotDownloaded(tx)
                                    }, Some(id_sender_tx), Some(Arc::downgrade(&status_to_update)))).await.unwrap();
                                }
                            }

                            all_snapshots.insert(id_sender_rx.await.unwrap(), status_to_update);

                            snapshots_count += 1;
                        }

                        if !new_snapshots.is_empty() {
                            let mut cursor = tx.cursor(table)?;
                            for (&idx, &info_hash) in new_snapshots.iter() {
                                cursor.append(idx as u64, info_hash)?;
                            }

                            info!("Registered {} new shapshots from chainspec", new_snapshots.len());

                            tx.commit()?;
                            tx = db.begin_mutable()?;
                        }
                    }


                    info!("Added {snapshots_count} snapshots to torrent engine");

                    // Wait for all snapshot metadata to be saved
                    while let Some((info_hash, data)) = new_torrent_data.next().await {
                        tx.set(tables::Torrents, info_hash, data)?;

                        tx.commit()?;
                        tx = db.begin_mutable()?;
                        info!("Downloaded metadata: {}/{snapshots_count}", snapshots_count - new_torrent_data.len());
                    }
                    tx.commit()?;

                    // Wait for all snapshots to be downloaded
                    loop {
                        let mut total_downloaded = 0;
                        let mut total_size = 0;
                        let mut total_complete = 0;
                        for (id, snapshot_info) in &all_snapshots {
                            let _ = id;
                            if let Some(snapshot_info) = &*snapshot_info.lock() {
                                let size = snapshot_info.size_when_done.unwrap();
                                let percent_done = snapshot_info.percent_done.unwrap();

                                total_downloaded += (size as f64 * percent_done as f64) as u64;
                                total_size += size;

                                if snapshot_info.status.unwrap() > 4 {
                                    total_complete += 1;
                                }
                            }
                        }

                        if total_complete == all_snapshots.len() {
                            info!("Snapshots downloaded, total size: {total_size}");
                            break;
                        }

                        info!("{} out of {} downloaded", ByteSize::b(total_downloaded), ByteSize::b(total_size as u64));

                        tokio::time::sleep(TORRENT_STATUS_REFRESH_INTERVAL).await;
                    }
                }

                let header_snapshotter = Arc::new(AsyncMutex::new(Snapshotter::new(
                    &snapshot_dir,
                    &db.begin()?,
                )?));
                let body_snapshotter = Arc::new(AsyncMutex::new(Snapshotter::new(
                    &snapshot_dir,
                    &db.begin()?,
                )?));
                let sender_snapshotter = Arc::new(AsyncMutex::new(Snapshotter::new(
                    &snapshot_dir,
                    &db.begin()?,
                )?));

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
                        bt_sender: stage_snapshot_tx.clone(),
                    },
                    false,
                );
                staged_sync.push(BodyDownload { node, consensus }, false);
                staged_sync.push(TotalTxIndex, false);
                staged_sync.push(
                    BodySnapshot {
                        snapshotter: body_snapshotter,
                        bt_sender: stage_snapshot_tx.clone(),
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
                        bt_sender: stage_snapshot_tx,
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
