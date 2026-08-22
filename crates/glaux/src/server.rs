//! Assembly of the all-in-one server: fakecloud's service registry with the
//! embedded control-plane services, glaux's Athena and Firehose registered
//! over fakecloud's stubs, and the axum router in front of fakecloud's
//! dispatcher.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::Extension;
use axum::routing::get;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::dispatch::{self, DispatchConfig};
use fakecloud_core::multi_account::{AccountState, MultiAccountState};
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_glue::{GlueAccounts, GlueService, SharedGlueState};
use fakecloud_iam::iam_service::IamService;
use fakecloud_iam::sts_service::StsService;
use fakecloud_kms::KmsService;
use fakecloud_logs::LogsService;
use fakecloud_s3::{S3Service, SharedS3State};
use fakecloud_secretsmanager::SecretsManagerService;
use fakecloud_sns::SnsService;
use fakecloud_sqs::SqsService;
use fakecloud_ssm::SsmService;
use glaux_athena::{AthenaService, AthenaServiceConfig};
use glaux_catalog::{GlauxConfig, GlueApi, StorageBackend};
use glaux_firehose::{FirehoseService, FirehoseServiceConfig, S3DeliverySink};
use parking_lot::RwLock;
use tokio::net::TcpListener;

use crate::bridge::{AthenaAwsService, FirehoseAwsService};
use crate::engine::LiveCatalogEngine;
use crate::glue::InProcessGlue;
use crate::storage::InProcessStorage;

/// The fakecloud release every `fakecloud-*` dependency is pinned to.
pub const FAKECLOUD_VERSION: &str = "0.44.10";

/// fakecloud services embedded in this binary, by registry name. Athena and
/// Firehose are glaux's own implementations, not fakecloud's stubs.
pub const EMBEDDED_FAKECLOUD_SERVICES: &[&str] = &[
    "s3",
    "glue",
    "sqs",
    "sns",
    "iam",
    "sts",
    "ssm",
    "secretsmanager",
    "kms",
    "logs",
];

/// How to serve: the bind address and the public endpoint URL fakecloud
/// embeds in generated URLs (SQS queue URLs, ...).
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// Socket address to bind.
    pub addr: SocketAddr,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([0, 0, 0, 0], 4566)),
        }
    }
}

impl ServeOptions {
    /// The URL clients reach this server at, for fakecloud's URL-generating
    /// services. A wildcard bind is reported as `localhost`.
    pub fn endpoint_url(&self) -> String {
        let host = if self.addr.ip().is_unspecified() {
            "localhost".to_string()
        } else {
            self.addr.ip().to_string()
        };
        format!("http://{host}:{}", self.addr.port())
    }
}

/// Everything the binary (or a test) needs from an assembled server.
pub struct Glaux {
    /// The axum application: fakecloud's dispatcher plus health routes.
    pub router: Router,
    /// The service registry, for introspection.
    pub registry: Arc<ServiceRegistry>,
    /// glaux's Athena service.
    pub athena: Arc<AthenaService>,
    /// glaux's Firehose service (flush it on shutdown).
    pub firehose: Arc<FirehoseService>,
    /// The in-process S3 backend the engines read from.
    pub storage: Arc<InProcessStorage>,
    /// The in-process Glue backend the engines read from.
    pub glue: Arc<InProcessGlue>,
    /// fakecloud's raw S3 state (tests peek at it; nothing else should).
    pub s3_state: SharedS3State,
    /// fakecloud's raw Glue state.
    pub glue_state: SharedGlueState,
}

impl Glaux {
    /// Assemble the server. Fails explicitly when `config` asks for external
    /// S3/Glue endpoints: the all-in-one binary always serves its embedded
    /// fakecloud in-process, and pretending to honor an endpoint it does not
    /// use would be silently wrong. Use `glaux-server` for that.
    pub fn build(config: &GlauxConfig, options: &ServeOptions) -> Result<Self, String> {
        if let Some(endpoint) = &config.s3_endpoint {
            return Err(format!(
                "s3_endpoint is set to {endpoint:?}, but the all-in-one glaux binary always uses \
                 its embedded fakecloud S3 in-process. Unset it, or run glaux-server to target an \
                 external S3 endpoint."
            ));
        }
        if let Some(endpoint) = &config.glue_endpoint {
            return Err(format!(
                "glue_endpoint is set to {endpoint:?}, but the all-in-one glaux binary always \
                 uses its embedded fakecloud Glue in-process. Unset it, or run glaux-server to \
                 target an external Glue endpoint."
            ));
        }

        let account = config.account_id.as_str();
        let region = config.region.as_str();
        let endpoint_url = options.endpoint_url();
        fn multi<T: AccountState>(
            account: &str,
            region: &str,
            endpoint_url: &str,
        ) -> Arc<RwLock<MultiAccountState<T>>> {
            Arc::new(RwLock::new(MultiAccountState::new(
                account,
                region,
                endpoint_url,
            )))
        }

        // --- fakecloud control plane -------------------------------------
        let iam_state = multi(account, region, &endpoint_url);
        let sqs_state = multi(account, region, &endpoint_url);
        let sns_state = Arc::new(RwLock::new({
            let mut accounts: MultiAccountState<fakecloud_sns::SnsState> =
                MultiAccountState::new(account, region, &endpoint_url);
            accounts.default_mut().seed_default_opted_out();
            accounts
        }));
        let ssm_state = multi(account, region, &endpoint_url);
        let secretsmanager_state = multi(account, region, &endpoint_url);
        let kms_state = multi(account, region, &endpoint_url);
        let logs_state = multi(account, region, &endpoint_url);
        let s3_state: SharedS3State = multi(account, region, &endpoint_url);
        let glue_state: SharedGlueState = Arc::new(RwLock::new(GlueAccounts::new()));

        // Cross-service fan-out, wired the way fakecloud's main does for the
        // subset embedded here: SNS → SQS, S3 notifications → SQS/SNS.
        let sqs_delivery = Arc::new(fakecloud_sqs::delivery::SqsDeliveryImpl::new(Arc::clone(
            &sqs_state,
        )));
        let delivery_for_sns = Arc::new(DeliveryBus::new().with_sqs(sqs_delivery.clone()));
        let sns_delivery = Arc::new(fakecloud_sns::delivery::SnsDeliveryImpl::new(
            Arc::clone(&sns_state),
            Arc::clone(&delivery_for_sns),
        ));
        let delivery_for_s3 = Arc::new(
            DeliveryBus::new()
                .with_sqs(sqs_delivery.clone())
                .with_sns(sns_delivery.clone()),
        );

        let s3_service = Arc::new(S3Service::new(Arc::clone(&s3_state), delivery_for_s3));

        let mut registry = ServiceRegistry::new();
        registry.register(Arc::clone(&s3_service) as Arc<dyn fakecloud_core::service::AwsService>);
        registry.register(Arc::new(GlueService::new(Arc::clone(&glue_state))));
        registry.register(Arc::new(
            SqsService::new(Arc::clone(&sqs_state)).with_region(region.to_string()),
        ));
        registry.register(Arc::new(
            SnsService::new(Arc::clone(&sns_state), delivery_for_sns)
                .with_region(region.to_string()),
        ));
        registry.register(Arc::new(IamService::new(Arc::clone(&iam_state))));
        registry.register(Arc::new(StsService::new(Arc::clone(&iam_state))));
        registry.register(Arc::new(
            SsmService::new(Arc::clone(&ssm_state))
                .with_secretsmanager(Arc::clone(&secretsmanager_state)),
        ));
        registry.register(Arc::new(SecretsManagerService::new(Arc::clone(
            &secretsmanager_state,
        ))));
        registry.register(Arc::new(build_kms(Arc::clone(&kms_state))));
        registry.register(Arc::new(LogsService::new(
            Arc::clone(&logs_state),
            Arc::new(DeliveryBus::new()),
        )));

        // --- glaux data plane ---------------------------------------------
        let storage = Arc::new(InProcessStorage::new(
            Arc::clone(&s3_state),
            s3_service,
            account,
            region,
        ));
        let glue = Arc::new(InProcessGlue::new(Arc::clone(&glue_state), account, region));
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let glue_dyn: Arc<dyn GlueApi> = glue.clone();

        let engine = Arc::new(LiveCatalogEngine::new(
            Arc::clone(&glue_dyn),
            Arc::clone(&storage_dyn),
        ));
        let athena = Arc::new(AthenaService::new(
            AthenaServiceConfig::from(&config.athena),
            engine,
            Arc::clone(&storage_dyn),
        ));
        let firehose = Arc::new(FirehoseService::new(
            FirehoseServiceConfig::from(config),
            Arc::new(S3DeliverySink::new(storage_dyn, glue_dyn)),
        ));

        // Re-registering a name replaces the service: these take the slots
        // fakecloud's own `athena` / `firehose` stubs would occupy.
        registry.register(Arc::new(AthenaAwsService::new(Arc::clone(&athena))));
        registry.register(Arc::new(FirehoseAwsService::new(Arc::clone(&firehose))));

        let registry = Arc::new(registry);
        let dispatch_config = Arc::new(DispatchConfig::new(region, account));
        let router = Router::new()
            .route("/_glaux/health", get(health))
            .route("/_fakecloud/health", get(health))
            .fallback(dispatch::dispatch)
            .layer(Extension(Arc::clone(&registry)))
            .layer(Extension(dispatch_config));

        Ok(Self {
            router,
            registry,
            athena,
            firehose,
            storage,
            glue,
            s3_state,
            glue_state,
        })
    }

    /// Registered service names, sorted.
    pub fn service_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .registry
            .service_names()
            .into_iter()
            .map(str::to_string)
            .collect();
        names.sort();
        names
    }

    /// Serve `router` on `listener` until `shutdown` resolves, then flush
    /// every Firehose buffer. Returns the streams whose final flush failed.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<Vec<(String, glaux_firehose::SinkError)>> {
        let firehose = Arc::clone(&self.firehose);
        axum::serve(
            listener,
            self.router
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown)
        .await?;
        Ok(firehose.shutdown().await)
    }
}

async fn health() -> &'static str {
    "ok"
}

/// Construct fakecloud's KMS service **outside** any tokio runtime context.
///
/// When `KmsService::new` detects a runtime it eagerly pre-generates RSA
/// 2048/3072/4096 keypairs on a blocking thread — tens of CPU-seconds in
/// a debug build, and a runtime shutdown (e.g. a test's) blocks until that
/// thread finishes. Built from a plain thread instead, KMS generates RSA
/// keys lazily on the first asymmetric `CreateKey`, exactly as fakecloud
/// itself behaves when constructed without a runtime.
fn build_kms(state: fakecloud_kms::SharedKmsState) -> KmsService {
    std::thread::Builder::new()
        .name("glaux-kms-init".to_string())
        .spawn(move || KmsService::new(state))
        .expect("spawn kms init thread")
        .join()
        .expect("kms init thread panicked")
}
