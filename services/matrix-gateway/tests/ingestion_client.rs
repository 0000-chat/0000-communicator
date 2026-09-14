use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use communicator_matrix_gateway::{
    config::OAuthClientAuthMethod,
    ingestion::{
        BatchSink, Delivery, DeliveryErrorClass, IngestionClient, OAuthTokenProvider, PendingBatch,
        RetryPolicy, SecretString, TokenProvider,
    },
    secret::SafeError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers};

const TENANT_ID: &str = "tenant_demo";
const BATCH_ID: &str = "batch_demo";
const REQUEST_BODY: &[u8] = b"{\"body\":\"exact-outbox-bytes\",\"order\":[3,1,2]}";
const TOKEN_CANARY: &str = "token-canary-never-print";
const SECRET_CANARY: &str = "client-secret-canary-never-print";
const BODY_CANARY: &str = "request-body-canary-never-print";
const UPSTREAM_CANARY: &str = "upstream-error-body-canary-never-print";

#[derive(Debug, Default)]
struct FakeTokenProvider {
    tokens: Mutex<Vec<SecretString>>,
    calls: Mutex<Vec<bool>>,
}

impl FakeTokenProvider {
    fn new(tokens: &[&str]) -> Self {
        Self {
            tokens: Mutex::new(
                tokens
                    .iter()
                    .map(|token| SecretString::new(*token))
                    .collect(),
            ),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<bool> {
        self.calls.lock().expect("fake calls lock").clone()
    }
}

#[async_trait]
impl TokenProvider for FakeTokenProvider {
    async fn bearer(&self, force_refresh: bool) -> Result<SecretString, SafeError> {
        self.calls
            .lock()
            .expect("fake calls lock")
            .push(force_refresh);
        let mut tokens = self.tokens.lock().expect("fake tokens lock");
        if tokens.is_empty() {
            return Err(SafeError::new("test_token_missing"));
        }
        if force_refresh && tokens.len() == 1 {
            return Ok(tokens[0].clone());
        }
        Ok(tokens.remove(0))
    }
}

fn pending_batch() -> PendingBatch {
    PendingBatch::new(TENANT_ID, BATCH_ID, REQUEST_BODY.to_vec())
}

fn fast_retry_policy(max_attempts: usize) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        jitter: Duration::ZERO,
        max_retry_after: Duration::from_millis(5),
    }
}

fn accepted_response(archive_status: &str) -> ResponseTemplate {
    ResponseTemplate::new(202).set_body_raw(
        format!(
            "{{\"schema_version\":1,\"tenant_id\":\"{TENANT_ID}\",\"batch_id\":\"{BATCH_ID}\",\"status\":\"accepted\",\"archive_status\":\"{archive_status}\"}}"
        ),
        "application/json",
    )
}

fn sequence(responses: Vec<ResponseTemplate>) -> impl Respond {
    let responses = Arc::new(responses);
    let next = Arc::new(AtomicUsize::new(0));
    move |_request: &Request| {
        let index = next.fetch_add(1, Ordering::SeqCst);
        responses[index.min(responses.len() - 1)].clone()
    }
}

fn header(request: &Request, name: &str) -> String {
    request
        .headers
        .get(name)
        .unwrap_or_else(|| panic!("missing request header {name}"))
        .to_str()
        .expect("header is valid UTF-8")
        .to_owned()
}

fn form_fields(request: &Request) -> HashMap<String, String> {
    url::form_urlencoded::parse(&request.body)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

fn test_client(
    server_uri: &str,
    provider: Arc<dyn TokenProvider>,
    timeout: Duration,
) -> IngestionClient {
    IngestionClient::new_for_test(server_uri, provider, timeout)
        .expect("loopback ingestion client")
        .with_retry_policy(fast_retry_policy(2))
}

#[tokio::test]
async fn oauth_basic_auth_uses_form_encoding_without_secret_in_body() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/token"))
        .respond_with(|request: &Request| {
            assert_eq!(header(request, "accept"), "application/json");
            assert_eq!(
                header(request, "content-type"),
                "application/x-www-form-urlencoded"
            );
            assert_eq!(
                header(request, "authorization"),
                format!("Basic {}", STANDARD.encode("client id:client/secret"))
            );
            assert_eq!(
                form_fields(request),
                HashMap::from([(
                    String::from("grant_type"),
                    String::from("client_credentials")
                )])
            );
            ResponseTemplate::new(200).set_body_raw(
                format!(
                    "{{\"access_token\":\"{TOKEN_CANARY}\",\"token_type\":\"Bearer\",\"expires_in\":300,\"scope\":\"ingestion\",\"provider_extension\":{{\"regional\":true}}}}"
                ),
                "application/json",
            )
        })
        .mount(&server)
        .await;

    let provider = OAuthTokenProvider::new_for_test(
        format!("{}/token", server.uri()),
        "client id",
        SecretString::new("client/secret"),
        OAuthClientAuthMethod::Basic,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("OAuth provider");
    let token = provider.bearer(false).await.expect("bearer token");
    assert_eq!(token.as_str(), TOKEN_CANARY);
}

#[tokio::test]
async fn oauth_body_auth_uses_percent_encoded_client_credentials() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/token"))
        .respond_with(|request: &Request| {
            assert!(request.headers.get("authorization").is_none());
            assert_eq!(header(request, "accept"), "application/json");
            assert_eq!(
                header(request, "content-type"),
                "application/x-www-form-urlencoded"
            );
            assert_eq!(
                form_fields(request),
                HashMap::from([
                    (
                        String::from("grant_type"),
                        String::from("client_credentials")
                    ),
                    (String::from("client_id"), String::from("client id/+")),
                    (String::from("client_secret"), String::from("secret & = +")),
                ])
            );
            ResponseTemplate::new(200).set_body_raw(
                "{\"access_token\":\"body-token\",\"token_type\":\"Bearer\",\"expires_in\":300}",
                "application/json",
            )
        })
        .mount(&server)
        .await;

    let provider = OAuthTokenProvider::new_for_test(
        format!("{}/token", server.uri()),
        "client id/+",
        SecretString::new("secret & = +"),
        OAuthClientAuthMethod::Post,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("OAuth provider");
    assert_eq!(
        provider.bearer(false).await.expect("bearer token").as_str(),
        "body-token"
    );
}

#[tokio::test]
async fn oauth_tokens_cache_in_memory_and_refresh_inside_configured_skew() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let responder_calls = Arc::clone(&calls);
    Mock::given(matchers::path("/token"))
        .respond_with(move |_request: &Request| {
            let number = responder_calls.fetch_add(1, Ordering::SeqCst);
            let expires_in = if number == 1 { 1 } else { 300 };
            ResponseTemplate::new(200).set_body_raw(
                format!(
                    "{{\"access_token\":\"cached-token-{number}\",\"token_type\":\"Bearer\",\"expires_in\":{expires_in}}}"
                ),
                "application/json",
            )
        })
        .mount(&server)
        .await;
    let provider = OAuthTokenProvider::new_for_test(
        format!("{}/token", server.uri()),
        "client",
        SecretString::new(SECRET_CANARY),
        OAuthClientAuthMethod::Post,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("OAuth provider");

    let first = provider.bearer(false).await.expect("first token");
    let second = provider.bearer(false).await.expect("cached token");
    assert_eq!(first.as_str(), second.as_str());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!format!("{provider:?}").contains(SECRET_CANARY));
    assert!(!format!("{first:?}").contains(first.as_str()));

    let skewed = OAuthTokenProvider::new_for_test(
        format!("{}/token", server.uri()),
        "client",
        SecretString::new(SECRET_CANARY),
        OAuthClientAuthMethod::Post,
        Duration::from_secs(1),
        Duration::from_secs(2),
    )
    .expect("OAuth provider");
    let _ = skewed.bearer(false).await.expect("skewed first token");
    let _ = skewed.bearer(false).await.expect("skewed refresh");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn oauth_rejects_token_lifetime_above_worker_limit() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            "{\"access_token\":\"long-lived-token\",\"token_type\":\"Bearer\",\"expires_in\":301}",
            "application/json",
        ))
        .mount(&server)
        .await;

    let provider = OAuthTokenProvider::new_for_test(
        format!("{}/token", server.uri()),
        "client",
        SecretString::new(SECRET_CANARY),
        OAuthClientAuthMethod::Post,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect("OAuth provider");
    let error = provider
        .bearer(false)
        .await
        .expect_err("OAuth lifetime above worker limit");
    assert_eq!(error.code(), "oauth_token_invalid");
}

#[tokio::test]
async fn oauth_requires_bearer_type_json_and_a_bounded_response() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/wrong-type"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            "{\"access_token\":\"opaque\",\"token_type\":\"MAC\",\"expires_in\":60}",
            "application/json",
        ))
        .mount(&server)
        .await;
    Mock::given(matchers::path("/too-large"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(vec![b'x'; 64 * 1024 + 1], "application/json"),
        )
        .mount(&server)
        .await;
    Mock::given(matchers::path("/wrong-content-type"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    for path in ["wrong-type", "too-large", "wrong-content-type"] {
        let provider = OAuthTokenProvider::new_for_test(
            format!("{}/{path}", server.uri()),
            "client",
            SecretString::new(SECRET_CANARY),
            OAuthClientAuthMethod::Post,
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .expect("OAuth provider");
        let error = provider
            .bearer(false)
            .await
            .expect_err("invalid OAuth response");
        assert!(!format!("{error:?}").contains(SECRET_CANARY));
        assert!(!error.to_string().contains(SECRET_CANARY));
    }
}

#[tokio::test]
async fn production_urls_are_https_only_and_test_urls_are_loopback_only() {
    let server = MockServer::start().await;
    let error = OAuthTokenProvider::new(
        format!("{}/token", server.uri()),
        "client",
        SecretString::new(SECRET_CANARY),
        OAuthClientAuthMethod::Post,
        Duration::from_secs(1),
        Duration::from_secs(30),
    )
    .expect_err("production OAuth must reject HTTP");
    assert!(!format!("{error:?}").contains(SECRET_CANARY));

    let provider = Arc::new(FakeTokenProvider::new(&["token"]));
    let error = IngestionClient::new(server.uri(), provider, Duration::from_secs(1))
        .expect_err("production ingestion must reject HTTP");
    assert_eq!(error.class(), DeliveryErrorClass::Terminal);

    let error = IngestionClient::new_for_test(
        "http://192.0.2.1:8080",
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    )
    .expect_err("test client must reject non-loopback HTTP");
    assert_eq!(error.class(), DeliveryErrorClass::Terminal);
}

#[tokio::test]
async fn ingestion_sets_worker_compatible_json_headers_and_sends_exact_bytes() {
    let server = MockServer::start().await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(accepted_response("created"))
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["bearer-token"])),
        Duration::from_secs(1),
    );

    assert_eq!(
        client.deliver(&pending_batch()).await.expect("accepted"),
        Delivery::Accepted
    );
    let requests = server.received_requests().await.expect("recorded requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].body, REQUEST_BODY);
    assert_eq!(header(&requests[0], "accept"), "application/json");
    assert_eq!(header(&requests[0], "authorization"), "Bearer bearer-token");
    assert_eq!(header(&requests[0], "content-type"), "application/json");
    assert_eq!(header(&requests[0], "content-encoding"), "identity");
    assert_eq!(header(&requests[0], "x-tenant-id"), TENANT_ID);
    assert_eq!(header(&requests[0], "x-batch-id"), BATCH_ID);
}

#[tokio::test]
async fn created_and_already_committed_are_accepted() {
    for archive_status in ["created", "already_committed"] {
        let server = MockServer::start().await;
        Mock::given(matchers::path("/internal/v1/ingestion/batches"))
            .respond_with(accepted_response(archive_status))
            .mount(&server)
            .await;
        let client = test_client(
            &server.uri(),
            Arc::new(FakeTokenProvider::new(&["token"])),
            Duration::from_secs(1),
        );
        assert_eq!(
            client.deliver(&pending_batch()).await.expect("accepted"),
            Delivery::Accepted
        );
    }
}

#[tokio::test]
async fn response_loss_retries_the_same_pending_outbox_bytes() {
    let (uri, server_task) = spawn_response_loss_server().await;
    let client = test_client(
        &uri,
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    assert_eq!(
        client
            .deliver(&pending_batch())
            .await
            .expect("accepted after response loss"),
        Delivery::Accepted
    );
    let bodies = server_task.await.expect("response-loss server task");
    assert_eq!(bodies, vec![REQUEST_BODY.to_vec(), REQUEST_BODY.to_vec()]);
}

#[tokio::test]
async fn truncated_202_response_retries_the_same_pending_outbox_bytes() {
    let (uri, server_task) = spawn_truncated_accepted_server().await;
    let client = test_client(
        &uri,
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    assert_eq!(
        client
            .deliver(&pending_batch())
            .await
            .expect("accepted after truncated response"),
        Delivery::Accepted
    );
    let bodies = server_task.await.expect("truncated-response server task");
    assert_eq!(bodies, vec![REQUEST_BODY.to_vec(), REQUEST_BODY.to_vec()]);
}

#[tokio::test]
async fn one_401_forces_one_refresh_and_repeated_401_pauses() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(401),
            accepted_response("created"),
        ]))
        .mount(&server)
        .await;
    let provider = Arc::new(FakeTokenProvider::new(&["old-token", "new-token"]));
    let client = test_client(&server.uri(), provider.clone(), Duration::from_secs(1));
    assert_eq!(
        client
            .deliver(&pending_batch())
            .await
            .expect("refresh then accept"),
        Delivery::Accepted
    );
    assert_eq!(provider.calls(), vec![false, true]);
    let requests = server.received_requests().await.expect("recorded requests");
    assert_eq!(header(&requests[0], "authorization"), "Bearer old-token");
    assert_eq!(header(&requests[1], "authorization"), "Bearer new-token");
    assert_eq!(requests[0].body, requests[1].body);

    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(401),
            ResponseTemplate::new(401),
        ]))
        .mount(&server)
        .await;
    let provider = Arc::new(FakeTokenProvider::new(&["old-token", "new-token"]));
    let client = test_client(&server.uri(), provider.clone(), Duration::from_secs(1));
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("repeated 401 pauses");
    assert_eq!(error.class(), DeliveryErrorClass::Paused);
    assert_eq!(provider.calls(), vec![false, true]);
}

#[tokio::test]
async fn mismatched_202_fields_and_terminal_statuses_stop_without_retry() {
    let mismatches = [
        "{\"schema_version\":1,\"tenant_id\":\"wrong-tenant\",\"batch_id\":\"batch_demo\",\"status\":\"accepted\",\"archive_status\":\"created\"}",
        "{\"schema_version\":1,\"tenant_id\":\"tenant_demo\",\"batch_id\":\"wrong-batch\",\"status\":\"accepted\",\"archive_status\":\"created\"}",
        "{\"schema_version\":2,\"tenant_id\":\"tenant_demo\",\"batch_id\":\"batch_demo\",\"status\":\"accepted\",\"archive_status\":\"created\"}",
        "{\"schema_version\":1,\"tenant_id\":\"tenant_demo\",\"batch_id\":\"batch_demo\",\"status\":\"rejected\",\"archive_status\":\"created\"}",
        "{\"schema_version\":1,\"tenant_id\":\"tenant_demo\",\"batch_id\":\"batch_demo\",\"status\":\"accepted\",\"archive_status\":\"unknown\"}",
        "{\"schema_version\":1,\"tenant_id\":\"tenant_demo\",\"batch_id\":\"batch_demo\",\"status\":\"accepted\",\"archive_status\":\"created\",\"extra\":true}",
    ];
    for body in mismatches {
        let server = MockServer::start().await;
        Mock::given(matchers::path("/internal/v1/ingestion/batches"))
            .respond_with(ResponseTemplate::new(202).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let client = test_client(
            &server.uri(),
            Arc::new(FakeTokenProvider::new(&["token"])),
            Duration::from_secs(1),
        );
        let error = client
            .deliver(&pending_batch())
            .await
            .expect_err("strict response rejection");
        assert_eq!(error.class(), DeliveryErrorClass::Terminal);
        assert_eq!(
            server
                .received_requests()
                .await
                .expect("recorded requests")
                .len(),
            1
        );
    }

    for status in [400, 404, 409, 413] {
        let server = MockServer::start().await;
        Mock::given(matchers::path("/internal/v1/ingestion/batches"))
            .respond_with(ResponseTemplate::new(status).set_body_string(UPSTREAM_CANARY))
            .mount(&server)
            .await;
        let client = test_client(
            &server.uri(),
            Arc::new(FakeTokenProvider::new(&["token"])),
            Duration::from_secs(1),
        );
        let error = client
            .deliver(&pending_batch())
            .await
            .expect_err("terminal status");
        assert_eq!(error.class(), DeliveryErrorClass::Terminal);
        assert!(!format!("{error:?}").contains(UPSTREAM_CANARY));
        assert_eq!(
            server
                .received_requests()
                .await
                .expect("recorded requests")
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn bounded_429_retry_after_and_5xx_are_retryable() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(429).insert_header("retry-after", "0"),
            accepted_response("created"),
        ]))
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    assert_eq!(
        client.deliver(&pending_batch()).await.expect("429 retry"),
        Delivery::Accepted
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("recorded requests")
            .len(),
        2
    );

    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(429).insert_header("retry-after", "999999"),
            ResponseTemplate::new(429).insert_header("retry-after", "999999"),
        ]))
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    let started = Instant::now();
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("bounded 429");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(error.class(), DeliveryErrorClass::Retryable);
    assert!(
        error
            .retry_after()
            .is_some_and(|delay| delay <= Duration::from_millis(5))
    );

    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(503).set_body_string(UPSTREAM_CANARY),
            accepted_response("created"),
        ]))
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    assert_eq!(
        client.deliver(&pending_batch()).await.expect("5xx retry"),
        Delivery::Accepted
    );
}

#[tokio::test]
async fn five_hundred_retry_after_is_honored_before_retrying() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(sequence(vec![
            ResponseTemplate::new(503).insert_header("retry-after", "0"),
            accepted_response("created"),
        ]))
        .mount(&server)
        .await;
    let client = IngestionClient::new_for_test(
        server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    )
    .expect("loopback ingestion client")
    .with_retry_policy(RetryPolicy {
        max_attempts: 2,
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(1),
        jitter: Duration::ZERO,
        max_retry_after: Duration::from_secs(1),
    });

    let result = tokio::time::timeout(Duration::from_millis(200), client.deliver(&pending_batch()))
        .await
        .expect("bounded Retry-After should avoid exponential delay")
        .expect("5xx retry");
    assert_eq!(result, Delivery::Accepted);
}

#[tokio::test]
async fn malformed_and_oversized_5xx_retry_after_values_stay_bounded_and_redacted() {
    for retry_after in ["not-a-duration", "999999"] {
        let server = MockServer::start().await;
        Mock::given(matchers::path("/internal/v1/ingestion/batches"))
            .respond_with(sequence(vec![
                ResponseTemplate::new(503)
                    .insert_header("retry-after", retry_after)
                    .set_body_string(UPSTREAM_CANARY),
                ResponseTemplate::new(503)
                    .insert_header("retry-after", retry_after)
                    .set_body_string(UPSTREAM_CANARY),
            ]))
            .mount(&server)
            .await;
        let client = test_client(
            &server.uri(),
            Arc::new(FakeTokenProvider::new(&["token"])),
            Duration::from_secs(1),
        );

        let error = client
            .deliver(&pending_batch())
            .await
            .expect_err("bounded 5xx retry");
        assert_eq!(error.class(), DeliveryErrorClass::Retryable);
        assert!(!format!("{error:?}").contains(UPSTREAM_CANARY));
        if retry_after == "not-a-duration" {
            assert_eq!(error.retry_after(), None);
        } else {
            assert!(
                error
                    .retry_after()
                    .is_some_and(|delay| delay <= Duration::from_millis(5))
            );
        }
    }
}

#[tokio::test]
async fn redirects_are_rejected_and_ingestion_responses_are_bounded_json() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/unexpected", server.uri()))
                .set_body_string(UPSTREAM_CANARY),
        )
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("redirect rejected");
    assert_eq!(error.class(), DeliveryErrorClass::Terminal);
    assert!(!format!("{error:?}").contains(UPSTREAM_CANARY));
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("recorded requests")
            .len(),
        1
    );

    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("content-type", "application/json")
                .set_body_bytes(vec![b'x'; 64 * 1024 + 1]),
        )
        .mount(&server)
        .await;
    let client = test_client(
        &server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_secs(1),
    );
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("response too large");
    assert_eq!(error.class(), DeliveryErrorClass::Terminal);
    assert!(!format!("{error:?}").contains(BODY_CANARY));
}

#[tokio::test]
async fn network_failures_and_timeouts_remain_retryable_without_upstream_text() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind closed port");
    let port = listener.local_addr().expect("closed port address").port();
    drop(listener);
    let client = IngestionClient::new_for_test(
        format!("http://127.0.0.1:{port}"),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_millis(40),
    )
    .expect("closed loopback client")
    .with_retry_policy(fast_retry_policy(2));
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("network failure");
    assert_eq!(error.class(), DeliveryErrorClass::Retryable);

    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_raw("{}", "application/json")
                .set_delay(Duration::from_millis(100)),
        )
        .mount(&server)
        .await;
    let client = IngestionClient::new_for_test(
        server.uri(),
        Arc::new(FakeTokenProvider::new(&["token"])),
        Duration::from_millis(20),
    )
    .expect("timeout client")
    .with_retry_policy(fast_retry_policy(1));
    let error = client
        .deliver(&pending_batch())
        .await
        .expect_err("request timeout");
    assert_eq!(error.class(), DeliveryErrorClass::Retryable);
}

#[tokio::test]
async fn diagnostics_redact_tokens_credentials_request_bodies_urls_and_upstream_bodies() {
    let server = MockServer::start().await;
    Mock::given(matchers::path("/internal/v1/ingestion/batches"))
        .respond_with(ResponseTemplate::new(400).set_body_string(UPSTREAM_CANARY))
        .mount(&server)
        .await;
    let provider = Arc::new(FakeTokenProvider::new(&[TOKEN_CANARY]));
    let client = test_client(&server.uri(), provider.clone(), Duration::from_secs(1));
    let batch = PendingBatch::new(TENANT_ID, BATCH_ID, BODY_CANARY.as_bytes().to_vec());
    let error = client
        .deliver(&batch)
        .await
        .expect_err("terminal canary response");
    for diagnostic in [
        format!("{client:?}"),
        format!("{provider:?}"),
        format!("{batch:?}"),
        format!("{error:?}"),
        error.to_string(),
    ] {
        assert!(!diagnostic.contains(TOKEN_CANARY));
        assert!(!diagnostic.contains(SECRET_CANARY));
        assert!(!diagnostic.contains(BODY_CANARY));
        assert!(!diagnostic.contains(UPSTREAM_CANARY));
    }
}

async fn spawn_response_loss_server() -> (String, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind response-loss server");
    let address = listener.local_addr().expect("response-loss address");
    let task = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for attempt in 0..2 {
            let (mut socket, _) = listener
                .accept()
                .await
                .expect("accept response-loss request");
            let body = read_request_body(&mut socket)
                .await
                .expect("read request body");
            bodies.push(body);
            if attempt == 1 {
                let response_body = format!(
                    "{{\"schema_version\":1,\"tenant_id\":\"{TENANT_ID}\",\"batch_id\":\"{BATCH_ID}\",\"status\":\"accepted\",\"archive_status\":\"created\"}}"
                );
                let response = format!(
                    "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write accepted response");
            }
        }
        bodies
    });
    (format!("http://{address}"), task)
}

async fn spawn_truncated_accepted_server() -> (String, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind truncated-response server");
    let address = listener
        .local_addr()
        .expect("truncated-response server address");
    let task = tokio::spawn(async move {
        let response_body = format!(
            "{{\"schema_version\":1,\"tenant_id\":\"{TENANT_ID}\",\"batch_id\":\"{BATCH_ID}\",\"status\":\"accepted\",\"archive_status\":\"created\"}}"
        );
        let truncated_body = &response_body[..response_body.len() - 1];
        let mut bodies = Vec::new();
        for attempt in 0..2 {
            let (mut socket, _) = listener
                .accept()
                .await
                .expect("accept truncated-response request");
            let body = read_request_body(&mut socket)
                .await
                .expect("read truncated-response request body");
            bodies.push(body);
            let body_to_send = if attempt == 0 {
                truncated_body
            } else {
                response_body.as_str()
            };
            let response = format!(
                "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                body_to_send
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write truncated-response response");
        }
        bodies
    });
    (format!("http://{address}"), task)
}

async fn read_request_body(socket: &mut tokio::net::TcpStream) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end;
    let content_length;
    loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            return Ok(Vec::new());
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = end + 4;
            let headers = String::from_utf8_lossy(&bytes[..end]);
            content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':')
                        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value)
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break;
        }
    }
    while bytes.len() < header_end + content_length {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes[header_end..header_end + content_length].to_vec())
}
