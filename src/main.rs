// src/main.rs - Rust SurrealDB Parallel Importer (Refactored with Auth)

use std::fs::File;
use std::io::{self, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, warn, Instrument, info_span};
use tracing_subscriber::{fmt, EnvFilter};

use serde_json::Value;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root; // Import Root auth helper
use surrealdb::Surreal;
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

// --- Configuration ---
// TODO: Move to a Config struct and parse from args/env
const FILE_PATH: &str = "arxiv_array.json";
const DATABASE_URL: &str = "ws://localhost:8000";
const NAMESPACE: &str = "kaggle_data";
const DATABASE: &str = "arxiv";
const TABLE_NAME: &str = "arxiv_data";
const NUM_WORKERS: usize = 8; // Concurrency limit for insertions
const CHANNEL_BUFFER_SIZE: usize = 1000;
const CONNECT_TIMEOUT_SECONDS: u64 = 15;
const DB_RETRY_COUNT: usize = 3;
const DB_RETRY_BASE_MS: u64 = 100;
// --- Authentication Credentials ---
const DB_USER: &str = "root";
const DB_PASS: &str = "root";


// --- Error Handling ---
#[derive(Error, Debug)]
enum ImportError {
    #[error("I/O Error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON Parsing Error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Database Error: {0}")]
    Database(#[from] surrealdb::Error),
    #[error("Channel Send Error: {0}")]
    ChannelSend(#[from] mpsc::error::SendError<Value>),
    #[error("Connection Timeout after {0} seconds")]
    ConnectionTimeout(u64),
    #[error("Tokio Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Semaphore Acquisition Error")]
    SemaphoreAcquireError,
}

// --- Logging Setup ---
fn initialize_logging() {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(true)
        .init();
}

// --- Parser Task Logic ---
async fn run_parser_task(
    file_path: &'static str,
    tx: mpsc::Sender<Value>, // Move sender into the task
    processed_count: Arc<AtomicUsize>,
) -> Result<(), ImportError> {
     // ... (parser logic remains the same) ...
     info!("Parser task started. Reading from: {}", file_path);
     let file = File::open(file_path)?;
     let reader = BufReader::new(file);
     let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();

     for result in stream {
         match result {
             Ok(record) => {
                 let record_type_str = match record {
                     serde_json::Value::Null => "null",
                     serde_json::Value::Bool(_) => "boolean",
                     serde_json::Value::Number(_) => "number",
                     serde_json::Value::String(_) => "string",
                     serde_json::Value::Array(_) => "array",
                     serde_json::Value::Object(_) => "object",
                 };
                 if !record.is_object() {
                     debug!(value_type = %record_type_str, value_snippet = %format!("{:.100}", record), "Parser received top-level value (not an object)");
                 } else {
                      debug!(value_type = %record_type_str, "Parser received top-level value");
                 }

                 if record.is_object() {
                     debug!("Sending record object to workers.");
                     if tx.send(record).await.is_err() {
                         warn!("Channel closed by receiver. Stopping parser task.");
                         break; // Exit loop if receiver is gone
                     }
                     processed_count.fetch_add(1, Ordering::Relaxed);
                 } else {
                     warn!(
                         "Skipping top-level item - not a JSON object. Type: {}",
                         record_type_str
                     );
                 }
             }
             Err(e) => {
                 error!("JSON parsing error: {}. Attempting to continue.", e);
             }
         }
     }
     info!("Parser task finished reading file.");
     Ok(())
}

// --- Single Insertion Logic (with Retries) ---
async fn perform_insertion(
    db: Surreal<Client>, // Use the specific client type
    table_name: &'static str,
    record: Value,
    inserted_count: Arc<AtomicUsize>,
    failed_count: Arc<AtomicUsize>,
    permit: OwnedSemaphorePermit, // Takes ownership of the permit
) {
    // ... (insertion logic with retry remains the same) ...
    let retry_strategy = ExponentialBackoff::from_millis(DB_RETRY_BASE_MS)
        .map(jitter)
        .take(DB_RETRY_COUNT);

    debug!("Processing record...");

    let result = Retry::spawn(retry_strategy, || async {
        let db_attempt = db.clone();
        let record_attempt = record.clone();
        let attempt_result: Result<Vec<Value>, surrealdb::Error> =
            db_attempt.create(table_name).content(record_attempt).await;

        match attempt_result {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!("DB create failed, retrying... Error: {}", e);
                Err(e)
            }
        }
    }).await;

    match result {
        Ok(created_result) => {
             if !created_result.is_empty() {
                inserted_count.fetch_add(1, Ordering::Relaxed);
                debug!("Record inserted successfully.");
             } else {
                warn!("db.create returned Ok([]) (after retries if any). Assuming success/duplicate ignored. Snippet: {:.200}", record.to_string());
                inserted_count.fetch_add(1, Ordering::Relaxed);
             }
        }
        Err(e) => {
            error!("Database error after retries: {}", e);
            failed_count.fetch_add(1, Ordering::Relaxed);
        }
    }
    drop(permit);
}


// --- Worker Spawner Task Logic ---
async fn run_worker_spawner_task(
    mut rx: mpsc::Receiver<Value>, // Take ownership of the receiver
    db_url: &'static str,
    namespace: &'static str,
    database: &'static str,
    table_name: &'static str,
    semaphore: Arc<Semaphore>,
    processed_count: Arc<AtomicUsize>, // Needed for record ID generation
    inserted_count: Arc<AtomicUsize>,
    failed_count: Arc<AtomicUsize>,
) -> Result<(), ImportError> {
    info!("Worker spawner task starting...");

    // --- Establish Connection with Timeout ---
    let db: Surreal<Client>;
    info!("Attempting DB connection ({}s timeout)...", CONNECT_TIMEOUT_SECONDS);
    let connect_future = Surreal::new::<Ws>(db_url);

    match timeout(Duration::from_secs(CONNECT_TIMEOUT_SECONDS), connect_future).await {
        Ok(Ok(db_conn)) => {
            db = db_conn;
            info!("DB connection established.");
        }
        Ok(Err(db_err)) => {
            error!("DB connection attempt failed: {}", db_err);
            return Err(ImportError::Database(db_err));
        }
        Err(_) => {
            error!("DB connection attempt timed out after {} seconds.", CONNECT_TIMEOUT_SECONDS);
            return Err(ImportError::ConnectionTimeout(CONNECT_TIMEOUT_SECONDS));
        }
    }
    // --- Connection Established ---

    // --- Authentication ---
    info!("Attempting sign in as user '{}'...", DB_USER);
    // Use Root authentication helper
    db.signin(Root {
        username: DB_USER,
        password: DB_PASS,
    }).await?; // Propagate error if signin fails
    info!("Successfully signed in.");
    // --- End Authentication ---

    // --- Use Namespace and Database ---
    // This will implicitly create them if they don't exist and root has permissions
    info!("Selecting namespace '{}' and database '{}'...", namespace, database);
    db.use_ns(namespace).use_db(database).await?;
    info!("Namespace/DB selected (created if didn't exist).");
    // --- End Use Namespace/DB ---


    let mut insertion_tasks = Vec::new();

    while let Some(record) = rx.recv().await {
        let db_clone = db.clone();
        let task_inserted_count = inserted_count.clone();
        let task_failed_count = failed_count.clone();
        let semaphore_clone = semaphore.clone();

        let current_processed_count = processed_count.load(Ordering::Relaxed);
        let record_id_str = record.get("id").and_then(|v| v.as_str()).map(str::to_string)
            .unwrap_or_else(|| format!("record_{}", current_processed_count));

        let permit = semaphore_clone.acquire_owned().await.map_err(|_| {
            error!("Semaphore closed unexpectedly during acquire.");
            ImportError::SemaphoreAcquireError
        })?;

        let task_handle = tokio::spawn(
            perform_insertion(
                db_clone,
                table_name,
                record,
                task_inserted_count,
                task_failed_count,
                permit,
            )
            .instrument(info_span!("insertion_task", record_id = %record_id_str)),
        );
        insertion_tasks.push(task_handle);
    }

    info!("Channel closed. Waiting for all insertion tasks...");
    let results = futures::future::join_all(insertion_tasks).await;
    for (i, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            error!("Insertion sub-task {} panicked or was cancelled: {}", i, e);
        }
    }
    info!("All insertion tasks finished.");
    Ok(())
}


// --- Main Orchestration ---
#[tokio::main]
async fn main() -> Result<(), ImportError> {
    initialize_logging();

    let app_span = info_span!("main_app");
    let _enter = app_span.enter();

    info!("Starting SurrealDB parallel importer...");
    // ... (logging configuration remains the same) ...

    let processed_count = Arc::new(AtomicUsize::new(0));
    let inserted_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(NUM_WORKERS));

    let (tx, rx) = mpsc::channel::<Value>(CHANNEL_BUFFER_SIZE);

    // Spawn tasks using the refactored functions
    let parser_handle = tokio::spawn(
        run_parser_task(FILE_PATH, tx, processed_count.clone())
            .instrument(info_span!("parser_task_wrapper"))
    );

    let worker_spawner_handle = tokio::spawn(
        run_worker_spawner_task(
            rx,
            DATABASE_URL,
            NAMESPACE,
            DATABASE,
            TABLE_NAME,
            semaphore.clone(),
            processed_count.clone(),
            inserted_count.clone(),
            failed_count.clone(),
        )
        .instrument(info_span!("worker_spawner_wrapper"))
    );

    // --- Wait for Tasks and Report ---
    info!("Waiting for parser task to complete...");
    match parser_handle.await {
        Ok(Ok(_)) => info!("Parser task completed successfully."),
        Ok(Err(e)) => error!("Parser task failed: {}", e),
        Err(e) => error!("Parser task panicked or was cancelled: {}", e),
    }

    info!("Waiting for worker spawner task to complete...");
    match worker_spawner_handle.await {
         Ok(Ok(_)) => info!("Worker spawner task completed successfully."),
         Ok(Err(e)) => error!("Worker spawner task failed: {}", e),
         Err(e) => error!("Worker spawner task panicked or was cancelled: {}", e),
    }

    // --- Final Summary ---
    // ... (summary logic remains the same) ...
    info!("--- Import Summary ---");
    let final_processed = processed_count.load(Ordering::SeqCst);
    let final_inserted = inserted_count.load(Ordering::SeqCst);
    let final_failed = failed_count.load(Ordering::SeqCst);
    info!("Records Processed (by parser): {}", final_processed);
    info!("Records Inserted Successfully: {}", final_inserted);
    info!("Records Failed to Insert: {}", final_failed);
    info!("----------------------");

    if final_failed > 0 {
         error!("Import completed with {} failed insertions.", final_failed);
    } else if final_processed != final_inserted {
         warn!(
            "Import finished. Processed count ({}) != Inserted count ({}). Failed: {}. Check logs.",
            final_processed, final_inserted, final_failed
         );
    } else {
         info!("Import completed successfully.");
    }

    Ok(())
}
