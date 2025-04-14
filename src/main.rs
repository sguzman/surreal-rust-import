// src/main.rs - Rust SurrealDB Parallel Importer with Tracing

use std::fs::File;
use std::io::{self, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Use tracing macros
// Removed unused Level import
use tracing::{debug, error, info, warn, Instrument, info_span};
use tracing_subscriber::{fmt, EnvFilter}; // Import tracing_subscriber components

use serde_json::Value;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// --- Configuration ---
const FILE_PATH: &str = "arxiv_array.json";
const DATABASE_URL: &str = "ws://localhost:8000";
const NAMESPACE: &str = "kaggle_data";
const DATABASE: &str = "arxiv";
const TABLE_NAME: &str = "arxiv_data";
const NUM_WORKERS: usize = 8;
const CHANNEL_BUFFER_SIZE: usize = 1000;

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
    #[error("Worker Task Failed: {0}")]
    WorkerTaskFailed(String),
    #[error("Tokio Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

// --- Main Application Logic ---
#[tokio::main]
async fn main() -> Result<(), ImportError> {
    // Initialize tracing subscriber
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(true)
        .init();

    let app_span = info_span!("main_app");
    async move { // Move logic into the span's async block

        info!("Starting SurrealDB parallel importer...");
        info!("Configuration:");
        info!("  File Path: {}", FILE_PATH);
        info!("  Database URL: {}", DATABASE_URL);
        info!("  Namespace: {}", NAMESPACE);
        info!("  Database: {}", DATABASE);
        info!("  Table Name: {}", TABLE_NAME);
        info!("  Worker Tasks Limit: {}", NUM_WORKERS);
        info!("  Channel Buffer: {}", CHANNEL_BUFFER_SIZE);

        let processed_count = Arc::new(AtomicUsize::new(0));
        let inserted_count = Arc::new(AtomicUsize::new(0));
        let failed_count = Arc::new(AtomicUsize::new(0));

        let (tx, mut rx) = mpsc::channel::<Value>(CHANNEL_BUFFER_SIZE);

        // --- Spawn Parser Task ---
        let parser_processed_count = processed_count.clone();
        let parser_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(
            async move {
                info!("Parser task started. Reading from: {}", FILE_PATH);
                let file = File::open(FILE_PATH)?;
                let reader = BufReader::new(file);
                let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();

                for result in stream {
                    match result {
                        Ok(record) => {
                            if record.is_object() {
                                debug!("Sending record to workers.");
                                if tx.send(record).await.is_err() {
                                    warn!("Channel closed by receiver. Stopping parser task.");
                                    break;
                                }
                                parser_processed_count.fetch_add(1, Ordering::Relaxed);
                            } else {
                                // Refactored match statement for clarity and to avoid macro issues
                                let type_str = match record {
                                    serde_json::Value::Null => "null",
                                    serde_json::Value::Bool(_) => "boolean",
                                    serde_json::Value::Number(_) => "number",
                                    serde_json::Value::String(_) => "string",
                                    serde_json::Value::Array(_) => "array",
                                    serde_json::Value::Object(_) => "object", // Should not happen here
                                };
                                warn!(
                                    "Skipping item - not a JSON object. Type: {}",
                                    type_str
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
            .instrument(info_span!("parser_task")),
        );

        // --- Spawn Worker Spawner Task ---
        let semaphore = Arc::new(tokio::sync::Semaphore::new(NUM_WORKERS));
        let worker_inserted_count = inserted_count.clone();
        let worker_failed_count = failed_count.clone();

        let worker_spawner_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(
            async move {
                info!("Worker spawner task starting...");
                let db = Surreal::new::<Ws>(DATABASE_URL).await?;
                info!("DB connection established.");

                // Optional Auth would go here

                db.use_ns(NAMESPACE).use_db(DATABASE).await?;
                info!("Namespace/DB selected.");

                let mut insertion_tasks = Vec::new();

                while let Some(record) = rx.recv().await {
                    let db_clone = db.clone();
                    let worker_inserted_count_clone = worker_inserted_count.clone();
                    let worker_failed_count_clone = worker_failed_count.clone();
                    let semaphore_clone = semaphore.clone();
                    // Use processed_count from the outer scope for the record ID span
                    // Note: This count might not perfectly match the record number if parsing skips items,
                    // but it provides a sequential identifier for tracing.
                    let current_processed_count = processed_count.load(Ordering::Relaxed);
                    let record_id_str = record.get("id").and_then(|v| v.as_str()).map(str::to_string)
                        .unwrap_or_else(|| format!("record_{}", current_processed_count));


                    let permit = match semaphore_clone.acquire_owned().await {
                         Ok(p) => p,
                         Err(_) => { error!("Semaphore closed unexpectedly."); break; }
                    };

                    let insertion_future = async move {
                        debug!("Processing record...");
                        let result: Result<Vec<Value>, surrealdb::Error> =
                            db_clone.create(TABLE_NAME).content(record.clone()).await;

                        match result {
                            Ok(created_result) => {
                                 if !created_result.is_empty() {
                                    worker_inserted_count_clone.fetch_add(1, Ordering::Relaxed);
                                    debug!("Record inserted successfully.");
                                 } else {
                                    warn!("db.create returned Ok([]). Assuming success/duplicate ignored. Snippet: {:.200}", record.to_string());
                                    worker_inserted_count_clone.fetch_add(1, Ordering::Relaxed);
                                 }
                            }
                            Err(e) => {
                                error!("Database error: {}", e);
                                worker_failed_count_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        drop(permit);
                    };

                    let instrumented_insertion = insertion_future
                        .instrument(info_span!("insertion_task", record_id = %record_id_str));

                    let task_handle = tokio::spawn(instrumented_insertion);
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
            .instrument(info_span!("worker_spawner")),
        );

        // --- Wait for Tasks and Report ---
        info!("Waiting for parser task to complete...");
        parser_handle.await??;
        info!("Parser task completed successfully.");

        info!("Waiting for worker spawner task to complete...");
        match worker_spawner_handle.await? {
            Ok(_) => info!("Worker spawner task completed successfully."),
            Err(e) => error!("Worker spawner task failed: {}", e),
        }

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
                "Import completed, but processed count ({}) != inserted count ({}). Check logs.",
                final_processed, final_inserted
             );
        } else {
             info!("Import completed successfully.");
        }

        Ok(()) // Return Ok from the main_app span's async block

    }.instrument(app_span).await // Instrument and await the main logic block

} // End of main function
