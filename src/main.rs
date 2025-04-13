
// src/main.rs - Rust SurrealDB Parallel Importer

use std::fs::File;
use std::io::{self, BufReader};
// use std::path::PathBuf; // Removed unused import
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
// use std::time::Duration; // Removed unused import

use log::{debug, error, info, warn};
use serde_json::Value; // Using dynamic Value for schemaless approach
// Correct import for Ws client type
use surrealdb::engine::remote::ws::Ws;
// use surrealdb::opt::auth::Root; // Removed unused import
use surrealdb::Surreal;
use thiserror::Error;
use tokio::sync::mpsc; // Async channel for communication
use tokio::task::JoinHandle;
// use tokio::time::sleep; // Removed unused import

// --- Configuration ---
// TODO: Replace hardcoded values with command-line arguments (e.g., using clap)
const FILE_PATH: &str = "arxiv_array.json";
const DATABASE_URL: &str = "ws://localhost:8000";
const NAMESPACE: &str = "kaggle_data";
const DATABASE: &str = "arxiv";
const TABLE_NAME: &str = "arxiv_data";
const NUM_WORKERS: usize = 8; // Number of parallel insertion workers
const CHANNEL_BUFFER_SIZE: usize = 1000; // Max items buffered between parser and workers

// Optional: Credentials if authentication is needed
// const DB_USER: &str = "root";
// const DB_PASS: &str = "admin";

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
    WorkerTaskFailed(String), // Keep this for potential future use
    #[error("Tokio Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError), // Added for task join errors
}

// --- Main Application Logic ---
#[tokio::main]
async fn main() -> Result<(), ImportError> {
    // Initialize logging (e.g., `RUST_LOG=info cargo run`)
    env_logger::init();

    info!("Starting SurrealDB parallel importer...");
    info!("Configuration:");
    info!("  File Path: {}", FILE_PATH);
    info!("  Database URL: {}", DATABASE_URL);
    info!("  Namespace: {}", NAMESPACE);
    info!("  Database: {}", DATABASE);
    info!("  Table Name: {}", TABLE_NAME);
    info!("  Worker Tasks Limit: {}", NUM_WORKERS); // Renamed for clarity with semaphore model
    info!("  Channel Buffer: {}", CHANNEL_BUFFER_SIZE);

    // Shared atomic counters for statistics
    let processed_count = Arc::new(AtomicUsize::new(0));
    let inserted_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));

    // Create a bounded channel to send parsed records to workers
    // tx (transmitter) sends, rx (receiver) receives
    let (tx, mut rx) = mpsc::channel::<Value>(CHANNEL_BUFFER_SIZE);

    // --- Spawn Parser Task ---
    let parser_processed_count = processed_count.clone();
    let parser_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(async move {
        info!("Parser task started. Reading from: {}", FILE_PATH);
        let file = File::open(FILE_PATH)?;
        // Use BufReader for potentially better performance
        let reader = BufReader::new(file);
        // Create a stream deserializer for the JSON array
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();

        for result in stream {
            match result {
                Ok(record) => {
                    // Validate if it's an object before sending
                    if record.is_object() {
                        debug!("Parser: Sending record to workers.");
                        // Send the parsed record to the workers
                        // This will block if the channel buffer is full (backpressure)
                        if tx.send(record).await.is_err() {
                            // If send fails, the receiver has been dropped, so we can stop parsing.
                            warn!("Parser: Channel closed by receiver. Stopping parser task.");
                            break;
                        }
                        parser_processed_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        // Log if the item in the top-level array is not an object
                        warn!(
                            "Parser: Skipping item - not a JSON object. Type: {}",
                            match record {
                                Value::Null => "null",
                                Value::Bool(_) => "boolean",
                                Value::Number(_) => "number",
                                Value::String(_) => "string",
                                Value::Array(_) => "array",
                                Value::Object(_) => "object", // Should not happen due to is_object check
                            }
                        );
                    }
                }
                Err(e) => {
                    // Log parsing error but try to continue if possible (depends on error type)
                    error!("Parser: JSON parsing error: {}. Attempting to continue.", e);
                    // Depending on the error, you might want to return Err here to stop everything.
                    // For example, if it's an IoError partway through.
                    // if e.is_io() { return Err(ImportError::Json(e)); }
                }
            }
        }
        info!("Parser task finished reading file.");
        // tx is automatically dropped here when the task scope ends, closing the channel.
        Ok(())
    });

    // --- Spawn Worker Spawner Task ---
    // This single task receives records and spawns insertion tasks, limited by a semaphore.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(NUM_WORKERS));
    let worker_inserted_count = inserted_count.clone();
    let worker_failed_count = failed_count.clone();

    let worker_spawner_handle: JoinHandle<Result<(), ImportError>> = tokio::spawn(async move {
        info!("Worker spawner task starting...");
        // Establish ONE database connection for this task.
        // Cloning the `db` handle is cheap and allows spawned tasks to use it.
        // Consider a connection pool (e.g., bb8-surrealdb, deadpool) for production.
        let db = Surreal::new::<Ws>(DATABASE_URL).await?;
        info!("Worker spawner task: DB connection established.");

        // --- Optional: Authentication ---
        // info!("Worker spawner task: Signing in...");
        // db.signin(Root { // Or use specific scope authentication
        //     username: DB_USER,
        //     password: DB_PASS,
        // }).await?;
        // info!("Worker spawner task: Signed in.");
        // --- End Optional: Authentication ---

        db.use_ns(NAMESPACE).use_db(DATABASE).await?;
        info!("Worker spawner task: Namespace/DB selected.");

        let mut insertion_tasks = Vec::new(); // Store handles of spawned insertion tasks

        // Loop until the channel is closed and empty
        while let Some(record) = rx.recv().await {
            let db_clone = db.clone(); // Clone handle for the spawned task
            let worker_inserted_count_clone = worker_inserted_count.clone();
            let worker_failed_count_clone = worker_failed_count.clone();
            let semaphore_clone = semaphore.clone();

            // Acquire a permit. This limits concurrency to NUM_WORKERS.
            // `acquire_owned` returns a permit that automatically releases when dropped.
            let permit = match semaphore_clone.acquire_owned().await {
                 Ok(p) => p,
                 Err(_) => {
                     error!("Worker spawner: Semaphore closed unexpectedly.");
                     break; // Stop processing if semaphore is closed
                 }
            };

            // Spawn a new task for the actual database insertion
            let task_handle = tokio::spawn(async move {
                debug!("Insertion task: Processing record...");
                // Corrected type annotation for the expected result: Vec<Value>
                let result: Result<Vec<Value>, surrealdb::Error> =
                    db_clone.create(TABLE_NAME).content(record.clone()).await;

                match result {
                    Ok(created_result) => {
                         // created_result is now Vec<Value>
                         // Check if the returned vector is non-empty as indication of success
                         if !created_result.is_empty() {
                            worker_inserted_count_clone.fetch_add(1, Ordering::Relaxed);
                            debug!("Insertion task: Record inserted successfully.");
                         } else {
                            // This might happen if CREATE returns empty array on success in some cases?
                            // Or maybe on duplicate ID with certain strategies.
                            warn!("Insertion task: db.create command returned Ok([]). Assuming success/duplicate ignored. Record snippet: {:.200}", record.to_string());
                            // Count as inserted if no error occurred.
                            worker_inserted_count_clone.fetch_add(1, Ordering::Relaxed);
                         }
                    }
                    Err(e) => {
                        error!("Insertion task: Database error: {}", e);
                        worker_failed_count_clone.fetch_add(1, Ordering::Relaxed);
                        // Optionally log the problematic record snippet
                        // error!("Problematic record snippet: {:.200}", record.to_string());
                    }
                }
                drop(permit); // Release the semaphore permit when the task is done
            });
            insertion_tasks.push(task_handle);
        }

        info!("Worker spawner task: Channel closed. Waiting for all insertion tasks...");
        // Wait for all spawned insertion tasks to complete
        let results = futures::future::join_all(insertion_tasks).await;
         // Check insertion task results for panics
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                error!("Insertion sub-task {} panicked or was cancelled: {}", i, e);
                // Optionally increment failed count here if panics should be counted
                // worker_failed_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        info!("Worker spawner task: All insertion tasks finished.");
        Ok(())

    }); // End of worker spawner task

    // --- Wait for Tasks and Report ---
    info!("Waiting for parser task to complete...");
    // Use ? to propagate errors from tasks
    parser_handle.await??; // First ? for JoinError, second for ImportError
    info!("Parser task completed successfully.");


    info!("Waiting for worker spawner task to complete...");
    // Use ? to propagate errors from tasks
    // Corrected formatting string: removed extra {}
    match worker_spawner_handle.await? { // First ? for JoinError
        Ok(_) => info!("Worker spawner task completed successfully."),
        Err(e) => error!("Worker spawner task failed: {}", e), // Removed extra {}
    }


    info!("--- Import Summary ---");
    let final_processed = processed_count.load(Ordering::SeqCst);
    let final_inserted = inserted_count.load(Ordering::SeqCst);
    let final_failed = failed_count.load(Ordering::SeqCst);
    info!("Records Processed (by parser): {}", final_processed);
    info!("Records Inserted Successfully: {}", final_inserted);
    info!("Records Failed to Insert: {}", final_failed);
    info!("----------------------");

    // Final status check
    if final_failed > 0 {
         error!("Import completed with {} failed insertions.", final_failed);
         // Consider returning an error code from main if needed
         // std::process::exit(1);
    } else if final_processed != final_inserted {
         warn!(
            "Import completed, but processed count ({}) does not match inserted count ({}). Check logs for skipped items or insertion logic.",
            final_processed, final_inserted
         );
    }
     else {
         info!("Import completed successfully.");
    }


    Ok(())
}
