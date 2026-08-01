#![deny(missing_docs, unsafe_code)]
//! # sqlxmq
//!
//! A job queue built on `sqlx` and `PostgreSQL`.
//!
//! This library allows a CRUD application to run background jobs without complicating its
//! deployment. The only runtime dependency is `PostgreSQL`, so this is ideal for applications
//! already using a `PostgreSQL` database.
//!
//! Although using a SQL database as a job queue means compromising on latency of
//! delivered jobs, there are several show-stopping issues present in ordinary job
//! queues which are avoided altogether.
//!
//! With most other job queues, in-flight jobs are state that is not covered by normal
//! database backups. Even if jobs _are_ backed up, there is no way to restore both
//! a database and a job queue to a consistent point-in-time without manually
//! resolving conflicts.
//!
//! By storing jobs in the database, existing backup procedures will store a perfectly
//! consistent state of both in-flight jobs and persistent data. Additionally, jobs can
//! be spawned and completed as part of other transactions, making it easy to write correct
//! application code.
//!
//! Leveraging the power of `PostgreSQL`, this job queue offers several features not
//! present in other job queues.
//!
//! # Features
//!
//! - **Send/receive multiple jobs at once.**
//!
//!   This reduces the number of queries to the database.
//!
//! - **Send jobs to be executed at a future date and time.**
//!
//!   Avoids the need for a separate scheduling system.
//!
//! - **Reliable delivery of jobs.**
//!
//! - **Automatic retries with exponential backoff.**
//!
//!   Number of retries and initial backoff parameters are configurable.
//!
//! - **Transactional sending of jobs.**
//!
//!   Avoids sending spurious jobs if a transaction is rolled back.
//!
//! - **Transactional completion of jobs.**
//!
//!   If all side-effects of a job are updates to the database, this provides
//!   true exactly-once execution of jobs.
//!
//! - **Transactional check-pointing of jobs.**
//!
//!   Long-running jobs can check-point their state to avoid having to restart
//!   from the beginning if there is a failure: the next retry can continue
//!   from the last check-point.
//!
//! - **Opt-in strictly ordered job delivery.**
//!
//!   Jobs within the same channel will be processed strictly in-order
//!   if this option is enabled for the job.
//!
//! - **Fair job delivery.**
//!
//!   A channel with a lot of jobs ready to run will not starve a channel with fewer
//!   jobs.
//!
//! - **Opt-in two-phase commit.**
//!
//!   This is particularly useful on an ordered channel where a position can be "reserved"
//!   in the job order, but not committed until later.
//!
//! - **JSON and/or binary payloads.**
//!
//!   Jobs can use whichever is most convenient.
//!
//! - **Automatic keep-alive of jobs.**
//!
//!   Long-running jobs will automatically be "kept alive" to prevent them being
//!   retried whilst they're still ongoing.
//!
//! - **Concurrency limits.**
//!
//!   Specify the minimum and maximum number of concurrent jobs each runner should
//!   handle.
//!
//! - **Built-in job registry via an attribute macro.**
//!
//!   Jobs can be easily registered with a runner, and default configuration specified
//!   on a per-job basis.
//!
//! - **Implicit channels.**
//!
//!   Channels are implicitly created and destroyed when jobs are sent and processed,
//!   so no setup is required.
//!
//! - **Channel groups.**
//!
//!   Easily subscribe to multiple channels at once, thanks to the separation of
//!   channel name and channel arguments.
//!
//! - **NOTIFY-based polling.**
//!
//!   This saves resources when few jobs are being processed.
//!
//! # Getting started
//!
//! ## Database schema
//!
//! This crate expects certain database tables and stored procedures to exist.
//! You can copy the migration files from this crate into your own migrations
//! folder.
//!
//! All database items created by this crate are prefixed with `mq`, so as not
//! to conflict with your own schema.
//!
//! ## Defining jobs
//!
//! The first step is to define a function to be run on the job queue.
//!
//! ```rust
//! use std::error::Error;
//!
//! use sqlxmq::{job, CurrentJob};
//!
//! // Arguments to the `#[job]` attribute allow setting default job options.
//! #[job(channel_name = "foo")]
//! async fn example_job(
//!     // The first argument should always be the current job.
//!     mut current_job: CurrentJob,
//!     // Additional arguments are optional, but can be used to access context
//!     // provided via [`JobRegistry::set_context`].
//!     message: &'static str,
//! ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
//!     // Decode a JSON payload
//!     let who: Option<String> = current_job.json()?;
//!
//!     // Do some work
//!     println!("{}, {}!", message, who.as_deref().unwrap_or("world"));
//!
//!     // Mark the job as complete
//!     current_job.complete().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Listening for jobs
//!
//! Next we need to create a job runner: this is what listens for new jobs
//! and executes them.
//!
//! ```rust,no_run
//! use std::error::Error;
//!
//! use sqlxmq::JobRegistry;
//!
//! # use sqlxmq::{job, CurrentJob};
//! #
//! # #[job]
//! # async fn example_job(
//! #     current_job: CurrentJob,
//! # ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> { Ok(()) }
//! #
//! # async fn connect_to_db() -> sqlx::Result<sqlx::Pool<sqlx::Postgres>> {
//! #     unimplemented!()
//! # }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     // You'll need to provide a Postgres connection pool.
//!     let pool = connect_to_db().await?;
//!
//!     // Construct a job registry from our single job.
//!     let mut registry = JobRegistry::new(&[example_job]);
//!     // Here is where you can configure the registry
//!     // registry.set_error_handler(...)
//!
//!     // And add context
//!     registry.set_context("Hello");
//!
//!     let runner = registry
//!         // Create a job runner using the connection pool.
//!         .runner(&pool)
//!         // Here is where you can configure the job runner
//!         // Aim to keep 10-20 jobs running at a time.
//!         .set_concurrency(10, 20)
//!         // Start the job runner in the background.
//!         .run()
//!         .await?;
//!
//!     // The job runner will continue listening and running
//!     // jobs until `runner` is dropped.
//!     Ok(())
//! }
//! ```
//!
//! ## Spawning a job
//!
//! The final step is to actually run a job.
//!
//! ```rust
//! # use std::error::Error;
//! # use sqlxmq::{job, CurrentJob};
//! #
//! # #[job]
//! # async fn example_job(
//! #     current_job: CurrentJob,
//! # ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> { Ok(()) }
//! #
//! # async fn example(
//! #     pool: sqlx::Pool<sqlx::Postgres>
//! # ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
//! example_job.builder()
//!     // This is where we can override job configuration
//!     .set_channel_name("bar")
//!     .set_json("John")?
//!     .spawn(&pool)
//!     .await?;
//! #     Ok(())
//! # }
//! ```

#[doc(hidden)]
pub mod hidden;
mod registry;
mod runner;
mod spawn;
mod utils;

pub use registry::*;
pub use runner::*;
pub use spawn::*;
pub use sqlxmq_macros::job;
pub use utils::OwnedHandle;

/// Helper function to determine if a particular error condition is retryable.
///
/// For best results, database operations should be automatically retried if one
/// of these errors is returned.
pub fn should_retry(error: &sqlx::Error) -> bool {
    if let Some(db_error) = error.as_database_error() {
        // It's more readable as a match
        #[allow(clippy::match_like_matches_macro)]
        match (db_error.code().as_deref(), db_error.constraint()) {
            // Foreign key constraint violation on ordered channel
            (Some("23503"), Some("mq_msgs_after_message_id_fkey")) => true,
            // Unique constraint violation on ordered channel
            (Some("23505"), Some("mq_msgs_channel_name_channel_args_after_message_id_idx")) => true,
            // Serialization failure
            (Some("40001"), _) => true,
            // Deadlock detected
            (Some("40P01"), _) => true,
            // Other
            _ => false,
        }
    } else {
        false
    }
}

/// Unit tests for [`should_retry`]. These do not require a database
/// connection: the errors are constructed from a stub `DatabaseError`.
#[cfg(test)]
mod should_retry_tests {
    use super::*;

    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt::{self, Display};

    use sqlx::error::{DatabaseError, ErrorKind};

    #[derive(Debug)]
    struct StubDbError {
        code: Option<&'static str>,
        constraint: Option<&'static str>,
    }

    impl Display for StubDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("stub database error")
        }
    }

    impl StdError for StubDbError {}

    impl DatabaseError for StubDbError {
        fn message(&self) -> &str {
            "stub database error"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }
        fn constraint(&self) -> Option<&str> {
            self.constraint
        }
        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
    }

    fn db_error(code: Option<&'static str>, constraint: Option<&'static str>) -> sqlx::Error {
        sqlx::Error::Database(Box::new(StubDbError { code, constraint }))
    }

    #[test]
    fn retries_serialization_failure() {
        assert!(should_retry(&db_error(Some("40001"), None)));
    }

    #[test]
    fn retries_deadlock() {
        assert!(should_retry(&db_error(Some("40P01"), None)));
    }

    /// Two ordered messages racing to chain onto the same predecessor.
    #[test]
    fn retries_ordered_channel_unique_violation() {
        assert!(should_retry(&db_error(
            Some("23505"),
            Some("mq_msgs_channel_name_channel_args_after_message_id_idx"),
        )));
    }

    /// The predecessor of an ordered message was deleted concurrently.
    #[test]
    fn retries_ordered_channel_foreign_key_violation() {
        assert!(should_retry(&db_error(
            Some("23503"),
            Some("mq_msgs_after_message_id_fkey"),
        )));
    }

    /// The constraint violations are only retryable on the ordered-channel
    /// constraints: the same SQLSTATE raised by the caller's own schema means
    /// a genuine conflict, and retrying would just raise it again.
    #[test]
    fn does_not_retry_constraint_violations_on_other_constraints() {
        assert!(!should_retry(&db_error(
            Some("23505"),
            Some("users_email_key")
        )));
        assert!(!should_retry(&db_error(
            Some("23503"),
            Some("orders_user_id_fkey")
        )));
        assert!(!should_retry(&db_error(Some("23505"), None)));
    }

    #[test]
    fn does_not_retry_other_database_errors() {
        // Undefined table.
        assert!(!should_retry(&db_error(Some("42P01"), None)));
        // Syntax error.
        assert!(!should_retry(&db_error(Some("42601"), None)));
        assert!(!should_retry(&db_error(None, None)));
    }

    /// Non-database errors (connection loss, decoding failures, pool
    /// timeouts) are not retryable by this helper.
    #[test]
    fn does_not_retry_non_database_errors() {
        assert!(!should_retry(&sqlx::Error::PoolTimedOut));
        assert!(!should_retry(&sqlx::Error::RowNotFound));
        assert!(!should_retry(&sqlx::Error::Io(std::io::Error::other(
            "connection reset"
        ))));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as sqlxmq;

    use std::collections::HashSet;
    use std::error::Error;
    use std::sync::Once;
    use std::time::{Duration, Instant};

    use futures::channel::mpsc;
    use futures::StreamExt;
    use sqlx::{Pool, Postgres};
    use uuid::Uuid;

    /// Upper bound on every wait in these tests.
    ///
    /// This is a hang detector, not a timing assumption: a working
    /// implementation dispatches as soon as the work is ready, so the only
    /// way to reach this limit is for something to be genuinely stuck. It is
    /// deliberately far longer than any operation here should take, so that a
    /// slow or loaded machine cannot turn it into a flaky failure.
    const TIMEOUT: Duration = Duration::from_secs(30);

    /// Initial retry backoff for the tests that exercise the retry schedule.
    ///
    /// It can be short because those tests assert on the schedule recorded in
    /// the database rather than on how long the test waited.
    const BACKOFF: Duration = Duration::from_millis(100);

    fn init_logging() {
        static INIT_LOGGER: Once = Once::new();
        INIT_LOGGER.call_once(pretty_env_logger::init);
    }

    /// Jobs dispatched by a test runner, in the order the runner handed them
    /// out.
    ///
    /// Tests take jobs from here and drive them explicitly: completing a job
    /// finishes it, dropping one without completing it leaves it to be
    /// retried, exactly as a failed job would be.
    struct JobStream(mpsc::UnboundedReceiver<CurrentJob>);

    impl JobStream {
        /// Wait for the next job to be dispatched.
        async fn next(&mut self) -> CurrentJob {
            tokio::time::timeout(TIMEOUT, self.0.next())
                .await
                .expect("timed out waiting for a job to be dispatched")
                .expect("the job runner stopped before dispatching a job")
        }

        /// Wait for the next `n` jobs to be dispatched.
        async fn next_n(&mut self, n: usize) -> Vec<CurrentJob> {
            let mut jobs = Vec::with_capacity(n);
            for _ in 0..n {
                jobs.push(self.next().await);
            }
            jobs
        }
    }

    /// Start a job runner which forwards every dispatched job to the returned
    /// [`JobStream`] instead of running it.
    ///
    /// Handing jobs to the test rather than to a handler is what lets these
    /// tests wait for the events they care about instead of sleeping for long
    /// enough that the events have probably happened.
    async fn test_job_runner(pool: &Pool<Postgres>) -> (JobRunnerHandle, JobStream) {
        configured_job_runner(pool, |options| options).await
    }

    /// As [`test_job_runner`], with an opportunity to change the runner
    /// options first.
    async fn configured_job_runner(
        pool: &Pool<Postgres>,
        configure: impl FnOnce(&mut JobRunnerOptions) -> &mut JobRunnerOptions,
    ) -> (JobRunnerHandle, JobStream) {
        init_logging();

        let (tx, rx) = mpsc::unbounded();
        let mut options = JobRunnerOptions::new(pool, move |job| {
            // If the test has dropped the receiver the job is dropped too,
            // and will be retried like any other unfinished job.
            let _ = tx.unbounded_send(job);
        });
        configure(&mut options);
        let runner = options.run().await.unwrap();

        (runner, JobStream(rx))
    }

    /// Number of messages in the queue, excluding the nil sentinel row.
    async fn queue_len(pool: &Pool<Postgres>) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM mq_msgs WHERE id != uuid_nil()")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The message this one is chained behind: the nil UUID for the head of
    /// an ordered chain, and `None` for an unordered message.
    async fn after_message_id(pool: &Pool<Postgres>, id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("SELECT after_message_id FROM mq_msgs WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Attempts remaining, and whether a further attempt is scheduled.
    ///
    /// `mq_poll` clears `attempt_at` as it hands out the final attempt, so
    /// `(0, false)` means the message is exhausted and can never be polled
    /// again. Asserting on this beats waiting to see whether another attempt
    /// shows up.
    async fn attempt_state(pool: &Pool<Postgres>, id: Uuid) -> (i32, bool) {
        sqlx::query_as("SELECT attempts, attempt_at IS NOT NULL FROM mq_msgs WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The raw JSON payload currently stored for a message.
    async fn stored_payload(pool: &Pool<Postgres>, id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT payload_json::TEXT FROM mq_payloads WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The retry backoff currently recorded for a message. `mq_poll` doubles
    /// it on every attempt.
    async fn retry_backoff(pool: &Pool<Postgres>, id: Uuid) -> Duration {
        let micros: i64 = sqlx::query_scalar(
            "SELECT (EXTRACT(EPOCH FROM retry_backoff) * 1000000)::BIGINT FROM mq_msgs WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
        Duration::from_micros(micros as u64)
    }

    fn job_proto<'a, 'b>(builder: &'a mut JobBuilder<'b>) -> &'a mut JobBuilder<'b> {
        builder.set_channel_name("bar")
    }

    /// Context which lets the registry-based jobs report that they ran.
    type RanJobs = mpsc::UnboundedSender<&'static str>;

    #[job(channel_name = "foo", ordered, retries = 3, backoff_secs = 2.0)]
    async fn example_job1(
        mut current_job: CurrentJob,
        ran: RanJobs,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        current_job.complete().await?;
        ran.unbounded_send("example_job1")?;
        Ok(())
    }

    #[job(proto(job_proto))]
    async fn example_job2(
        mut current_job: CurrentJob,
        ran: RanJobs,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        current_job.complete().await?;
        ran.unbounded_send("example_job2")?;
        Ok(())
    }

    #[job]
    async fn example_job_with_ctx(
        mut current_job: CurrentJob,
        ctx1: i32,
        ctx2: &'static str,
        ran: RanJobs,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        assert_eq!(ctx1, 42);
        assert_eq!(ctx2, "Hello, world!");
        current_job.complete().await?;
        ran.unbounded_send("example_job_with_ctx")?;
        Ok(())
    }

    async fn named_job_runner(pool: &Pool<Postgres>, ran: RanJobs) -> JobRunnerHandle {
        init_logging();

        let mut registry = JobRegistry::new(&[example_job1, example_job2, example_job_with_ctx]);
        registry
            .set_context(42)
            .set_context("Hello, world!")
            .set_context(ran);
        registry.runner(pool).run().await.unwrap()
    }

    #[sqlx::test]
    async fn it_can_spawn_job(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let id = JobBuilder::new("foo").spawn(&pool).await.unwrap();

        let mut job = jobs.next().await;
        assert_eq!(job.id(), id);
        assert_eq!(job.name(), "foo");
        job.complete().await.unwrap();

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    #[sqlx::test]
    async fn it_can_clear_jobs(pool: Pool<Postgres>) {
        let mut kept = HashSet::new();
        for channel_name in ["foo", "bar", "baz"] {
            for _ in 0..2 {
                let id = JobBuilder::new("foo")
                    .set_channel_name(channel_name)
                    .spawn(&pool)
                    .await
                    .unwrap();
                if channel_name == "bar" {
                    kept.insert(id);
                }
            }
        }
        assert_eq!(queue_len(&pool).await, 6);

        sqlxmq::clear(&pool, &["foo", "baz"]).await.unwrap();

        // Clearing deletes the messages outright, so what survived can be
        // checked directly rather than inferred from what fails to run.
        assert_eq!(queue_len(&pool).await, 2);

        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let mut delivered = HashSet::new();
        for mut job in jobs.next_n(2).await {
            delivered.insert(job.id());
            job.complete().await.unwrap();
        }
        assert_eq!(delivered, kept);

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    #[sqlx::test]
    async fn it_can_spawn_batch_of_jobs(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let mut job_a = JobBuilder::new("a");
        job_a.set_json(&"first").unwrap();
        let mut job_b = JobBuilder::new("b");
        job_b.set_json(&"second").unwrap();
        let job_c = JobBuilder::new("c");

        let ids = spawn_batch(&pool, &[job_a, job_b, job_c]).await.unwrap();
        assert_eq!(ids.len(), 3);

        let mut received = Vec::new();
        for mut job in jobs.next_n(3).await {
            let payload: Option<String> = job.json().unwrap();
            received.push((job.name().to_owned(), payload));
            job.complete().await.unwrap();
        }
        received.sort();
        assert_eq!(
            received,
            vec![
                ("a".to_owned(), Some("first".to_owned())),
                ("b".to_owned(), Some("second".to_owned())),
                ("c".to_owned(), None),
            ]
        );

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    #[sqlx::test]
    async fn it_chains_ordered_jobs_spawned_in_batch(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let builders: Vec<_> = ["a", "b", "c"]
            .iter()
            .copied()
            .map(|name| {
                let mut builder = JobBuilder::new(name);
                builder.set_ordered(true);
                builder
            })
            .collect();
        let ids = spawn_batch(&pool, &builders).await.unwrap();

        // The chain must follow the order of the batch, not the order of the
        // randomly generated message ids.
        assert_eq!(after_message_id(&pool, ids[0]).await, Some(Uuid::nil()));
        assert_eq!(after_message_id(&pool, ids[1]).await, Some(ids[0]));
        assert_eq!(after_message_id(&pool, ids[2]).await, Some(ids[1]));

        // `mq_poll` only considers messages at the head of a chain, so the
        // assertions above are also what stops `b` and `c` running early.
        for (i, expected_name) in ["a", "b", "c"].iter().copied().enumerate() {
            let mut job = jobs.next().await;
            assert_eq!(job.id(), ids[i]);
            assert_eq!(job.name(), expected_name);
            job.complete().await.unwrap();
        }

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    #[sqlx::test]
    async fn it_runs_jobs_in_order(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let first = JobBuilder::new("foo")
            .set_ordered(true)
            .spawn(&pool)
            .await
            .unwrap();
        let second = JobBuilder::new("bar")
            .set_ordered(true)
            .spawn(&pool)
            .await
            .unwrap();

        // Spawned one at a time, `bar` still chains behind `foo`.
        assert_eq!(after_message_id(&pool, first).await, Some(Uuid::nil()));
        assert_eq!(after_message_id(&pool, second).await, Some(first));

        let mut job = jobs.next().await;
        assert_eq!(job.id(), first);
        job.complete().await.unwrap();

        let mut job = jobs.next().await;
        assert_eq!(job.id(), second);
        job.complete().await.unwrap();

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    #[sqlx::test]
    async fn it_runs_jobs_in_parallel(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) = test_job_runner(&pool).await;

        let mut spawned = HashSet::new();
        spawned.insert(JobBuilder::new("foo").spawn(&pool).await.unwrap());
        spawned.insert(JobBuilder::new("bar").spawn(&pool).await.unwrap());

        // Neither job is completed before the other is dispatched: unordered
        // jobs must not wait for each other.
        let dispatched = jobs.next_n(2).await;
        assert_eq!(
            dispatched
                .iter()
                .map(|job| job.id())
                .collect::<HashSet<_>>(),
            spawned
        );

        for mut job in dispatched {
            job.complete().await.unwrap();
        }

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    /// A job which is never completed is retried, with the backoff doubling
    /// each time, until its attempts run out.
    ///
    /// Keep-alive is switched off here: its whole purpose is to postpone the
    /// retry of a job that is still running, which is precisely what this
    /// test measures. With it on, the retry schedule depends on how quickly
    /// the dispatched job happens to be dropped.
    #[sqlx::test]
    async fn it_retries_failed_jobs(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) =
            configured_job_runner(&pool, |options| options.set_keep_alive(false)).await;

        let start = Instant::now();
        let id = JobBuilder::new("foo")
            .set_retry_backoff(BACKOFF)
            .set_retries(2)
            .spawn(&pool)
            .await
            .unwrap();

        // The initial attempt plus two retries. Dropping a job without
        // completing it is what marks the attempt as failed.
        for _ in 0..3 {
            let job = jobs.next().await;
            assert_eq!(job.id(), id);
            drop(job);
        }

        // Each attempt is scheduled a backoff ahead of the previous one, and
        // `start` precedes the first attempt, so the elapsed time is a sound
        // lower bound: it fails if `mq_poll` ever hands out a message before
        // its `attempt_at`.
        assert!(
            start.elapsed() >= BACKOFF + BACKOFF * 2,
            "retries were delivered earlier than their backoff allows"
        );

        // `mq_poll` clears `attempt_at` as it hands out the last attempt, so
        // the message can never be polled again. That makes the state below
        // final, and establishes that there is no fourth attempt without
        // having to wait to see whether one turns up.
        assert_eq!(attempt_state(&pool, id).await, (0, false));

        // Three attempts, so the backoff has been doubled three times. This
        // is only read once the message is exhausted: reading it between
        // attempts would race the next poll.
        assert_eq!(retry_backoff(&pool, id).await, BACKOFF * 8);

        runner.stop().await;
    }

    /// Checkpointing replaces the payload used by the next attempt without
    /// consuming one, so the retry picks up where the first attempt left off.
    ///
    /// The runner is stopped for the duration of the checkpoint and restarted
    /// afterwards. Nothing about checkpointing requires that, but it removes
    /// the one window in which this test could race the implementation: a
    /// checkpoint carries no keep-alive of its own, so a runner left polling
    /// may hand out the retry before the new payload is committed and read
    /// the old one. Stopping the runner closes the window outright rather
    /// than making the backoff long enough to hide it.
    ///
    /// Keep-alive is switched off for the same reason as in
    /// `it_retries_failed_jobs`.
    #[sqlx::test]
    async fn it_can_checkpoint_jobs(pool: Pool<Postgres>) {
        let (mut runner, mut jobs) =
            configured_job_runner(&pool, |options| options.set_keep_alive(false)).await;

        let id = JobBuilder::new("foo")
            .set_retry_backoff(BACKOFF)
            .set_retries(5)
            .set_json(&false)
            .unwrap()
            .spawn(&pool)
            .await
            .unwrap();

        let mut job = jobs.next().await;
        assert_eq!(job.id(), id);
        assert_eq!(job.json::<bool>().unwrap(), Some(false));

        runner.stop().await;

        job.checkpoint(Checkpoint::new().set_json(&true).unwrap())
            .await
            .unwrap();
        assert_eq!(
            stored_payload(&pool, id).await.as_deref(),
            Some("true"),
            "the checkpoint should have replaced the stored payload"
        );

        // The attempt ends without the job being completed, so it is retried.
        drop(job);

        let (mut runner, mut jobs) =
            configured_job_runner(&pool, |options| options.set_keep_alive(false)).await;

        let mut job = jobs.next().await;
        assert_eq!(job.id(), id);
        assert_eq!(
            job.json::<bool>().unwrap(),
            Some(true),
            "the retry should see the checkpointed payload"
        );
        job.complete().await.unwrap();

        runner.stop().await;

        // Completing deletes the message, so there is no third attempt to
        // wait for.
        assert!(!exists(&pool, id).await.unwrap());
        assert_eq!(queue_len(&pool).await, 0);
    }

    #[sqlx::test]
    async fn it_can_use_registry(pool: Pool<Postgres>) {
        let (ran_tx, mut ran_rx) = mpsc::unbounded();
        let mut runner = named_job_runner(&pool, ran_tx).await;

        example_job1.builder().spawn(&pool).await.unwrap();
        example_job2.builder().spawn(&pool).await.unwrap();
        example_job_with_ctx.builder().spawn(&pool).await.unwrap();

        let mut ran = Vec::new();
        for _ in 0..3 {
            ran.push(
                tokio::time::timeout(TIMEOUT, ran_rx.next())
                    .await
                    .expect("timed out waiting for the registered jobs to run")
                    .expect("the job runner stopped before every job ran"),
            );
        }
        ran.sort_unstable();
        assert_eq!(
            ran,
            ["example_job1", "example_job2", "example_job_with_ctx"]
        );

        assert_eq!(queue_len(&pool).await, 0);
        runner.stop().await;
    }

    /// `mq_poll` claims candidate rows with `FOR UPDATE SKIP LOCKED`, so a
    /// poller must never block on, or hand back, rows another poller has
    /// already claimed.
    ///
    /// Both polls run inside explicit transactions: row locks are held until
    /// the transaction ends, so the second poll genuinely contends with the
    /// first. Without `SKIP LOCKED` the second poll blocks on the first
    /// transaction's row locks and this test times out; without the
    /// `MATERIALIZED` CTE the locking subquery can be re-executed and return
    /// rows already claimed, and the disjointness assertion fails.
    #[sqlx::test]
    async fn it_polls_disjoint_messages_from_concurrent_transactions(pool: Pool<Postgres>) {
        const NUM_JOBS: usize = 4;
        const BATCH_SIZE: i32 = 2;

        let builders: Vec<_> = (0..NUM_JOBS)
            .map(|_| JobBuilder::new("concurrent"))
            .collect();
        let spawned: HashSet<Uuid> = spawn_batch(&pool, &builders)
            .await
            .unwrap()
            .into_iter()
            .collect();

        async fn poll_ids(tx: &mut sqlx::Transaction<'_, Postgres>, batch_size: i32) -> Vec<Uuid> {
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM mq_poll($1, $2) WHERE id IS NOT NULL")
                .bind(Option::<Vec<String>>::None)
                .bind(batch_size)
                .fetch_all(&mut **tx)
                .await
                .unwrap()
        }

        let mut tx_a = pool.begin().await.unwrap();
        let mut tx_b = pool.begin().await.unwrap();

        // `tx_a` claims a batch and holds the locks: it is not committed yet.
        let claimed_a = poll_ids(&mut tx_a, BATCH_SIZE).await;
        assert_eq!(claimed_a.len(), BATCH_SIZE as usize);

        // `tx_b` must skip what `tx_a` holds rather than waiting for it.
        let claimed_b = tokio::time::timeout(TIMEOUT, poll_ids(&mut tx_b, BATCH_SIZE))
            .await
            .expect("concurrent poll blocked on the first transaction's row locks");
        assert_eq!(claimed_b.len(), BATCH_SIZE as usize);

        tx_a.commit().await.unwrap();
        tx_b.commit().await.unwrap();

        let set_a: HashSet<Uuid> = claimed_a.iter().copied().collect();
        let set_b: HashSet<Uuid> = claimed_b.iter().copied().collect();
        assert_eq!(
            set_a.len(),
            claimed_a.len(),
            "a poll returned duplicate ids"
        );
        assert_eq!(
            set_b.len(),
            claimed_b.len(),
            "a poll returned duplicate ids"
        );
        assert!(
            set_a.is_disjoint(&set_b),
            "concurrent polls claimed overlapping messages: {:?} and {:?}",
            set_a,
            set_b
        );
        assert_eq!(
            &set_a | &set_b,
            spawned,
            "the two polls together should have claimed every spawned job"
        );
    }

    /// End-to-end counterpart to the test above: several independent job
    /// runners sharing one database must between them deliver each job
    /// exactly once, and must all make progress rather than deadlocking or
    /// starving each other.
    ///
    /// Concurrency limits are kept low so that no single runner can drain the
    /// queue in one batch, forcing the runners to contend for the same rows.
    ///
    /// Unlike the test above this one does not single out `SKIP LOCKED` — it
    /// guards the delivery contract that the locking strategy exists to
    /// uphold, and so stays valid if that strategy is changed again.
    #[sqlx::test]
    async fn it_delivers_each_job_exactly_once_with_concurrent_runners(pool: Pool<Postgres>) {
        const NUM_RUNNERS: usize = 4;
        const NUM_JOBS: usize = 24;
        const NUM_CHANNELS: usize = 4;

        init_logging();

        let (tx, mut rx) = mpsc::unbounded();

        let mut runners = Vec::new();
        for runner_idx in 0..NUM_RUNNERS {
            let tx = tx.clone();
            let runner = JobRunnerOptions::new(&pool, move |job| {
                let _ = tx.unbounded_send((runner_idx, job));
            })
            .set_concurrency(2, 6)
            .run()
            .await
            .unwrap();
            runners.push(runner);
        }
        // Drop the spare sender so the receiver ends if every runner stops.
        drop(tx);

        // Spread the jobs over several channels so that `mq_active_channels`
        // is exercised as well as the per-channel batching.
        let channel_args: Vec<String> = (0..NUM_JOBS)
            .map(|i| (i % NUM_CHANNELS).to_string())
            .collect();
        let builders: Vec<_> = channel_args
            .iter()
            .map(|args| {
                let mut builder = JobBuilder::new("concurrent");
                builder.set_channel_args(args);
                builder
            })
            .collect();
        let spawned: HashSet<Uuid> = spawn_batch(&pool, &builders)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(spawned.len(), NUM_JOBS);

        let mut delivered = HashSet::new();
        let mut delivering_runners = HashSet::new();
        for _ in 0..NUM_JOBS {
            let (runner_idx, mut job) = tokio::time::timeout(TIMEOUT, rx.next())
                .await
                .expect("timed out waiting for jobs to be delivered")
                .expect("all runners stopped before every job was delivered");
            assert!(
                delivered.insert(job.id()),
                "job {} was delivered more than once",
                job.id()
            );
            delivering_runners.insert(runner_idx);
            job.complete().await.unwrap();
        }
        assert_eq!(delivered, spawned);

        // Anything already queued beyond the expected count is a redelivery.
        assert!(rx.try_recv().is_err(), "a job was delivered more than once");

        // Every job was completed, so the queue must be empty.
        assert_eq!(queue_len(&pool).await, 0);

        log::info!(
            "{} of {} runners took part in delivery",
            delivering_runners.len(),
            NUM_RUNNERS
        );

        for mut runner in runners {
            runner.stop().await;
        }
    }
}
