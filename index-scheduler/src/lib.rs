/*!
This crate defines the index scheduler, which is responsible for:
1. Keeping references to meilisearch's indexes and mapping them to their
user-defined names.
2. Scheduling tasks given by the user and executing them, in batch if possible.

When an `IndexScheduler` is created, a new thread containing a reference to the
scheduler is created. This thread runs the scheduler's run loop, where the
scheduler waits to be woken up to process new tasks. It wakes up when:

1. it is launched for the first time
2. a new task is registered
3. a batch of tasks has been processed

It is only within this thread that the scheduler is allowed to process tasks.
On the other hand, the publicly accessible methods of the scheduler can be
called asynchronously from any thread. These methods can either query the
content of the scheduler or enqueue new tasks.
*/

mod autobatcher;
mod batch;
pub mod error;
mod features;
mod index_mapper;
#[cfg(test)]
mod insta_snapshot;
mod lru;
mod utils;
pub mod uuid_codec;

pub type Result<T> = std::result::Result<T, Error>;
pub type TaskId = u32;

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{self, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dump::{KindDump, TaskDump, UpdateFile};
pub use error::Error;
pub use features::RoFeatures;
use file_store::FileStore;
use flate2::bufread::GzEncoder;
use flate2::Compression;
use shared_types::error::ResponseError;
use shared_types::features::{InstanceTogglableFeatures, RuntimeTogglableFeatures};
use shared_types::heed::byteorder::BE;
use shared_types::heed::types::{SerdeBincode, SerdeJson, Str, I128};
use shared_types::heed::{self, Database, Env, PutFlags, RoTxn, RwTxn};
use shared_types::search_engine::documents::DocumentsBatchBuilder;
use shared_types::search_engine::update::IndexerConfig;
use shared_types::search_engine::vector::{Embedder, EmbedderOptions, EmbeddingConfigs};
use shared_types::search_engine::{self, CboRoaringBitmapCodec, Index, RoaringBitmapCodec, BEU32};
use shared_types::task_view::TaskView;
use shared_types::tasks::{Kind, KindWithContent, Status, Task};
use puffin::FrameView;
use rayon::current_num_threads;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use roaring::RoaringBitmap;
use synchronoise::SignalEvent;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use utils::{filter_out_references_to_newer_tasks, keep_tasks_within_datetimes, map_bound};
use uuid::Uuid;

use crate::index_mapper::IndexMapper;
use crate::utils::{check_index_swap_validity, clamp_to_page_size};

pub(crate) type BEI128 = I128<BE>;

/// Defines a subset of tasks to be retrieved from the [`IndexScheduler`].
///
/// An empty/default query (where each field is set to `None`) matches all tasks.
/// Each non-null field restricts the set of tasks further.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The maximum number of tasks to be matched
    pub limit: Option<u32>,
    /// The minimum [task id](`shared_types::tasks::Task::uid`) to be matched
    pub from: Option<u32>,
    /// The allowed [statuses](`shared_types::tasks::Task::status`) of the matched tasls
    pub statuses: Option<Vec<Status>>,
    /// The allowed [kinds](shared_types::tasks::Kind) of the matched tasks.
    ///
    /// The kind of a task is given by:
    /// ```
    /// # use shared_types::tasks::{Task, Kind};
    /// # fn doc_func(task: Task) -> Kind {
    /// task.kind.as_kind()
    /// # }
    /// ```
    pub types: Option<Vec<Kind>>,
    /// The allowed [index ids](shared_types::tasks::Task::index_uid) of the matched tasks
    pub index_uids: Option<Vec<String>>,
    /// The [task ids](`shared_types::tasks::Task::uid`) to be matched
    pub uids: Option<Vec<TaskId>>,
    /// The [task ids](`shared_types::tasks::Task::uid`) of the [`TaskCancelation`](shared_types::tasks::Task::Kind::TaskCancelation) tasks
    /// that canceled the matched tasks.
    pub canceled_by: Option<Vec<TaskId>>,
    /// Exclusive upper bound of the matched tasks' [`enqueued_at`](shared_types::tasks::Task::enqueued_at) field.
    pub before_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`enqueued_at`](shared_types::tasks::Task::enqueued_at) field.
    pub after_enqueued_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`started_at`](shared_types::tasks::Task::started_at) field.
    pub before_started_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`started_at`](shared_types::tasks::Task::started_at) field.
    pub after_started_at: Option<OffsetDateTime>,
    /// Exclusive upper bound of the matched tasks' [`finished_at`](shared_types::tasks::Task::finished_at) field.
    pub before_finished_at: Option<OffsetDateTime>,
    /// Exclusive lower bound of the matched tasks' [`finished_at`](shared_types::tasks::Task::finished_at) field.
    pub after_finished_at: Option<OffsetDateTime>,
}

impl Query {
    /// Return `true` if every field of the query is set to `None`, such that the query
    /// matches all tasks.
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            Query {
                limit: None,
                from: None,
                statuses: None,
                types: None,
                index_uids: None,
                uids: None,
                canceled_by: None,
                before_enqueued_at: None,
                after_enqueued_at: None,
                before_started_at: None,
                after_started_at: None,
                before_finished_at: None,
                after_finished_at: None,
            }
        )
    }

    /// Add an [index id](shared_types::tasks::Task::index_uid) to the list of permitted indexes.
    pub fn with_index(self, index_uid: String) -> Self {
        let mut index_vec = self.index_uids.unwrap_or_default();
        index_vec.push(index_uid);
        Self { index_uids: Some(index_vec), ..self }
    }

    // Removes the `from` and `limit` restrictions from the query.
    // Useful to get the total number of tasks matching a filter.
    pub fn without_limits(self) -> Self {
        Query { limit: None, from: None, ..self }
    }
}

#[derive(Debug, Clone)]
struct ProcessingTasks {
    /// The date and time at which the indexation started.
    started_at: OffsetDateTime,
    /// The list of tasks ids that are currently running.
    processing: RoaringBitmap,
}

impl ProcessingTasks {
    /// Creates an empty `ProcessingAt` struct.
    fn new() -> ProcessingTasks {
        ProcessingTasks { started_at: OffsetDateTime::now_utc(), processing: RoaringBitmap::new() }
    }

    /// Stores the currently processing tasks, and the date time at which it started.
    fn start_processing_at(&mut self, started_at: OffsetDateTime, processing: RoaringBitmap) {
        self.started_at = started_at;
        self.processing = processing;
    }

    /// Set the processing tasks to an empty list
    fn stop_processing(&mut self) -> RoaringBitmap {
        std::mem::take(&mut self.processing)
    }

    /// Returns `true` if there, at least, is one task that is currently processing that we must stop.
    fn must_cancel_processing_tasks(&self, canceled_tasks: &RoaringBitmap) -> bool {
        !self.processing.is_disjoint(canceled_tasks)
    }
}

#[derive(Default, Clone, Debug)]
struct MustStopProcessing(Arc<AtomicBool>);

impl MustStopProcessing {
    fn get(&self) -> bool {
        self.0.load(Relaxed)
    }

    fn must_stop(&self) {
        self.0.store(true, Relaxed);
    }

    fn reset(&self) {
        self.0.store(false, Relaxed);
    }
}

/// Database const names for the `IndexScheduler`.
mod db_name {
    pub const ALL_TASKS: &str = "all-tasks";
    pub const STATUS: &str = "status";
    pub const KIND: &str = "kind";
    pub const INDEX_TASKS: &str = "index-tasks";
    pub const CANCELED_BY: &str = "canceled_by";
    pub const ENQUEUED_AT: &str = "enqueued-at";
    pub const STARTED_AT: &str = "started-at";
    pub const FINISHED_AT: &str = "finished-at";
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breakpoint {
    // this state is only encountered while creating the scheduler in the test suite.
    Init,

    Start,
    BatchCreated,
    BeforeProcessing,
    AfterProcessing,
    AbortedIndexation,
    ProcessBatchSucceeded,
    ProcessBatchFailed,
    InsideProcessBatch,
}

#[derive(Debug)]
pub struct IndexSchedulerOptions {
    /// The path to the version file of Meilisearch.
    pub version_file_path: PathBuf,
    /// The path to the folder containing the auth LMDB env.
    pub auth_path: PathBuf,
    /// The path to the folder containing the task databases.
    pub tasks_path: PathBuf,
    /// The path to the file store containing the files associated to the tasks.
    pub update_file_path: PathBuf,
    /// The path to the folder containing meilisearch's indexes.
    pub indexes_path: PathBuf,
    /// The path to the folder containing the snapshots.
    pub snapshots_path: PathBuf,
    /// The path to the folder containing the dumps.
    pub dumps_path: PathBuf,
    /// The URL on which we must send the tasks statuses
    pub webhook_url: Option<String>,
    /// The value we will send into the Authorization HTTP header on the webhook URL
    pub webhook_authorization_header: Option<String>,
    /// The maximum size, in bytes, of the task index.
    pub task_db_size: usize,
    /// The size, in bytes, with which a meilisearch index is opened the first time of each meilisearch index.
    pub index_base_map_size: usize,
    /// Whether we open a meilisearch index with the MDB_WRITEMAP option or not.
    pub enable_mdb_writemap: bool,
    /// The size, in bytes, by which the map size of an index is increased when it resized due to being full.
    pub index_growth_amount: usize,
    /// The number of indexes that can be concurrently opened in memory.
    pub index_count: usize,
    /// Configuration used during indexing for each meilisearch index.
    pub indexer_config: IndexerConfig,
    /// Set to `true` iff the index scheduler is allowed to automatically
    /// batch tasks together, to process multiple tasks at once.
    pub autobatching_enabled: bool,
    /// Set to `true` iff the index scheduler is allowed to automatically
    /// delete the finished tasks when there are too many tasks.
    pub cleanup_enabled: bool,
    /// The maximum number of tasks stored in the task queue before starting
    /// to auto schedule task deletions.
    pub max_number_of_tasks: usize,
    /// If the autobatcher is allowed to automatically batch tasks
    /// it will only batch this defined number of tasks at once.
    pub max_number_of_batched_tasks: usize,
    /// The experimental features enabled for this instance.
    pub instance_features: InstanceTogglableFeatures,
}

/// Structure which holds meilisearch's indexes and schedules the tasks
/// to be performed on them.
pub struct IndexScheduler {
    /// The LMDB environment which the DBs are associated with.
    pub(crate) env: Env,

    /// A boolean that can be set to true to stop the currently processing tasks.
    pub(crate) must_stop_processing: MustStopProcessing,

    /// The list of tasks currently processing
    pub(crate) processing_tasks: Arc<RwLock<ProcessingTasks>>,

    /// The list of files referenced by the tasks
    pub(crate) file_store: FileStore,

    // The main database, it contains all the tasks accessible by their Id.
    pub(crate) all_tasks: Database<BEU32, SerdeJson<Task>>,

    /// All the tasks ids grouped by their status.
    // TODO we should not be able to serialize a `Status::Processing` in this database.
    pub(crate) status: Database<SerdeBincode<Status>, RoaringBitmapCodec>,
    /// All the tasks ids grouped by their kind.
    pub(crate) kind: Database<SerdeBincode<Kind>, RoaringBitmapCodec>,
    /// Store the tasks associated to an index.
    pub(crate) index_tasks: Database<Str, RoaringBitmapCodec>,

    /// Store the tasks that were canceled by a task uid
    pub(crate) canceled_by: Database<BEU32, RoaringBitmapCodec>,

    /// Store the task ids of tasks which were enqueued at a specific date
    pub(crate) enqueued_at: Database<BEI128, CboRoaringBitmapCodec>,

    /// Store the task ids of finished tasks which started being processed at a specific date
    pub(crate) started_at: Database<BEI128, CboRoaringBitmapCodec>,

    /// Store the task ids of tasks which finished at a specific date
    pub(crate) finished_at: Database<BEI128, CboRoaringBitmapCodec>,

    /// In charge of creating, opening, storing and returning indexes.
    pub(crate) index_mapper: IndexMapper,

    /// In charge of fetching and setting the status of experimental features.
    features: features::FeatureData,

    /// Get a signal when a batch needs to be processed.
    pub(crate) wake_up: Arc<SignalEvent>,

    /// Whether auto-batching is enabled or not.
    pub(crate) autobatching_enabled: bool,

    /// Whether we should automatically cleanup the task queue or not.
    pub(crate) cleanup_enabled: bool,

    /// The max number of tasks allowed before the scheduler starts to delete
    /// the finished tasks automatically.
    pub(crate) max_number_of_tasks: usize,

    /// The maximum number of tasks that will be batched together.
    pub(crate) max_number_of_batched_tasks: usize,

    /// The webhook url we should send tasks to after processing every batches.
    pub(crate) webhook_url: Option<String>,
    /// The Authorization header to send to the webhook URL.
    pub(crate) webhook_authorization_header: Option<String>,

    /// A frame to output the indexation profiling files to disk.
    pub(crate) puffin_frame: Arc<puffin::GlobalFrameView>,

    /// The path used to create the dumps.
    pub(crate) dumps_path: PathBuf,

    /// The path used to create the snapshots.
    pub(crate) snapshots_path: PathBuf,

    /// The path to the folder containing the auth LMDB env.
    pub(crate) auth_path: PathBuf,

    /// The path to the version file of Meilisearch.
    pub(crate) version_file_path: PathBuf,

    embedders: Arc<RwLock<HashMap<EmbedderOptions, Arc<Embedder>>>>,

}

impl IndexScheduler {
    fn private_clone(&self) -> IndexScheduler {
        IndexScheduler {
            env: self.env.clone(),
            must_stop_processing: self.must_stop_processing.clone(),
            processing_tasks: self.processing_tasks.clone(),
            file_store: self.file_store.clone(),
            all_tasks: self.all_tasks,
            status: self.status,
            kind: self.kind,
            index_tasks: self.index_tasks,
            canceled_by: self.canceled_by,
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            index_mapper: self.index_mapper.clone(),
            wake_up: self.wake_up.clone(),
            autobatching_enabled: self.autobatching_enabled,
            cleanup_enabled: self.cleanup_enabled,
            max_number_of_tasks: self.max_number_of_tasks,
            max_number_of_batched_tasks: self.max_number_of_batched_tasks,
            puffin_frame: self.puffin_frame.clone(),
            snapshots_path: self.snapshots_path.clone(),
            dumps_path: self.dumps_path.clone(),
            auth_path: self.auth_path.clone(),
            version_file_path: self.version_file_path.clone(),
            webhook_url: self.webhook_url.clone(),
            webhook_authorization_header: self.webhook_authorization_header.clone(),
            embedders: self.embedders.clone(),
            features: self.features.clone(),
        }
    }
}

impl IndexScheduler {
    /// Create an index scheduler and start its run loop.
    pub fn new(
        options: IndexSchedulerOptions,
    ) -> Result<Self> {
        std::fs::create_dir_all(&options.tasks_path)?;
        std::fs::create_dir_all(&options.update_file_path)?;
        std::fs::create_dir_all(&options.indexes_path)?;

        if cfg!(windows) && options.enable_mdb_writemap {
            // programmer error if this happens: in normal use passing the option on Windows is an error in main
            panic!("Windows doesn't support the MDB_WRITEMAP LMDB option");
        }

        let task_db_size = clamp_to_page_size(options.task_db_size);
        let budget = if options.indexer_config.skip_index_budget {
            IndexBudget {
                map_size: options.index_base_map_size,
                index_count: options.index_count,
                task_db_size,
            }
        } else {
            Self::index_budget(
                &options.tasks_path,
                options.index_base_map_size,
                task_db_size,
                options.index_count,
            )
        };

        let env = heed::EnvOpenOptions::new()
            .max_dbs(11)
            .map_size(budget.task_db_size)
            .open(options.tasks_path)?;

        let features = features::FeatureData::new(&env, options.instance_features)?;

        let file_store = FileStore::new(&options.update_file_path)?;

        let mut wtxn = env.write_txn()?;
        let all_tasks = env.create_database(&mut wtxn, Some(db_name::ALL_TASKS))?;
        let status = env.create_database(&mut wtxn, Some(db_name::STATUS))?;
        let kind = env.create_database(&mut wtxn, Some(db_name::KIND))?;
        let index_tasks = env.create_database(&mut wtxn, Some(db_name::INDEX_TASKS))?;
        let canceled_by = env.create_database(&mut wtxn, Some(db_name::CANCELED_BY))?;
        let enqueued_at = env.create_database(&mut wtxn, Some(db_name::ENQUEUED_AT))?;
        let started_at = env.create_database(&mut wtxn, Some(db_name::STARTED_AT))?;
        let finished_at = env.create_database(&mut wtxn, Some(db_name::FINISHED_AT))?;
        wtxn.commit()?;

        // allow unreachable_code to get rids of the warning in the case of a test build.
        let this = Self {
            must_stop_processing: MustStopProcessing::default(),
            processing_tasks: Arc::new(RwLock::new(ProcessingTasks::new())),
            file_store,
            all_tasks,
            status,
            kind,
            index_tasks,
            canceled_by,
            enqueued_at,
            started_at,
            finished_at,
            index_mapper: IndexMapper::new(
                &env,
                options.indexes_path,
                budget.map_size,
                options.index_growth_amount,
                budget.index_count,
                options.enable_mdb_writemap,
                options.indexer_config,
            )?,
            env,
            wake_up: Arc::new(SignalEvent::auto(true)),
            puffin_frame: Arc::new(puffin::GlobalFrameView::default()),
            autobatching_enabled: options.autobatching_enabled,
            cleanup_enabled: options.cleanup_enabled,
            max_number_of_tasks: options.max_number_of_tasks,
            max_number_of_batched_tasks: options.max_number_of_batched_tasks,
            dumps_path: options.dumps_path,
            snapshots_path: options.snapshots_path,
            auth_path: options.auth_path,
            version_file_path: options.version_file_path,
            webhook_url: options.webhook_url,
            webhook_authorization_header: options.webhook_authorization_header,
            embedders: Default::default(),
            features,
        };

        this.run();
        Ok(this)
    }

    /// Return `Ok(())` if the index scheduler is able to access one of its database.
    pub fn health(&self) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        self.all_tasks.first(&rtxn)?;
        Ok(())
    }

    fn index_budget(
        tasks_path: &Path,
        base_map_size: usize,
        mut task_db_size: usize,
        max_index_count: usize,
    ) -> IndexBudget {
        #[cfg(windows)]
        const DEFAULT_BUDGET: usize = 6 * 1024 * 1024 * 1024 * 1024; // 6 TiB, 1 index
        #[cfg(not(windows))]
        const DEFAULT_BUDGET: usize = 80 * 1024 * 1024 * 1024 * 1024; // 80 TiB, 18 indexes

        let budget = if Self::is_good_heed(tasks_path, DEFAULT_BUDGET) {
            DEFAULT_BUDGET
        } else {
            tracing::debug!("determining budget with dichotomic search");
            utils::dichotomic_search(DEFAULT_BUDGET / 2, |map_size| {
                Self::is_good_heed(tasks_path, map_size)
            })
        };

        tracing::debug!("memmap budget: {budget}B");
        let mut budget = budget / 2;
        if task_db_size > (budget / 2) {
            task_db_size = clamp_to_page_size(budget * 2 / 5);
            tracing::debug!(
                "Decreasing max size of task DB to {task_db_size}B due to constrained memory space"
            );
        }
        budget -= task_db_size;

        // won't be mutated again
        let budget = budget;
        let task_db_size = task_db_size;

        tracing::debug!("index budget: {budget}B");
        let mut index_count = budget / base_map_size;
        if index_count < 2 {
            // take a bit less than half than the budget to make sure we can always afford to open an index
            let map_size = (budget * 2) / 5;
            // single index of max budget
            tracing::debug!("1 index of {map_size}B can be opened simultaneously.");
            return IndexBudget { map_size, index_count: 1, task_db_size };
        }
        // give us some space for an additional index when the cache is already full
        // decrement is OK because index_count >= 2.
        index_count -= 1;
        if index_count > max_index_count {
            index_count = max_index_count;
        }
        tracing::debug!("Up to {index_count} indexes of {base_map_size}B opened simultaneously.");
        IndexBudget { map_size: base_map_size, index_count, task_db_size }
    }

    fn is_good_heed(tasks_path: &Path, map_size: usize) -> bool {
        if let Ok(env) =
            heed::EnvOpenOptions::new().map_size(clamp_to_page_size(map_size)).open(tasks_path)
        {
            env.prepare_for_closing().wait();
            true
        } else {
            // We're treating all errors equally here, not only allocation errors.
            // This means there's a possiblity for the budget to lower due to errors different from allocation errors.
            // For persistent errors, this is OK as long as the task db is then reopened normally without ignoring the error this time.
            // For transient errors, this could lead to an instance with too low a budget.
            // However transient errors are: 1) less likely than persistent errors 2) likely to cause other issues down the line anyway.
            false
        }
    }

    pub fn read_txn(&self) -> Result<RoTxn> {
        self.env.read_txn().map_err(|e| e.into())
    }

    /// Start the run loop for the given index scheduler.
    ///
    /// This function will execute in a different thread and must be called
    /// only once per index scheduler.
    fn run(&self) {
        let run = self.private_clone();
        std::thread::Builder::new()
            .name(String::from("scheduler"))
            .spawn(move || {
                #[cfg(test)]
                run.breakpoint(Breakpoint::Init);

                run.wake_up.wait();

                loop {
                    let puffin_enabled = run.features().check_puffin().is_ok();
                    puffin::set_scopes_on(puffin_enabled);
                    puffin::GlobalProfiler::lock().new_frame();

                    match run.tick() {
                        Ok(TickOutcome::TickAgain(_)) => (),
                        Ok(TickOutcome::WaitForSignal) => run.wake_up.wait(),
                        Err(e) => {
                            tracing::error!("{e}");
                            // Wait one second when an irrecoverable error occurs.
                            if !e.is_recoverable() {
                                std::thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }

                    // Let's write the previous frame to disk but only if
                    // the user wanted to profile with puffin.
                    if puffin_enabled {
                        let mut frame_view = run.puffin_frame.lock();
                        if !frame_view.is_empty() {
                            let now = OffsetDateTime::now_utc();
                            let mut file = match File::create(format!("{}.puffin", now)) {
                                Ok(file) => file,
                                Err(e) => {
                                    tracing::error!("{e}");
                                    continue;
                                }
                            };
                            if let Err(e) = frame_view.save_to_writer(&mut file) {
                                tracing::error!("{e}");
                            }
                            if let Err(e) = file.sync_all() {
                                tracing::error!("{e}");
                            }
                            // We erase this frame view as it is no more useful. We want to
                            // measure the new frames now that we exported the previous ones.
                            *frame_view = FrameView::default();
                        }
                    }
                }
            })
            .unwrap();
    }

    pub fn indexer_config(&self) -> &IndexerConfig {
        &self.index_mapper.indexer_config
    }

    /// Return the real database size (i.e.: The size **with** the free pages)
    pub fn size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Return the used database size (i.e.: The size **without** the free pages)
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    /// Return the index corresponding to the name.
    ///
    /// * If the index wasn't opened before, the index will be opened.
    /// * If the index doesn't exist on disk, the `IndexNotFoundError` is thrown.
    ///
    /// ### Note
    ///
    /// As an `Index` requires a large swath of the virtual memory address space, correct usage of an `Index` does not
    /// keep its handle for too long.
    ///
    /// Some configurations also can't reasonably open multiple indexes at once.
    /// If you need to fetch information from or perform an action on all indexes,
    /// see the `try_for_each_index` function.
    pub fn index(&self, name: &str) -> Result<Index> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index(&rtxn, name)
    }

    /// Return the name of all indexes without opening them.
    pub fn index_names(&self) -> Result<Vec<String>> {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.index_names(&rtxn)
    }

    /// Attempts `f` for each index that exists known to the index scheduler.
    ///
    /// It is preferable to use this function rather than a loop that opens all indexes, as a way to avoid having all indexes opened,
    /// which is unsupported in general.
    ///
    /// Since `f` is allowed to return a result, and `Index` is cloneable, it is still possible to wrongly build e.g. a vector of
    /// all the indexes, but this function makes it harder and so less likely to do accidentally.
    ///
    /// If many indexes exist, this operation can take time to complete (in the order of seconds for a 1000 of indexes) as it needs to open
    /// all the indexes.
    pub fn try_for_each_index<U, V>(&self, f: impl FnMut(&str, &Index) -> Result<U>) -> Result<V>
    where
        V: FromIterator<U>,
    {
        let rtxn = self.env.read_txn()?;
        self.index_mapper.try_for_each_index(&rtxn, f)
    }

    /// Return the task ids matched by the given query from the index scheduler's point of view.
    pub(crate) fn get_task_ids(&self, rtxn: &RoTxn, query: &Query) -> Result<RoaringBitmap> {
        let ProcessingTasks {
            started_at: started_at_processing, processing: processing_tasks, ..
        } = self.processing_tasks.read().unwrap().clone();

        let mut tasks = self.all_task_ids(rtxn)?;

        if let Some(from) = &query.from {
            tasks.remove_range(from.saturating_add(1)..);
        }

        if let Some(status) = &query.statuses {
            let mut status_tasks = RoaringBitmap::new();
            for status in status {
                match status {
                    // special case for Processing tasks
                    Status::Processing => {
                        status_tasks |= &processing_tasks;
                    }
                    status => status_tasks |= &self.get_status(rtxn, *status)?,
                };
            }
            if !status.contains(&Status::Processing) {
                tasks -= &processing_tasks;
            }
            tasks &= status_tasks;
        }

        if let Some(uids) = &query.uids {
            let uids = RoaringBitmap::from_iter(uids);
            tasks &= &uids;
        }

        if let Some(canceled_by) = &query.canceled_by {
            let mut all_canceled_tasks = RoaringBitmap::new();
            for cancel_task_uid in canceled_by {
                if let Some(canceled_by_uid) = self.canceled_by.get(rtxn, cancel_task_uid)? {
                    all_canceled_tasks |= canceled_by_uid;
                }
            }

            // if the canceled_by has been specified but no task
            // matches then we prefer matching zero than all tasks.
            if all_canceled_tasks.is_empty() {
                return Ok(RoaringBitmap::new());
            } else {
                tasks &= all_canceled_tasks;
            }
        }

        if let Some(kind) = &query.types {
            let mut kind_tasks = RoaringBitmap::new();
            for kind in kind {
                kind_tasks |= self.get_kind(rtxn, *kind)?;
            }
            tasks &= &kind_tasks;
        }

        if let Some(index) = &query.index_uids {
            let mut index_tasks = RoaringBitmap::new();
            for index in index {
                index_tasks |= self.index_tasks(rtxn, index)?;
            }
            tasks &= &index_tasks;
        }

        // For the started_at filter, we need to treat the part of the tasks that are processing from the part of the
        // tasks that are not processing. The non-processing ones are filtered normally while the processing ones
        // are entirely removed unless the in-memory startedAt variable falls within the date filter.
        // Once we have filtered the two subsets, we put them back together and assign it back to `tasks`.
        tasks = {
            let (mut filtered_non_processing_tasks, mut filtered_processing_tasks) =
                (&tasks - &processing_tasks, &tasks & &processing_tasks);

            // special case for Processing tasks
            // A closure that clears the filtered_processing_tasks if their started_at date falls outside the given bounds
            let mut clear_filtered_processing_tasks =
                |start: Bound<OffsetDateTime>, end: Bound<OffsetDateTime>| {
                    let start = map_bound(start, |b| b.unix_timestamp_nanos());
                    let end = map_bound(end, |b| b.unix_timestamp_nanos());
                    let is_within_dates = RangeBounds::contains(
                        &(start, end),
                        &started_at_processing.unix_timestamp_nanos(),
                    );
                    if !is_within_dates {
                        filtered_processing_tasks.clear();
                    }
                };
            match (query.after_started_at, query.before_started_at) {
                (None, None) => (),
                (None, Some(before)) => {
                    clear_filtered_processing_tasks(Bound::Unbounded, Bound::Excluded(before))
                }
                (Some(after), None) => {
                    clear_filtered_processing_tasks(Bound::Excluded(after), Bound::Unbounded)
                }
                (Some(after), Some(before)) => {
                    clear_filtered_processing_tasks(Bound::Excluded(after), Bound::Excluded(before))
                }
            };

            keep_tasks_within_datetimes(
                rtxn,
                &mut filtered_non_processing_tasks,
                self.started_at,
                query.after_started_at,
                query.before_started_at,
            )?;
            filtered_non_processing_tasks | filtered_processing_tasks
        };

        keep_tasks_within_datetimes(
            rtxn,
            &mut tasks,
            self.enqueued_at,
            query.after_enqueued_at,
            query.before_enqueued_at,
        )?;

        keep_tasks_within_datetimes(
            rtxn,
            &mut tasks,
            self.finished_at,
            query.after_finished_at,
            query.before_finished_at,
        )?;

        if let Some(limit) = query.limit {
            tasks = tasks.into_iter().rev().take(limit as usize).collect();
        }

        Ok(tasks)
    }

    /// The returned structure contains:
    /// 1. The name of the property being observed can be `statuses`, `types`, or `indexes`.
    /// 2. The name of the specific data related to the property can be `enqueued` for the `statuses`, `settingsUpdate` for the `types`, or the name of the index for the `indexes`, for example.
    /// 3. The number of times the properties appeared.
    pub fn get_stats(&self) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let rtxn = self.read_txn()?;

        let mut res = BTreeMap::new();

        let processing_tasks = { self.processing_tasks.read().unwrap().processing.len() };

        res.insert(
            "statuses".to_string(),
            enum_iterator::all::<Status>()
                .map(|s| {
                    let tasks = self.get_status(&rtxn, s)?.len();
                    match s {
                        Status::Enqueued => Ok((s.to_string(), tasks - processing_tasks)),
                        Status::Processing => Ok((s.to_string(), processing_tasks)),
                        s => Ok((s.to_string(), tasks)),
                    }
                })
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );
        res.insert(
            "types".to_string(),
            enum_iterator::all::<Kind>()
                .map(|s| Ok((s.to_string(), self.get_kind(&rtxn, s)?.len())))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );
        res.insert(
            "indexes".to_string(),
            self.index_tasks
                .iter(&rtxn)?
                .map(|res| Ok(res.map(|(name, bitmap)| (name.to_string(), bitmap.len()))?))
                .collect::<Result<BTreeMap<String, u64>>>()?,
        );

        Ok(res)
    }

    // Return true if there is at least one task that is processing.
    pub fn is_task_processing(&self) -> Result<bool> {
        Ok(!self.processing_tasks.read().unwrap().processing.is_empty())
    }

    /// Return true iff there is at least one task associated with this index
    /// that is processing.
    pub fn is_index_processing(&self, index: &str) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        let processing_tasks = self.processing_tasks.read().unwrap().processing.clone();
        let index_tasks = self.index_tasks(&rtxn, index)?;
        let nbr_index_processing_tasks = processing_tasks.intersection_len(&index_tasks);
        Ok(nbr_index_processing_tasks > 0)
    }

    /// Return the task ids matching the query along with the total number of tasks
    /// by ignoring the from and limit parameters from the user's point of view.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_task_ids_from_authorized_indexes(
        &self,
        rtxn: &RoTxn,
        query: &Query,
        filters: &auth::AuthFilter,
    ) -> Result<(RoaringBitmap, u64)> {
        // compute all tasks matching the filter by ignoring the limits, to find the number of tasks matching
        // the filter.
        // As this causes us to compute the filter twice it is slightly inefficient, but doing it this way spares
        // us from modifying the underlying implementation, and the performance remains sufficient.
        // Should this change, we would modify `get_task_ids` to directly return the number of matching tasks.
        let total_tasks = self.get_task_ids(rtxn, &query.clone().without_limits())?;
        let mut tasks = self.get_task_ids(rtxn, query)?;

        // If the query contains a list of index uid or there is a finite list of authorized indexes,
        // then we must exclude all the kinds that aren't associated to one and only one index.
        if query.index_uids.is_some() || !filters.all_indexes_authorized() {
            for kind in enum_iterator::all::<Kind>().filter(|kind| !kind.related_to_one_index()) {
                tasks -= self.get_kind(rtxn, kind)?;
            }
        }

        // Any task that is internally associated with a non-authorized index
        // must be discarded.
        if !filters.all_indexes_authorized() {
            let all_indexes_iter = self.index_tasks.iter(rtxn)?;
            for result in all_indexes_iter {
                let (index, index_tasks) = result?;
                if !filters.is_index_authorized(index) {
                    tasks -= index_tasks;
                }
            }
        }

        Ok((tasks, total_tasks.len()))
    }

    /// Return the tasks matching the query from the user's point of view along
    /// with the total number of tasks matching the query, ignoring from and limit.
    ///
    /// There are two differences between an internal query and a query executed by
    /// the user.
    ///
    /// 1. IndexSwap tasks are not publicly associated with any index, but they are associated
    /// with many indexes internally.
    /// 2. The user may not have the rights to access the tasks (internally) associated with all indexes.
    pub fn get_tasks_from_authorized_indexes(
        &self,
        query: Query,
        filters: &auth::AuthFilter,
    ) -> Result<(Vec<Task>, u64)> {
        let rtxn = self.env.read_txn()?;

        let (tasks, total) = self.get_task_ids_from_authorized_indexes(&rtxn, &query, filters)?;
        let tasks = self.get_existing_tasks(
            &rtxn,
            tasks.into_iter().rev().take(query.limit.unwrap_or(u32::MAX) as usize),
        )?;

        let ProcessingTasks { started_at, processing, .. } =
            self.processing_tasks.read().map_err(|_| Error::CorruptedTaskQueue)?.clone();

        let ret = tasks.into_iter();
        if processing.is_empty() {
            Ok((ret.collect(), total))
        } else {
            Ok((
                ret.map(|task| {
                    if processing.contains(task.uid) {
                        Task { status: Status::Processing, started_at: Some(started_at), ..task }
                    } else {
                        task
                    }
                })
                .collect(),
                total,
            ))
        }
    }

    /// Register a new task in the scheduler.
    ///
    /// If it fails and data was associated with the task, it tries to delete the associated data.
    pub fn register(
        &self,
        kind: KindWithContent,
        task_id: Option<TaskId>,
        dry_run: bool,
    ) -> Result<Task> {
        let mut wtxn = self.env.write_txn()?;

        // if the task doesn't delete anything and 50% of the task queue is full, we must refuse to enqueue the incomming task
        if !matches!(&kind, KindWithContent::TaskDeletion { tasks, .. } if !tasks.is_empty())
            && (self.env.non_free_pages_size()? * 100) / self.env.info().map_size as u64 > 50
        {
            return Err(Error::NoSpaceLeftInTaskQueue);
        }

        let next_task_id = self.next_task_id(&wtxn)?;

        if let Some(uid) = task_id {
            if uid < next_task_id {
                return Err(Error::BadTaskId { received: uid, expected: next_task_id });
            }
        }

        let mut task = Task {
            uid: task_id.unwrap_or(next_task_id),
            enqueued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            error: None,
            canceled_by: None,
            details: kind.default_details(),
            status: Status::Enqueued,
            kind: kind.clone(),
        };
        // For deletion and cancelation tasks, we want to make extra sure that they
        // don't attempt to delete/cancel tasks that are newer than themselves.
        filter_out_references_to_newer_tasks(&mut task);
        // If the register task is an index swap task, verify that it is well-formed
        // (that it does not contain duplicate indexes).
        check_index_swap_validity(&task)?;

        // At this point the task is going to be registered and no further checks will be done
        if dry_run {
            return Ok(task);
        }

        // Get rid of the mutability.
        let task = task;

        self.all_tasks.put_with_flags(&mut wtxn, PutFlags::APPEND, &task.uid, &task)?;

        for index in task.indexes() {
            self.update_index(&mut wtxn, index, |bitmap| {
                bitmap.insert(task.uid);
            })?;
        }

        self.update_status(&mut wtxn, Status::Enqueued, |bitmap| {
            bitmap.insert(task.uid);
        })?;

        self.update_kind(&mut wtxn, task.kind.as_kind(), |bitmap| {
            bitmap.insert(task.uid);
        })?;

        utils::insert_task_datetime(&mut wtxn, self.enqueued_at, task.enqueued_at, task.uid)?;

        if let Err(e) = wtxn.commit() {
            self.delete_persisted_task_data(&task)?;
            return Err(e.into());
        }

        // If the registered task is a task cancelation
        // we inform the processing tasks to stop (if necessary).
        if let KindWithContent::TaskCancelation { tasks, .. } = kind {
            let tasks_to_cancel = RoaringBitmap::from_iter(tasks);
            if self.processing_tasks.read().unwrap().must_cancel_processing_tasks(&tasks_to_cancel)
            {
                self.must_stop_processing.must_stop();
            }
        }

        // notify the scheduler loop to execute a new tick
        self.wake_up.signal();

        Ok(task)
    }

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(&mut self) -> Result<Dump> {
        Dump::new(self)
    }

    /// Create a new index without any associated task.
    pub fn create_raw_index(
        &self,
        name: &str,
        date: Option<(OffsetDateTime, OffsetDateTime)>,
    ) -> Result<Index> {
        let wtxn = self.env.write_txn()?;
        let index = self.index_mapper.create_index(wtxn, name, date)?;
        Ok(index)
    }

    /// Create a file and register it in the index scheduler.
    ///
    /// The returned file and uuid can be used to associate
    /// some data to a task. The file will be kept until
    /// the task has been fully processed.
    pub fn create_update_file(&self, dry_run: bool) -> Result<(Uuid, file_store::File)> {
        if dry_run {
            Ok((Uuid::nil(), file_store::File::dry_file()?))
        } else {
            Ok(self.file_store.new_update()?)
        }
    }

    #[cfg(test)]
    pub fn create_update_file_with_uuid(&self, uuid: u128) -> Result<(Uuid, file_store::File)> {
        Ok(self.file_store.new_update_with_uuid(uuid)?)
    }

    /// The size on disk taken by all the updates files contained in the `IndexScheduler`, in bytes.
    pub fn compute_update_file_size(&self) -> Result<u64> {
        Ok(self.file_store.compute_total_size()?)
    }

    /// Delete a file from the index scheduler.
    ///
    /// Counterpart to the [`create_update_file`](IndexScheduler::create_update_file) method.
    pub fn delete_update_file(&self, uuid: Uuid) -> Result<()> {
        Ok(self.file_store.delete(uuid)?)
    }

    /// Perform one iteration of the run loop.
    ///
    /// 1. See if we need to cleanup the task queue
    /// 2. Find the next batch of tasks to be processed.
    /// 3. Update the information of these tasks following the start of their processing.
    /// 4. Update the in-memory list of processed tasks accordingly.
    /// 5. Process the batch:
    ///    - perform the actions of each batched task
    ///    - update the information of each batched task following the end
    ///      of their processing.
    /// 6. Reset the in-memory list of processed tasks.
    ///
    /// Returns the number of processed tasks.
    fn tick(&self) -> Result<TickOutcome> {

        if self.cleanup_enabled {
            self.cleanup_task_queue()?;
        }

        let rtxn = self.env.read_txn().map_err(Error::HeedTransaction)?;
        let batch =
            match self.create_next_batch(&rtxn).map_err(|e| Error::CreateBatch(Box::new(e)))? {
                Some(batch) => batch,
                None => return Ok(TickOutcome::WaitForSignal),
            };
        let index_uid = batch.index_uid().map(ToOwned::to_owned);
        drop(rtxn);

        // 1. store the starting date with the bitmap of processing tasks.
        let ids = batch.ids();
        let processed_tasks = ids.len();
        let started_at = OffsetDateTime::now_utc();

        // We reset the must_stop flag to be sure that we don't stop processing tasks
        self.must_stop_processing.reset();
        self.processing_tasks.write().unwrap().start_processing_at(started_at, ids.clone());

        #[cfg(test)]
        self.breakpoint(Breakpoint::BatchCreated);

        // 2. Process the tasks
        let res = {
            let cloned_index_scheduler = self.private_clone();
            let handle = std::thread::Builder::new()
                .name(String::from("batch-operation"))
                .spawn(move || cloned_index_scheduler.process_batch(batch))
                .unwrap();
            handle.join().unwrap_or(Err(Error::ProcessBatchPanicked))
        };

        // Reset the currently updating index to relinquish the index handle
        self.index_mapper.set_currently_updating_index(None);


        let mut wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;

        let finished_at = OffsetDateTime::now_utc();
        match res {
            Ok(tasks) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::ProcessBatchSucceeded);

                let mut success = 0;
                let mut failure = 0;

                #[allow(unused_variables)]
                for (i, mut task) in tasks.into_iter().enumerate() {
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);


                    match task.error {
                        Some(_) => failure += 1,
                        None => success += 1,
                    }

                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
                tracing::info!("A batch of tasks was successfully completed with {success} successful tasks and {failure} failed tasks.");
            }
            // If we have an abortion error we must stop the tick here and re-schedule tasks.
            Err(Error::Milli(search_engine::Error::InternalError(
                search_engine::InternalError::AbortedIndexation,
            )))
            | Err(Error::AbortedTask) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::AbortedIndexation);
                wtxn.abort();

                tracing::info!("A batch of tasks was aborted.");
                // We make sure that we don't call `stop_processing` on the `processing_tasks`,
                // this is because we want to let the next tick call `create_next_batch` and keep
                // the `started_at` date times and `processings` of the current processing tasks.
                // This date time is used by the task cancelation to store the right `started_at`
                // date in the task on disk.
                return Ok(TickOutcome::TickAgain(0));
            }
            // If an index said it was full, we need to:
            // 1. identify which index is full
            // 2. close the associated environment
            // 3. resize it
            // 4. re-schedule tasks
            Err(Error::Milli(search_engine::Error::UserError(
                search_engine::UserError::MaxDatabaseSizeReached,
            ))) if index_uid.is_some() => {
                // fixme: add index_uid to match to avoid the unwrap
                let index_uid = index_uid.unwrap();
                // fixme: handle error more gracefully? not sure when this could happen
                self.index_mapper.resize_index(&wtxn, &index_uid)?;
                wtxn.abort();

                tracing::info!("The max database size was reached. Resizing the index.");

                return Ok(TickOutcome::TickAgain(0));
            }
            // In case of a failure we must get back and patch all the tasks with the error.
            Err(err) => {
                #[cfg(test)]
                self.breakpoint(Breakpoint::ProcessBatchFailed);
                let error: ResponseError = err.into();
                for id in ids.iter() {
                    let mut task = self
                        .get_task(&wtxn, id)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                        .ok_or(Error::CorruptedTaskQueue)?;
                    task.started_at = Some(started_at);
                    task.finished_at = Some(finished_at);
                    task.status = Status::Failed;
                    task.error = Some(error.clone());
                    task.details = task.details.map(|d| d.to_failed());



                    tracing::info!("Batch failed {}", error);

                    self.update_task(&mut wtxn, &task)
                        .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?;
                }
            }
        }

        let processed = self.processing_tasks.write().unwrap().stop_processing();



        wtxn.commit().map_err(Error::HeedTransaction)?;

        // Once the tasks are committed, we should delete all the update files associated ASAP to avoid leaking files in case of a restart
        tracing::debug!("Deleting the update files");

        //We take one read transaction **per thread**. Then, every thread is going to pull out new IDs from the roaring bitmap with the help of an atomic shared index into the bitmap
        let idx = AtomicU32::new(0);
        (0..current_num_threads()).into_par_iter().try_for_each(|_| -> Result<()> {
            let rtxn = self.read_txn()?;
            while let Some(id) = ids.select(idx.fetch_add(1, Ordering::Relaxed)) {
                let task = self
                    .get_task(&rtxn, id)
                    .map_err(|e| Error::TaskDatabaseUpdate(Box::new(e)))?
                    .ok_or(Error::CorruptedTaskQueue)?;
                if let Err(e) = self.delete_persisted_task_data(&task) {
                    tracing::error!(
                        "Failure to delete the content files associated with task {}. Error: {e}",
                        task.uid
                    );
                }
            }
            Ok(())
        })?;

        // We shouldn't crash the tick function if we can't send data to the webhook.
        let _ = self.notify_webhook(&processed);

        #[cfg(test)]
        self.breakpoint(Breakpoint::AfterProcessing);

        Ok(TickOutcome::TickAgain(processed_tasks))
    }

    /// Once the tasks changes have been committed we must send all the tasks that were updated to our webhook if there is one.
    fn notify_webhook(&self, updated: &RoaringBitmap) -> Result<()> {
        if let Some(ref url) = self.webhook_url {
            struct TaskReader<'a, 'b> {
                rtxn: &'a RoTxn<'a>,
                index_scheduler: &'a IndexScheduler,
                tasks: &'b mut roaring::bitmap::Iter<'b>,
                buffer: Vec<u8>,
                written: usize,
            }

            impl<'a, 'b> Read for TaskReader<'a, 'b> {
                fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
                    if self.buffer.is_empty() {
                        match self.tasks.next() {
                            None => return Ok(0),
                            Some(task_id) => {
                                let task = self
                                    .index_scheduler
                                    .get_task(self.rtxn, task_id)
                                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::Other,
                                            Error::CorruptedTaskQueue,
                                        )
                                    })?;

                                serde_json::to_writer(
                                    &mut self.buffer,
                                    &TaskView::from_task(&task),
                                )?;
                                self.buffer.push(b'\n');
                            }
                        }
                    }

                    let mut to_write = &self.buffer[self.written..];
                    let wrote = io::copy(&mut to_write, &mut buf)?;
                    self.written += wrote as usize;

                    // we wrote everything and must refresh our buffer on the next call
                    if self.written == self.buffer.len() {
                        self.written = 0;
                        self.buffer.clear();
                    }

                    Ok(wrote as usize)
                }
            }

            let rtxn = self.env.read_txn()?;

            let task_reader = TaskReader {
                rtxn: &rtxn,
                index_scheduler: self,
                tasks: &mut updated.into_iter(),
                buffer: Vec::with_capacity(50), // on average a task is around ~100 bytes
                written: 0,
            };

            // let reader = GzEncoder::new(BufReader::new(task_reader), Compression::default());
            let reader = GzEncoder::new(BufReader::new(task_reader), Compression::default());
            let request = ureq::post(url)
                .timeout(Duration::from_secs(30))
                .set("Content-Encoding", "gzip")
                .set("Content-Type", "application/x-ndjson");
            let request = match &self.webhook_authorization_header {
                Some(header) => request.set("Authorization", header),
                None => request,
            };

            if let Err(e) = request.send(reader) {
                tracing::error!("While sending data to the webhook: {e}");
            }
        }

        Ok(())
    }

    /// Register a task to cleanup the task queue if needed
    fn cleanup_task_queue(&self) -> Result<()> {
        let rtxn = self.env.read_txn().map_err(Error::HeedTransaction)?;

        let nb_tasks = self.all_task_ids(&rtxn)?.len();
        // if we have less than 1M tasks everything is fine
        if nb_tasks < self.max_number_of_tasks as u64 {
            return Ok(());
        }

        let finished = self.status.get(&rtxn, &Status::Succeeded)?.unwrap_or_default()
            | self.status.get(&rtxn, &Status::Failed)?.unwrap_or_default()
            | self.status.get(&rtxn, &Status::Canceled)?.unwrap_or_default();

        let to_delete = RoaringBitmap::from_iter(finished.into_iter().rev().take(100_000));

        // /!\ the len must be at least 2 or else we might enter an infinite loop where we only delete
        //     the deletion tasks we enqueued ourselves.
        if to_delete.len() < 2 {
            tracing::warn!("The task queue is almost full, but no task can be deleted yet.");
            // the only thing we can do is hope that the user tasks are going to finish
            return Ok(());
        }

        tracing::info!(
            "The task queue is almost full. Deleting the oldest {} finished tasks.",
            to_delete.len()
        );

        // it's safe to unwrap here because we checked the len above
        let newest_task_id = to_delete.iter().last().unwrap();
        let last_task_to_delete =
            self.get_task(&rtxn, newest_task_id)?.ok_or(Error::CorruptedTaskQueue)?;
        drop(rtxn);

        // increase time by one nanosecond so that the enqueuedAt of the last task to delete is also lower than that date.
        let delete_before = last_task_to_delete.enqueued_at + Duration::from_nanos(1);

        self.register(
            KindWithContent::TaskDeletion {
                query: format!(
                    "?beforeEnqueuedAt={}&statuses=succeeded,failed,canceled",
                    delete_before.format(&Rfc3339).map_err(|_| Error::CorruptedTaskQueue)?,
                ),
                tasks: to_delete,
            },
            None,
            false,
        )?;

        Ok(())
    }

    pub fn index_stats(&self, index_uid: &str) -> Result<IndexStats> {
        let is_indexing = self.is_index_processing(index_uid)?;
        let rtxn = self.read_txn()?;
        let index_stats = self.index_mapper.stats_of(&rtxn, index_uid)?;

        Ok(IndexStats { is_indexing, inner_stats: index_stats })
    }

    pub fn features(&self) -> RoFeatures {
        self.features.features()
    }

    pub fn put_runtime_features(&self, features: RuntimeTogglableFeatures) -> Result<()> {
        let wtxn = self.env.write_txn().map_err(Error::HeedTransaction)?;
        self.features.put_runtime_features(wtxn, features)?;
        Ok(())
    }

    pub(crate) fn delete_persisted_task_data(&self, task: &Task) -> Result<()> {
        match task.content_uuid() {
            Some(content_file) => self.delete_update_file(content_file),
            None => Ok(()),
        }
    }

    // TODO: consider using a type alias or a struct embedder/template
    pub fn embedders(
        &self,
        embedding_configs: Vec<(String, search_engine::vector::EmbeddingConfig)>,
    ) -> Result<EmbeddingConfigs> {
        let res: Result<_> = embedding_configs
            .into_iter()
            .map(|(name, search_engine::vector::EmbeddingConfig { embedder_options, prompt })| {
                let prompt =
                    Arc::new(prompt.try_into().map_err(shared_types::search_engine::Error::from)?);
                // optimistically return existing embedder
                {
                    let embedders = self.embedders.read().unwrap();
                    if let Some(embedder) = embedders.get(&embedder_options) {
                        return Ok((name, (embedder.clone(), prompt)));
                    }
                }

                // add missing embedder
                let embedder = Arc::new(
                    Embedder::new(embedder_options.clone())
                        .map_err(shared_types::search_engine::vector::Error::from)
                        .map_err(shared_types::search_engine::Error::from)?,
                );
                {
                    let mut embedders = self.embedders.write().unwrap();
                    embedders.insert(embedder_options, embedder.clone());
                }
                Ok((name, (embedder, prompt)))
            })
            .collect();
        res.map(EmbeddingConfigs::new)
    }

}

pub struct Dump<'a> {
    index_scheduler: &'a IndexScheduler,
    wtxn: RwTxn<'a>,

    indexes: HashMap<String, RoaringBitmap>,
    statuses: HashMap<Status, RoaringBitmap>,
    kinds: HashMap<Kind, RoaringBitmap>,
}

impl<'a> Dump<'a> {
    pub(crate) fn new(index_scheduler: &'a mut IndexScheduler) -> Result<Self> {

        let wtxn = index_scheduler.env.write_txn()?;

        Ok(Dump {
            index_scheduler,
            wtxn,
            indexes: HashMap::new(),
            statuses: HashMap::new(),
            kinds: HashMap::new(),
        })
    }

    /// Register a new task coming from a dump in the scheduler.
    /// By taking a mutable ref we're pretty sure no one will ever import a dump while actix is running.
    pub fn register_dumped_task(
        &mut self,
        task: TaskDump,
        content_file: Option<Box<UpdateFile>>,
    ) -> Result<Task> {
        let content_uuid = match content_file {
            Some(content_file) if task.status == Status::Enqueued => {
                let (uuid, mut file) = self.index_scheduler.create_update_file(false)?;
                let mut builder = DocumentsBatchBuilder::new(&mut file);
                for doc in content_file {
                    builder.append_json_object(&doc?)?;
                }
                builder.into_inner()?;
                file.persist()?;

                Some(uuid)
            }
            // If the task isn't `Enqueued` then just generate a recognisable `Uuid`
            // in case we try to open it later.
            _ if task.status != Status::Enqueued => Some(Uuid::nil()),
            _ => None,
        };

        let task = Task {
            uid: task.uid,
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
            error: task.error,
            canceled_by: task.canceled_by,
            details: task.details,
            status: task.status,
            kind: match task.kind {
                KindDump::DocumentImport {
                    primary_key,
                    method,
                    documents_count,
                    allow_index_creation,
                } => KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                    method,
                    content_file: content_uuid.ok_or(Error::CorruptedDump)?,
                    documents_count,
                    allow_index_creation,
                },
                KindDump::DocumentDeletion { documents_ids } => KindWithContent::DocumentDeletion {
                    documents_ids,
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::DocumentDeletionByFilter { filter } => {
                    KindWithContent::DocumentDeletionByFilter {
                        filter_expr: filter,
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    }
                }
                KindDump::DocumentClear => KindWithContent::DocumentClear {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::Settings { settings, is_deletion, allow_index_creation } => {
                    KindWithContent::SettingsUpdate {
                        index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                        new_settings: settings,
                        is_deletion,
                        allow_index_creation,
                    }
                }
                KindDump::IndexDeletion => KindWithContent::IndexDeletion {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                },
                KindDump::IndexCreation { primary_key } => KindWithContent::IndexCreation {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexUpdate { primary_key } => KindWithContent::IndexUpdate {
                    index_uid: task.index_uid.ok_or(Error::CorruptedDump)?,
                    primary_key,
                },
                KindDump::IndexSwap { swaps } => KindWithContent::IndexSwap { swaps },
                KindDump::TaskCancelation { query, tasks } => {
                    KindWithContent::TaskCancelation { query, tasks }
                }
                KindDump::TasksDeletion { query, tasks } => {
                    KindWithContent::TaskDeletion { query, tasks }
                }
                KindDump::DumpCreation { keys, instance_uid } => {
                    KindWithContent::DumpCreation { keys, instance_uid }
                }
                KindDump::SnapshotCreation => KindWithContent::SnapshotCreation,
            },
        };

        self.index_scheduler.all_tasks.put(&mut self.wtxn, &task.uid, &task)?;

        for index in task.indexes() {
            match self.indexes.get_mut(index) {
                Some(bitmap) => {
                    bitmap.insert(task.uid);
                }
                None => {
                    let mut bitmap = RoaringBitmap::new();
                    bitmap.insert(task.uid);
                    self.indexes.insert(index.to_string(), bitmap);
                }
            };
        }

        utils::insert_task_datetime(
            &mut self.wtxn,
            self.index_scheduler.enqueued_at,
            task.enqueued_at,
            task.uid,
        )?;

        // we can't override the started_at & finished_at, so we must only set it if the tasks is finished and won't change
        if matches!(task.status, Status::Succeeded | Status::Failed | Status::Canceled) {
            if let Some(started_at) = task.started_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.started_at,
                    started_at,
                    task.uid,
                )?;
            }
            if let Some(finished_at) = task.finished_at {
                utils::insert_task_datetime(
                    &mut self.wtxn,
                    self.index_scheduler.finished_at,
                    finished_at,
                    task.uid,
                )?;
            }
        }

        self.statuses.entry(task.status).or_default().insert(task.uid);
        self.kinds.entry(task.kind.as_kind()).or_default().insert(task.uid);

        Ok(task)
    }

    /// Commit all the changes and exit the importing dump state
    pub fn finish(mut self) -> Result<()> {
        for (index, bitmap) in self.indexes {
            self.index_scheduler.index_tasks.put(&mut self.wtxn, &index, &bitmap)?;
        }
        for (status, bitmap) in self.statuses {
            self.index_scheduler.put_status(&mut self.wtxn, status, &bitmap)?;
        }
        for (kind, bitmap) in self.kinds {
            self.index_scheduler.put_kind(&mut self.wtxn, kind, &bitmap)?;
        }

        self.wtxn.commit()?;
        self.index_scheduler.wake_up.signal();

        Ok(())
    }
}

/// The outcome of calling the [`IndexScheduler::tick`] function.
pub enum TickOutcome {
    /// The scheduler should immediately attempt another `tick`.
    ///
    /// The `usize` field contains the number of processed tasks.
    TickAgain(u64),
    /// The scheduler should wait for an external signal before attempting another `tick`.
    WaitForSignal,
}

/// How many indexes we can afford to have open simultaneously.
struct IndexBudget {
    /// Map size of an index.
    map_size: usize,
    /// Maximum number of simultaneously opened indexes.
    index_count: usize,
    /// For very constrained systems we might need to reduce the base task_db_size so we can accept at least one index.
    task_db_size: usize,
}

/// The statistics that can be computed from an `Index` object and the scheduler.
///
/// Compared with `index_mapper::IndexStats`, it adds the scheduling status.
#[derive(Debug)]
pub struct IndexStats {
    /// Whether this index is currently performing indexation, according to the scheduler.
    pub is_indexing: bool,
    /// Internal stats computed from the index.
    pub inner_stats: index_mapper::IndexStats,
}
