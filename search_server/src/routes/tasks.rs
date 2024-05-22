use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebQueryParameter;
use deserr::Deserr;
use index_scheduler::{IndexScheduler, Query, TaskId};
use shared_types::deserr::query_params::Param;
use shared_types::deserr::DeserrQueryParamError;
use shared_types::error::deserr_codes::*;
use shared_types::error::{InvalidTaskDateError, ResponseError};
use shared_types::index_uid::IndexUid;
use shared_types::star_or::{OptionStarOr, OptionStarOrList};
use shared_types::task_view::TaskView;
use shared_types::tasks::{Kind, KindWithContent, Status};
use serde::Serialize;
use serde_json::json;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime, Time};
use tokio::task;

use super::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::Opt;

const DEFAULT_LIMIT: u32 = 20;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_tasks)))
            .route(web::delete().to(SeqHandler(delete_tasks))),
    )
    .service(web::resource("/cancel").route(web::post().to(SeqHandler(cancel_tasks))))
    .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}
#[derive(Debug, Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct TasksFilterQuery {
    #[deserr(default = Param(DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidTaskLimit>)]
    pub limit: Param<u32>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskFrom>)]
    pub from: Option<Param<TaskId>>,

    #[deserr(default, error = DeserrQueryParamError<InvalidTaskUids>)]
    pub uids: OptionStarOrList<u32>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskCanceledBy>)]
    pub canceled_by: OptionStarOrList<u32>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskTypes>)]
    pub types: OptionStarOrList<Kind>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskStatuses>)]
    pub statuses: OptionStarOrList<Status>,
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexUid>)]
    pub index_uids: OptionStarOrList<IndexUid>,

    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_enqueued_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_enqueued_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_started_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_started_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_finished_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_finished_at: OptionStarOr<OffsetDateTime>,
}

impl TasksFilterQuery {
    fn into_query(self) -> Query {
        Query {
            limit: Some(self.limit.0),
            from: self.from.as_deref().copied(),
            statuses: self.statuses.merge_star_and_none(),
            types: self.types.merge_star_and_none(),
            index_uids: self.index_uids.map(|x| x.to_string()).merge_star_and_none(),
            uids: self.uids.merge_star_and_none(),
            canceled_by: self.canceled_by.merge_star_and_none(),
            before_enqueued_at: self.before_enqueued_at.merge_star_and_none(),
            after_enqueued_at: self.after_enqueued_at.merge_star_and_none(),
            before_started_at: self.before_started_at.merge_star_and_none(),
            after_started_at: self.after_started_at.merge_star_and_none(),
            before_finished_at: self.before_finished_at.merge_star_and_none(),
            after_finished_at: self.after_finished_at.merge_star_and_none(),
        }
    }
}

impl TaskDeletionOrCancelationQuery {
    fn is_empty(&self) -> bool {
        matches!(
            self,
            TaskDeletionOrCancelationQuery {
                uids: OptionStarOrList::None,
                canceled_by: OptionStarOrList::None,
                types: OptionStarOrList::None,
                statuses: OptionStarOrList::None,
                index_uids: OptionStarOrList::None,
                after_enqueued_at: OptionStarOr::None,
                before_enqueued_at: OptionStarOr::None,
                after_started_at: OptionStarOr::None,
                before_started_at: OptionStarOr::None,
                after_finished_at: OptionStarOr::None,
                before_finished_at: OptionStarOr::None
            }
        )
    }
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct TaskDeletionOrCancelationQuery {
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskUids>)]
    pub uids: OptionStarOrList<u32>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskCanceledBy>)]
    pub canceled_by: OptionStarOrList<u32>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskTypes>)]
    pub types: OptionStarOrList<Kind>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskStatuses>)]
    pub statuses: OptionStarOrList<Status>,
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexUid>)]
    pub index_uids: OptionStarOrList<IndexUid>,

    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_enqueued_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_enqueued_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_started_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_started_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    pub after_finished_at: OptionStarOr<OffsetDateTime>,
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    pub before_finished_at: OptionStarOr<OffsetDateTime>,
}

impl TaskDeletionOrCancelationQuery {
    fn into_query(self) -> Query {
        Query {
            limit: None,
            from: None,
            statuses: self.statuses.merge_star_and_none(),
            types: self.types.merge_star_and_none(),
            index_uids: self.index_uids.map(|x| x.to_string()).merge_star_and_none(),
            uids: self.uids.merge_star_and_none(),
            canceled_by: self.canceled_by.merge_star_and_none(),
            before_enqueued_at: self.before_enqueued_at.merge_star_and_none(),
            after_enqueued_at: self.after_enqueued_at.merge_star_and_none(),
            before_started_at: self.before_started_at.merge_star_and_none(),
            after_started_at: self.after_started_at.merge_star_and_none(),
            before_finished_at: self.before_finished_at.merge_star_and_none(),
            after_finished_at: self.after_finished_at.merge_star_and_none(),
        }
    }
}

async fn cancel_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_CANCEL }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TaskDeletionOrCancelationQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();

    if params.is_empty() {
        return Err(index_scheduler::Error::TaskCancelationWithEmptyQuery.into());
    }

    analytics.publish(
        "Tasks Canceled".to_string(),
        json!({
            "filtered_by_uid": params.uids.is_some(),
            "filtered_by_index_uid": params.index_uids.is_some(),
            "filtered_by_type": params.types.is_some(),
            "filtered_by_status": params.statuses.is_some(),
            "filtered_by_canceled_by": params.canceled_by.is_some(),
            "filtered_by_before_enqueued_at": params.before_enqueued_at.is_some(),
            "filtered_by_after_enqueued_at": params.after_enqueued_at.is_some(),
            "filtered_by_before_started_at": params.before_started_at.is_some(),
            "filtered_by_after_started_at": params.after_started_at.is_some(),
            "filtered_by_before_finished_at": params.before_finished_at.is_some(),
            "filtered_by_after_finished_at": params.after_finished_at.is_some(),
        }),
        Some(&req),
    );

    let query = params.into_query();

    let (tasks, _) = index_scheduler.get_task_ids_from_authorized_indexes(
        &index_scheduler.read_txn()?,
        &query,
        index_scheduler.filters(),
    )?;
    let task_cancelation =
        KindWithContent::TaskCancelation { query: format!("?{}", req.query_string()), tasks };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task =
        task::spawn_blocking(move || index_scheduler.register(task_cancelation, uid, dry_run))
            .await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

async fn delete_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_DELETE }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TaskDeletionOrCancelationQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();

    if params.is_empty() {
        return Err(index_scheduler::Error::TaskDeletionWithEmptyQuery.into());
    }

    analytics.publish(
        "Tasks Deleted".to_string(),
        json!({
            "filtered_by_uid": params.uids.is_some(),
            "filtered_by_index_uid": params.index_uids.is_some(),
            "filtered_by_type": params.types.is_some(),
            "filtered_by_status": params.statuses.is_some(),
            "filtered_by_canceled_by": params.canceled_by.is_some(),
            "filtered_by_before_enqueued_at": params.before_enqueued_at.is_some(),
            "filtered_by_after_enqueued_at": params.after_enqueued_at.is_some(),
            "filtered_by_before_started_at": params.before_started_at.is_some(),
            "filtered_by_after_started_at": params.after_started_at.is_some(),
            "filtered_by_before_finished_at": params.before_finished_at.is_some(),
            "filtered_by_after_finished_at": params.after_finished_at.is_some(),
        }),
        Some(&req),
    );
    let query = params.into_query();

    let (tasks, _) = index_scheduler.get_task_ids_from_authorized_indexes(
        &index_scheduler.read_txn()?,
        &query,
        index_scheduler.filters(),
    )?;
    let task_deletion =
        KindWithContent::TaskDeletion { query: format!("?{}", req.query_string()), tasks };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = task::spawn_blocking(move || index_scheduler.register(task_deletion, uid, dry_run))
        .await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

#[derive(Debug, Serialize)]
pub struct AllTasks {
    results: Vec<TaskView>,
    total: u64,
    limit: u32,
    from: Option<u32>,
    next: Option<u32>,
}

async fn get_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TasksFilterQuery, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let mut params = params.into_inner();
    // We +1 just to know if there is more after this "page" or not.
    params.limit.0 = params.limit.0.saturating_add(1);
    let limit = params.limit.0;
    let query = params.into_query();

    let filters = index_scheduler.filters();
    let (tasks, total) = index_scheduler.get_tasks_from_authorized_indexes(query, filters)?;
    let mut results: Vec<_> = tasks.iter().map(TaskView::from_task).collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if results.len() == limit as usize { results.pop().map(|t| t.uid) } else { None };

    let from = results.first().map(|t| t.uid);
    let tasks = AllTasks { results, limit: limit.saturating_sub(1), total, from, next };

    Ok(HttpResponse::Ok().json(tasks))
}

async fn get_task(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    task_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let task_uid_string = task_uid.into_inner();

    let task_uid: TaskId = match task_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(index_scheduler::Error::InvalidTaskUids { task_uid: task_uid_string }.into())
        }
    };

    let query = index_scheduler::Query { uids: Some(vec![task_uid]), ..Query::default() };
    let filters = index_scheduler.filters();
    let (tasks, _) = index_scheduler.get_tasks_from_authorized_indexes(query, filters)?;

    if let Some(task) = tasks.first() {
        let task_view = TaskView::from_task(task);
        Ok(HttpResponse::Ok().json(task_view))
    } else {
        Err(index_scheduler::Error::TaskNotFound(task_uid).into())
    }
}

pub enum DeserializeDateOption {
    Before,
    After,
}

pub fn deserialize_date(
    value: &str,
    option: DeserializeDateOption,
) -> std::result::Result<OffsetDateTime, InvalidTaskDateError> {
    // We can't parse using time's rfc3339 format, since then we won't know what part of the
    // datetime was not explicitly specified, and thus we won't be able to increment it to the
    // next step.
    if let Ok(datetime) = OffsetDateTime::parse(value, &Rfc3339) {
        // fully specified up to the second
        // we assume that the subseconds are 0 if not specified, and we don't increment to the next second
        Ok(datetime)
    } else if let Ok(datetime) = Date::parse(
        value,
        format_description!("[year repr:full base:calendar]-[month repr:numerical]-[day]"),
    ) {
        let datetime = datetime.with_time(Time::MIDNIGHT).assume_utc();
        // add one day since the time was not specified
        match option {
            DeserializeDateOption::Before => Ok(datetime),
            DeserializeDateOption::After => {
                let datetime = datetime.checked_add(Duration::days(1)).unwrap_or(datetime);
                Ok(datetime)
            }
        }
    } else {
        Err(InvalidTaskDateError(value.to_owned()))
    }
}

pub fn deserialize_date_after(
    value: OptionStarOr<String>,
) -> std::result::Result<OptionStarOr<OffsetDateTime>, InvalidTaskDateError> {
    value.try_map(|x| deserialize_date(&x, DeserializeDateOption::After))
}
pub fn deserialize_date_before(
    value: OptionStarOr<String>,
) -> std::result::Result<OptionStarOr<OffsetDateTime>, InvalidTaskDateError> {
    value.try_map(|x| deserialize_date(&x, DeserializeDateOption::Before))
}


