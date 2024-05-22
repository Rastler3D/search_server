use lazy_static::lazy_static;
use prometheus::{
    opts, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};

lazy_static! {
    pub static ref SEARCH_SERVER_HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        opts!("search_server_http_requests_total", "Search Server HTTP requests total"),
        &["method", "path", "status"]
    )
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_DEGRADED_SEARCH_REQUESTS: IntGauge = register_int_gauge!(opts!(
        "search_server_degraded_search_requests",
        "Search Server number of degraded search requests"
    ))
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_DB_SIZE_BYTES: IntGauge =
        register_int_gauge!(opts!("search_server_db_size_bytes", "Search Server DB Size In Bytes"))
            .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_USED_DB_SIZE_BYTES: IntGauge = register_int_gauge!(opts!(
        "search_server_used_db_size_bytes",
        "Search Server Used DB Size In Bytes"
    ))
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_INDEX_COUNT: IntGauge =
        register_int_gauge!(opts!("search_server_index_count", "Search Server Index Count"))
            .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_INDEX_DOCS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        opts!("search_server_index_docs_count", "Search Server Index Docs Count"),
        &["index"]
    )
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_HTTP_RESPONSE_TIME_SECONDS: HistogramVec = register_histogram_vec!(
        "search_server_http_response_time_seconds",
        "Search Server HTTP response times",
        &["method", "path"],
        vec![0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
    )
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_NB_TASKS: IntGaugeVec = register_int_gauge_vec!(
        opts!("search_server_nb_tasks", "Search Server Number of tasks"),
        &["kind", "value"]
    )
    .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_LAST_UPDATE: IntGauge =
        register_int_gauge!(opts!("search_server_last_update", "Search Server Last Update"))
            .expect("Can't create a metric");
    pub static ref SEARCH_SERVER_IS_INDEXING: IntGauge =
        register_int_gauge!(opts!("search_server_is_indexing", "Search Server Is Indexing"))
            .expect("Can't create a metric");
}
