use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::IndexScheduler;
use auth::AuthController;
use shared_types::error::ResponseError;
use shared_types::keys::actions;
use prometheus::{Encoder, TextEncoder};

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::routes::create_all_stats;

pub fn configure(config: &mut web::ServiceConfig) {
    config.service(web::resource("").route(web::get().to(get_metrics)));
}

pub async fn get_metrics(
    index_scheduler: GuardedData<ActionPolicy<{ actions::METRICS_GET }>, Data<IndexScheduler>>,
    auth_controller: Data<AuthController>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_metrics()?;
    let auth_filters = index_scheduler.filters();
    if !auth_filters.all_indexes_authorized() {
        let mut error = ResponseError::from(AuthenticationError::InvalidToken);
        error
            .message
            .push_str(" The API key for the `/metrics` route must allow access to all indexes.");
        return Err(error);
    }

    let response = create_all_stats((*index_scheduler).clone(), auth_controller, auth_filters)?;

    crate::metrics::SEARCH_SERVER_DB_SIZE_BYTES.set(response.database_size as i64);
    crate::metrics::SEARCH_SERVER_USED_DB_SIZE_BYTES.set(response.used_database_size as i64);
    crate::metrics::SEARCH_SERVER_INDEX_COUNT.set(response.indexes.len() as i64);

    for (index, value) in response.indexes.iter() {
        crate::metrics::SEARCH_SERVER_INDEX_DOCS_COUNT
            .with_label_values(&[index])
            .set(value.number_of_documents as i64);
    }

    for (kind, value) in index_scheduler.get_stats()? {
        for (value, count) in value {
            crate::metrics::SEARCH_SERVER_NB_TASKS
                .with_label_values(&[&kind, &value])
                .set(count as i64);
        }
    }

    if let Some(last_update) = response.last_update {
        crate::metrics::SEARCH_SERVER_LAST_UPDATE.set(last_update.unix_timestamp());
    }
    crate::metrics::SEARCH_SERVER_IS_INDEXING.set(index_scheduler.is_task_processing()? as i64);

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder.encode(&prometheus::gather(), &mut buffer).expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");

    Ok(HttpResponse::Ok().insert_header(header::ContentType(mime::TEXT_PLAIN)).body(response))
}
