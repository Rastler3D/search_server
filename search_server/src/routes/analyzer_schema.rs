use std::collections::BTreeMap;
use actix_web::web;
use actix_web::web::Json;
use schemars::schema::{RootSchema};
use schemars::schema_for;
use analyzer::analyzer::BoxAnalyzer;
use shared_types::error::{ ResponseError};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(analyzer_schema)));
}


pub async fn analyzer_schema(
) -> Result<Json<RootSchema>, ResponseError> {

    Ok(Json(schema_for!(BTreeMap<String, BoxAnalyzer>)))
}


