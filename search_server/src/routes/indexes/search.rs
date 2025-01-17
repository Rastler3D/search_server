use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use index_scheduler::{IndexScheduler, RoFeatures};
use shared_types::deserr::query_params::Param;
use shared_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use shared_types::error::deserr_codes::*;
use shared_types::error::ResponseError;
use shared_types::index_uid::IndexUid;
use shared_types::search_engine;
use shared_types::serde_cs::vec::CS;
use serde_json::Value;
use tracing::debug;

use crate::analytics::{Analytics, SearchAggregator};
use crate::error::SearchSystemHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::metrics::SEARCH_SERVER_DEGRADED_SEARCH_REQUESTS;
use crate::search::{
    add_search_rules, perform_search, HybridQuery, MatchingStrategy, SearchKind, SearchQuery,
    SemanticRatio, DEFAULT_CROP_LENGTH, DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG,
    DEFAULT_HIGHLIGHT_PRE_TAG, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET, DEFAULT_SEMANTIC_RATIO,
};
use crate::search_queue::SearchQueue;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(search_with_url_query)))
            .route(web::post().to(SeqHandler(search_with_post))),
    );
}

#[derive(Debug, deserr::Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQueryGet {
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchQ>)]
    q: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchVector>)]
    vector: Option<CS<f32>>,
    #[deserr(default = Param(DEFAULT_SEARCH_OFFSET()), error = DeserrQueryParamError<InvalidSearchOffset>)]
    offset: Param<usize>,
    #[deserr(default = Param(DEFAULT_SEARCH_LIMIT()), error = DeserrQueryParamError<InvalidSearchLimit>)]
    limit: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidAnalyzer>)]
    pub analyzer: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchPage>)]
    page: Option<Param<usize>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchHitsPerPage>)]
    hits_per_page: Option<Param<usize>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToRetrieve>)]
    attributes_to_retrieve: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToCrop>)]
    attributes_to_crop: Option<CS<String>>,
    #[deserr(default = Param(DEFAULT_CROP_LENGTH()), error = DeserrQueryParamError<InvalidSearchCropLength>)]
    crop_length: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToHighlight>)]
    attributes_to_highlight: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFilter>)]
    filter: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSort>)]
    sort: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowQueryGraph>)]
    show_query_graph: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowMatchesPosition>)]
    show_matches_position: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScore>)]
    show_ranking_score: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchShowRankingScoreDetails>)]
    show_ranking_score_details: Param<bool>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchFacets>)]
    facets: Option<CS<String>>,
    #[deserr( default = DEFAULT_HIGHLIGHT_PRE_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPreTag>)]
    highlight_pre_tag: String,
    #[deserr( default = DEFAULT_HIGHLIGHT_POST_TAG(), error = DeserrQueryParamError<InvalidSearchHighlightPostTag>)]
    highlight_post_tag: String,
    #[deserr(default = DEFAULT_CROP_MARKER(), error = DeserrQueryParamError<InvalidSearchCropMarker>)]
    crop_marker: String,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchMatchingStrategy>)]
    matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchAttributesToSearchOn>)]
    pub attributes_to_search_on: Option<CS<String>>,
    #[deserr(default, error = DeserrQueryParamError<InvalidEmbedder>)]
    pub hybrid_embedder: Option<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidSearchSemanticRatio>)]
    pub hybrid_semantic_ratio: Option<SemanticRatioGet>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, deserr::Deserr)]
#[deserr(try_from(String) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatioGet(SemanticRatio);

impl std::convert::TryFrom<String> for SemanticRatioGet {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let f: f32 = s.parse().map_err(|_| InvalidSearchSemanticRatio)?;
        Ok(SemanticRatioGet(SemanticRatio::try_from(f)?))
    }
}

impl std::ops::Deref for SemanticRatioGet {
    type Target = SemanticRatio;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<SearchQueryGet> for SearchQuery {
    fn from(other: SearchQueryGet) -> Self {
        let filter = match other.filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        let hybrid = match (other.hybrid_embedder, other.hybrid_semantic_ratio) {
            (None, None) => None,
            (None, Some(semantic_ratio)) => {
                Some(HybridQuery { semantic_ratio: *semantic_ratio, embedder: None })
            }
            (Some(embedder), None) => Some(HybridQuery {
                semantic_ratio: DEFAULT_SEMANTIC_RATIO(),
                embedder: Some(embedder),
            }),
            (Some(embedder), Some(semantic_ratio)) => {
                Some(HybridQuery { semantic_ratio: *semantic_ratio, embedder: Some(embedder) })
            }
        };

        Self {
            q: other.q,
            vector: other.vector.map(CS::into_inner),
            offset: other.offset.0,
            limit: other.limit.0,
            page: other.page.as_deref().copied(),
            hits_per_page: other.hits_per_page.as_deref().copied(),
            attributes_to_retrieve: other.attributes_to_retrieve.map(|o| o.into_iter().collect()),
            attributes_to_crop: other.attributes_to_crop.map(|o| o.into_iter().collect()),
            crop_length: other.crop_length.0,
            attributes_to_highlight: other.attributes_to_highlight.map(|o| o.into_iter().collect()),
            filter,
            sort: other.sort.map(|attr| fix_sort_query_parameters(&attr)),
            show_matches_position: other.show_matches_position.0,
            show_ranking_score: other.show_ranking_score.0,
            show_ranking_score_details: other.show_ranking_score_details.0,
            facets: other.facets.map(|o| o.into_iter().collect()),
            highlight_pre_tag: other.highlight_pre_tag,
            highlight_post_tag: other.highlight_post_tag,
            crop_marker: other.crop_marker,
            matching_strategy: other.matching_strategy,
            attributes_to_search_on: other.attributes_to_search_on.map(|o| o.into_iter().collect()),
            hybrid,
            analyzer: other.analyzer,
            show_query_graph: other.show_query_graph.0,
        }
    }
}

// TODO: TAMO: split on :asc, and :desc, instead of doing some weird things

/// Transform the sort query parameter into something that matches the post expected format.
fn fix_sort_query_parameters(sort_query: &str) -> Vec<String> {
    let mut sort_parameters = Vec::new();
    let mut merge = false;
    for current_sort in sort_query.trim_matches('"').split(',').map(|s| s.trim()) {
        if current_sort.starts_with("_geoPoint(") {
            sort_parameters.push(current_sort.to_string());
            merge = true;
        } else if merge && !sort_parameters.is_empty() {
            let s = sort_parameters.last_mut().unwrap();
            s.push(',');
            s.push_str(current_sort);
            if current_sort.ends_with("):desc") || current_sort.ends_with("):asc") {
                merge = false;
            }
        } else {
            sort_parameters.push(current_sort.to_string());
            merge = false;
        }
    }
    sort_parameters
}

pub async fn search_with_url_query(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<SearchQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Search get");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let mut query: SearchQuery = params.into_inner().into();

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query, search_rules);
    }

    let mut aggregate = SearchAggregator::from_query(&query, &index_uid, &req);

    let index = index_scheduler.index(&index_uid)?;
    let features = index_scheduler.features();

    let search_kind = search_kind(&query, index_scheduler.get_ref(), &index, features)?;

    let _permit = search_queue.try_get_search_permit().await?;
    let search_result =
        tokio::task::spawn_blocking(move || perform_search(&index, query, search_kind)).await?;
    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.get_search(aggregate);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Search get");
    Ok(HttpResponse::Ok().json(search_result))
}

pub async fn search_with_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    index_uid: web::Path<String>,
    params: AwebJson<SearchQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let mut query = params.into_inner();
    debug!(parameters = ?query, "Search post");

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query, search_rules);
    }

    let mut aggregate = SearchAggregator::from_query(&query, &index_uid, &req);

    let index = index_scheduler.index(&index_uid)?;

    let features = index_scheduler.features();

    let search_kind = search_kind(&query, index_scheduler.get_ref(), &index, features)?;

    let _permit = search_queue.try_get_search_permit().await?;
    let search_result =
        tokio::task::spawn_blocking(move || perform_search(&index, query, search_kind)).await?;
    if let Ok(ref search_result) = search_result {
        aggregate.succeed(search_result);
    }
    analytics.post_search(aggregate);

    let search_result = search_result?;

    debug!(returns = ?search_result, "Search post");
    Ok(HttpResponse::Ok().json(search_result))
}

pub fn search_kind(
    query: &SearchQuery,
    index_scheduler: &IndexScheduler,
    index: &search_engine::Index,
    features: RoFeatures,
) -> Result<SearchKind, ResponseError> {
    if query.vector.is_some() {
        features.check_vector("Passing `vector` as a query parameter")?;
    }

    if query.hybrid.is_some() {
        features.check_vector("Passing `hybrid` as a query parameter")?;
    }

    // regardless of anything, always do a keyword search when we don't have a vector and the query is whitespace or missing
    if query.vector.is_none() {
        match &query.q {
            Some(q) if q.trim().is_empty() => return Ok(SearchKind::KeywordOnly),
            None => return Ok(SearchKind::KeywordOnly),
            _ => {}
        }
    }

    match &query.hybrid {
        Some(HybridQuery { semantic_ratio, embedder }) if **semantic_ratio == 1.0 => {
            Ok(SearchKind::semantic(
                index_scheduler,
                index,
                embedder.as_deref(),
                query.vector.as_ref().map(Vec::len),
            )?)
        }
        Some(HybridQuery { semantic_ratio, embedder: _ }) if **semantic_ratio == 0.0 => {
            Ok(SearchKind::KeywordOnly)
        }
        Some(HybridQuery { semantic_ratio, embedder }) => Ok(SearchKind::hybrid(
            index_scheduler,
            index,
            embedder.as_deref(),
            **semantic_ratio,
            query.vector.as_ref().map(Vec::len),
        )?),
        None => match (query.q.as_deref(), query.vector.as_deref()) {
            (_query, None) => Ok(SearchKind::KeywordOnly),
            (None, Some(_vector)) => Ok(SearchKind::semantic(
                index_scheduler,
                index,
                None,
                query.vector.as_ref().map(Vec::len),
            )?),
            (Some(_), Some(_)) => Err(SearchSystemHttpError::MissingSearchHybrid.into()),
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_fix_sort_query_parameters() {
        let sort = fix_sort_query_parameters("_geoPoint(12, 13):asc");
        assert_eq!(sort, vec!["_geoPoint(12,13):asc".to_string()]);
        let sort = fix_sort_query_parameters("doggo:asc,_geoPoint(12.45,13.56):desc");
        assert_eq!(sort, vec!["doggo:asc".to_string(), "_geoPoint(12.45,13.56):desc".to_string(),]);
        let sort = fix_sort_query_parameters(
            "doggo:asc , _geoPoint(12.45, 13.56, 2590352):desc , catto:desc",
        );
        assert_eq!(
            sort,
            vec![
                "doggo:asc".to_string(),
                "_geoPoint(12.45,13.56,2590352):desc".to_string(),
                "catto:desc".to_string(),
            ]
        );
        let sort = fix_sort_query_parameters("doggo:asc , _geoPoint(1, 2), catto:desc");
        // This is ugly but eh, I don't want to write a full parser just for this unused route
        assert_eq!(sort, vec!["doggo:asc".to_string(), "_geoPoint(1,2),catto:desc".to_string(),]);
    }
}
