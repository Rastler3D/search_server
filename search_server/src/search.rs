use core::fmt;
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deserr::Deserr;
use either::Either;
use indexmap::IndexMap;
use auth::IndexSearchRules;
use shared_types::deserr::DeserrJsonError;
use shared_types::error::deserr_codes::*;
use shared_types::error::ResponseError;
use shared_types::heed::RoTxn;
use shared_types::index_uid::IndexUid;
use shared_types::search_engine::score_details::{ScoreDetails};
use shared_types::search_engine::vector::Embedder;
use shared_types::search_engine::{Error, FacetValueHit, OrderBy, SearchForFacetValues, TimeBudget, UserError};
use shared_types::settings::DEFAULT_PAGINATION_MAX_TOTAL_HITS;
use shared_types::{search_engine, Document};
use search_engine::tokenizer::TokenizerBuilder;
use search_engine::{
    AscDesc, FieldId, FieldsIdsMap, Filter, FormatOptions, Index, MatchBounds, MatcherBuilder,
    SortError, TermsMatchingStrategy, DEFAULT_VALUES_PER_FACET,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::SearchSystemHttpError;

type MatchesPosition = BTreeMap<String, Vec<MatchBounds>>;

pub const DEFAULT_SEARCH_OFFSET: fn() -> usize = || 0;
pub const DEFAULT_SEARCH_LIMIT: fn() -> usize = || 20;
pub const DEFAULT_CROP_LENGTH: fn() -> usize = || 10;
pub const DEFAULT_CROP_MARKER: fn() -> String = || "…".to_string();
pub const DEFAULT_HIGHLIGHT_PRE_TAG: fn() -> String = || "<em>".to_string();
pub const DEFAULT_HIGHLIGHT_POST_TAG: fn() -> String = || "</em>".to_string();
pub const DEFAULT_SEMANTIC_RATIO: fn() -> SemanticRatio = || SemanticRatio(0.5);

impl SearchQuery {
    pub fn is_finite_pagination(&self) -> bool {
        self.page.or(self.hits_per_page).is_some()
    }
}

#[derive(Clone, Default, PartialEq, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchVector>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default, error = DeserrJsonError<InvalidAnalyzer>)]
    pub analyzer: Option<String>,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowQueryGraph>, default)]
    pub show_query_graph: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>, default)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
}

// Since this structure is logged A LOT we're going to reduce the number of things it logs to the bare minimum.
// - Only what IS used, we know everything else is set to None so there is no need to print it
// - Re-order the most important field to debug first
impl fmt::Debug for SearchQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            q,
            vector,
            hybrid,
            analyzer,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            attributes_to_highlight,
            show_query_graph,
            show_matches_position,
            show_ranking_score,
            show_ranking_score_details,
            filter,
            sort,
            facets,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,

        } = self;

        let mut debug = f.debug_struct("SearchQuery");

        // First, everything related to the number of documents to retrieve
        debug.field("limit", &limit).field("offset", &offset);
        if let Some(page) = page {
            debug.field("page", &page);
        }
        if let Some(hits_per_page) = hits_per_page {
            debug.field("hits_per_page", &hits_per_page);
        }

        // Then, everything related to the queries
        if let Some(q) = q {
            debug.field("q", &q);
        }
        if let Some(v) = vector {
            if v.len() < 10 {
                debug.field("vector", &v);
            } else {
                debug.field(
                    "vector",
                    &format!("[{}, {}, {}, ... {} dimensions]", v[0], v[1], v[2], v.len()),
                );
            }
        }
        if let Some(hybrid) = hybrid {
            debug.field("hybrid", &hybrid);
        }
        if let Some(analyzer) = analyzer {
            debug.field("analyzer", &analyzer);
        }
        if let Some(attributes_to_search_on) = attributes_to_search_on {
            debug.field("attributes_to_search_on", &attributes_to_search_on);
        }
        if let Some(filter) = filter {
            debug.field("filter", &filter);
        }
        if let Some(sort) = sort {
            debug.field("sort", &sort);
        }
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        debug.field("matching_strategy", &matching_strategy);

        // Then everything related to the formatting
        debug.field("crop_length", &crop_length);
        if *show_query_graph {
            debug.field("show_query_graph", show_query_graph);
        }
        if *show_matches_position {
            debug.field("show_matches_position", show_matches_position);
        }
        if *show_ranking_score {
            debug.field("show_ranking_score", show_ranking_score);
        }
        if *show_ranking_score_details {
            debug.field("self.show_ranking_score_details", show_ranking_score_details);
        }
        debug.field("crop_length", &crop_length);
        if let Some(facets) = facets {
            debug.field("facets", &facets);
        }
        if let Some(attributes_to_retrieve) = attributes_to_retrieve {
            debug.field("attributes_to_retrieve", &attributes_to_retrieve);
        }
        if let Some(attributes_to_crop) = attributes_to_crop {
            debug.field("attributes_to_crop", &attributes_to_crop);
        }
        if let Some(attributes_to_highlight) = attributes_to_highlight {
            debug.field("attributes_to_highlight", &attributes_to_highlight);
        }
        debug.field("highlight_pre_tag", &highlight_pre_tag);
        debug.field("highlight_post_tag", &highlight_post_tag);
        debug.field("crop_marker", &crop_marker);

        debug.finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserr)]
#[deserr(error = DeserrJsonError<InvalidHybridQuery>, rename_all = camelCase, deny_unknown_fields)]
pub struct HybridQuery {
    #[deserr(default, error = DeserrJsonError<InvalidSearchSemanticRatio>, default)]
    pub semantic_ratio: SemanticRatio,
    #[deserr(default, error = DeserrJsonError<InvalidEmbedder>, default)]
    pub embedder: Option<String>,
}

pub enum SearchKind {
    KeywordOnly,
    SemanticOnly { embedder_name: String, embedder: Arc<Embedder> },
    Hybrid { embedder_name: String, embedder: Arc<Embedder>, semantic_ratio: f32 },
}
impl SearchKind {
    pub(crate) fn semantic(
        index_scheduler: &index_scheduler::IndexScheduler,
        index: &Index,
        embedder_name: Option<&str>,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder) =
            Self::embedder(index_scheduler, index, embedder_name, vector_len)?;
        Ok(Self::SemanticOnly { embedder_name, embedder })
    }

    pub(crate) fn hybrid(
        index_scheduler: &index_scheduler::IndexScheduler,
        index: &Index,
        embedder_name: Option<&str>,
        semantic_ratio: f32,
        vector_len: Option<usize>,
    ) -> Result<Self, ResponseError> {
        let (embedder_name, embedder) =
            Self::embedder(index_scheduler, index, embedder_name, vector_len)?;
        Ok(Self::Hybrid { embedder_name, embedder, semantic_ratio })
    }

    fn embedder(
        index_scheduler: &index_scheduler::IndexScheduler,
        index: &Index,
        embedder_name: Option<&str>,
        vector_len: Option<usize>,
    ) -> Result<(String, Arc<Embedder>), ResponseError> {
        let embedder_configs = index.embedding_configs(&index.read_txn()?)?;
        let embedders = index_scheduler.embedders(embedder_configs)?;

        let embedder_name = embedder_name.unwrap_or_else(|| embedders.get_default_embedder_name());

        let embedder = embedders.get(embedder_name);

        let embedder = embedder
            .ok_or(search_engine::UserError::InvalidEmbedder(embedder_name.to_owned()))
            .map_err(search_engine::Error::from)?
            .0;

        if let Some(vector_len) = vector_len {
            if vector_len != embedder.dimensions() {
                return Err(shared_types::search_engine::Error::UserError(
                    shared_types::search_engine::UserError::InvalidVectorDimensions {
                        expected: embedder.dimensions(),
                        found: vector_len,
                    },
                )
                .into());
            }
        }

        Ok((embedder_name.to_owned(), embedder))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr)]
#[deserr(try_from(f32) = TryFrom::try_from -> InvalidSearchSemanticRatio)]
pub struct SemanticRatio(f32);

impl Default for SemanticRatio {
    fn default() -> Self {
        DEFAULT_SEMANTIC_RATIO()
    }
}

impl std::convert::TryFrom<f32> for SemanticRatio {
    type Error = InvalidSearchSemanticRatio;

    fn try_from(f: f32) -> Result<Self, Self::Error> {
        // the suggested "fix" is: `!(0.0..=1.0).contains(&f)`` which is allegedly less readable
        #[allow(clippy::manual_range_contains)]
        if f > 1.0 || f < 0.0 {
            Err(InvalidSearchSemanticRatio)
        } else {
            Ok(SemanticRatio(f))
        }
    }
}

impl std::ops::Deref for SemanticRatio {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


/// A `SearchQuery` + an index UID.
// This struct contains the fields of `SearchQuery` inline.
// This is because neither deserr nor serde support `flatten` when using `deny_unknown_fields.
// The `From<SearchQueryWithIndex>` implementation ensures both structs remain up to date.
#[derive(Debug, Clone, PartialEq, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQueryWithIndex {
    #[deserr(error = DeserrJsonError<InvalidIndexUid>, missing_field_error = DeserrJsonError::missing_index_uid)]
    pub index_uid: IndexUid,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub vector: Option<Vec<f32>>,
    #[deserr(default, error = DeserrJsonError<InvalidHybridQuery>)]
    pub hybrid: Option<HybridQuery>,
    #[deserr(default, error = DeserrJsonError<InvalidAnalyzer>)]
    pub analyzer: Option<String>,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowQueryGraph>, default)]
    pub show_query_graph: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScore>, default)]
    pub show_ranking_score: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowRankingScoreDetails>, default)]
    pub show_ranking_score_details: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToSearchOn>, default)]
    pub attributes_to_search_on: Option<Vec<String>>,
}

impl SearchQueryWithIndex {
    pub fn into_index_query(self) -> (IndexUid, SearchQuery) {
        let SearchQueryWithIndex {
            index_uid,
            q,
            vector,
            offset,
            limit,
            page,
            hits_per_page,
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length,
            attributes_to_highlight,
            show_query_graph,
            show_ranking_score,
            show_ranking_score_details,
            show_matches_position,
            filter,
            sort,
            facets,
            highlight_pre_tag,
            highlight_post_tag,
            crop_marker,
            matching_strategy,
            attributes_to_search_on,
            hybrid,
            analyzer
        } = self;
        (
            index_uid,
            SearchQuery {
                q,
                vector,
                offset,
                limit,
                page,
                hits_per_page,
                attributes_to_retrieve,
                attributes_to_crop,
                crop_length,
                attributes_to_highlight,
                show_ranking_score,
                show_ranking_score_details,
                show_matches_position,
                filter,
                sort,
                facets,
                highlight_pre_tag,
                highlight_post_tag,
                crop_marker,
                matching_strategy,
                attributes_to_search_on,
                hybrid,
                analyzer,
                show_query_graph,
            },
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Deserr, Serialize, Deserialize)]
#[deserr(rename_all = camelCase)]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    Last,
    /// All query words are mandatory
    All,
}

impl Default for MatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserr)]
#[deserr(rename_all = camelCase)]
pub enum FacetValuesSort {
    /// Facet values are sorted in alphabetical order, ascending from A to Z.
    #[default]
    Alpha,
    /// Facet values are sorted by decreasing count.
    /// The count is the number of records containing this facet value in the results of the query.
    Count,
}

impl From<FacetValuesSort> for OrderBy {
    fn from(val: FacetValuesSort) -> Self {
        match val {
            FacetValuesSort::Alpha => OrderBy::Lexicographic,
            FacetValuesSort::Count => OrderBy::Count,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: Document,
    #[serde(rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    pub formatted: Document,
    #[serde(rename = "_matchesPosition", skip_serializing_if = "Option::is_none")]
    pub matches_position: Option<MatchesPosition>,
    #[serde(rename = "_rankingScore", skip_serializing_if = "Option::is_none")]
    pub ranking_score: Option<f64>,
    #[serde(rename = "_rankingScoreDetails", skip_serializing_if = "Option::is_none")]
    pub ranking_score_details: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub query: String,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distribution: Option<BTreeMap<String, IndexMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_hit_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_graph: Option<String>,
}

impl fmt::Debug for SearchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let SearchResult {
            hits,
            query,
            processing_time_ms,
            hits_info,
            facet_distribution,
            facet_stats,
            semantic_hit_count,
            query_graph
        } = self;

        let mut debug = f.debug_struct("SearchResult");
        // The most important thing when looking at a search result is the time it took to process
        debug.field("processing_time_ms", &processing_time_ms);
        debug.field("hits", &format!("[{} hits returned]", hits.len()));
        debug.field("query", &query);
        debug.field("hits_info", &hits_info);
        if let Some(facet_distribution) = facet_distribution {
            debug.field("facet_distribution", &facet_distribution);
        }
        if let Some(facet_stats) = facet_stats {
            debug.field("facet_stats", &facet_stats);
        }
        if let Some(semantic_hit_count) = semantic_hit_count {
            debug.field("semantic_hit_count", &semantic_hit_count);
        }
        if let Some(query_graph) = query_graph {
            debug.field("query_graph", &query_graph);
        }

        debug.finish()
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchResultWithIndex {
    pub index_uid: String,
    #[serde(flatten)]
    pub result: SearchResult,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum HitsInfo {
    #[serde(rename_all = "camelCase")]
    Pagination { hits_per_page: usize, page: usize, total_pages: usize, total_hits: usize, top_score: Option<f64> },
    #[serde(rename_all = "camelCase")]
    OffsetLimit { limit: usize, offset: usize, total_hits: usize, top_score: Option<f64> },
}

impl HitsInfo {
    pub fn total_hits(&self) -> usize{
        match self {
            HitsInfo::Pagination { total_hits, .. } | HitsInfo::OffsetLimit { total_hits, .. } => {
                *total_hits
            }
        }
    }

    pub fn top_score(&self) -> Option<f64>{
        match self {
            HitsInfo::Pagination { top_score, .. } | HitsInfo::OffsetLimit { top_score, .. } => {
                top_score.clone()
            }
        }
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct FacetStats {
    pub min: f64,
    pub max: f64,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FacetSearchResult {
    pub facet_hits: Vec<FacetValueHit>,
    pub facet_query: Option<String>,
    pub processing_time_ms: u128,
}

/// Incorporate search rules in search query
pub fn add_search_rules(query: &mut SearchQuery, rules: IndexSearchRules) {
    query.filter = match (query.filter.take(), rules.filter) {
        (None, rules_filter) => rules_filter,
        (filter, None) => filter,
        (Some(filter), Some(rules_filter)) => {
            let filter = match filter {
                Value::Array(filter) => filter,
                filter => vec![filter],
            };
            let rules_filter = match rules_filter {
                Value::Array(rules_filter) => rules_filter,
                rules_filter => vec![rules_filter],
            };

            Some(Value::Array([filter, rules_filter].concat()))
        }
    }
}

fn prepare_search<'t>(
    index: &'t Index,
    rtxn: &'t RoTxn,
    query: &'t SearchQuery,
    search_kind: &SearchKind,
) -> Result<(search_engine::Search<'t>, bool, usize, usize), SearchSystemHttpError> {
    let mut search = index.search(rtxn);

    match search_kind {
        SearchKind::KeywordOnly => {
            if let Some(q) = &query.q {
                search.query(q);
            }
        }
        SearchKind::SemanticOnly { embedder_name, embedder } => {
            let vector = match query.vector.clone() {
                Some(vector) => vector,
                None => embedder
                    .embed_one(query.q.clone().unwrap())
                    .map_err(search_engine::vector::Error::from)
                    .map_err(search_engine::Error::from)?,
            };

            search.semantic(embedder_name.clone(), embedder.clone(), Some(vector));
        }
        SearchKind::Hybrid { embedder_name, embedder, semantic_ratio: _ } => {
            if let Some(q) = &query.q {
                search.query(q);
            }
            // will be embedded in hybrid search if necessary
            search.semantic(embedder_name.clone(), embedder.clone(), query.vector.clone());
        }
    }
    if let Some(ref analyzer) = query.analyzer {
        search.analyzer(analyzer.clone());
    }


    if let Some(ref searchable) = query.attributes_to_search_on {
        search.searchable_attributes(searchable);
    }

    let is_finite_pagination = query.is_finite_pagination();
    search.terms_matching_strategy(query.matching_strategy.into());
    search.output_query_graph(query.show_query_graph);

    let max_total_hits = index
        .pagination_max_total_hits(rtxn)
        .map_err(search_engine::Error::from)?
        .map(|x| x as usize)
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);


    // compute the offset on the limit depending on the pagination mode.
    let (offset, limit) = if is_finite_pagination {
        let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        let page = query.page.unwrap_or(1);

        // page 0 gives a limit of 0 forcing Meilisearch to return no document.
        page.checked_sub(1).map_or((0, 0), |p| (limit * p, limit))
    } else {
        (query.offset, query.limit)
    };

    // Make sure that a user can't get more documents than the hard limit,
    // we align that on the offset too.
    let offset = min(offset, max_total_hits);
    let limit = min(limit, max_total_hits.saturating_sub(offset));

    search.offset(offset as u64);
    search.limit(limit as u64);

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter)? {
            search.filter(facets);
        }
    }

    if let Some(ref sort) = query.sort {
        let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
            Ok(sorts) => sorts,
            Err(asc_desc_error) => {
                return Err(search_engine::Error::from(SortError::from(asc_desc_error)).into())
            }
        };

        search.sort_criteria(sort);
    }

    Ok((search, is_finite_pagination, max_total_hits, offset))
}

pub fn perform_search(
    index: &Index,
    query: SearchQuery,
    search_kind: SearchKind,
) -> Result<SearchResult, SearchSystemHttpError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;


    let (search, is_finite_pagination, max_total_hits, offset) =
        prepare_search(index, &rtxn, &query, &search_kind)?;

    let (
        search_engine::SearchResult {
            documents_ids,
            matching_words,
            candidates,
            document_scores,
            query_graph
        },
        semantic_hit_count,
    ) = match &search_kind {
        SearchKind::KeywordOnly => (search.execute()?, None),
        SearchKind::SemanticOnly { .. } => {
            let results = search.execute()?;
            let semantic_hit_count = results.document_scores.len() as u32;
            (results, Some(semantic_hit_count))
        }
        SearchKind::Hybrid { semantic_ratio, .. } => search.execute_hybrid(*semantic_ratio)?,
    };

    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();



    let fids = |attrs: &BTreeSet<String>| {
        let mut ids = BTreeSet::new();
        for attr in attrs {
            if attr == "*" {
                ids = fields_ids_map.ids().collect();
                break;
            }

            if let Some(id) = fields_ids_map.id(attr) {
                ids.insert(id);
            }
        }
        ids
    };

    // The attributes to retrieve are the ones explicitly marked as to retrieve (all by default),
    // but these attributes must be also be present
    // - in the fields_ids_map
    let to_retrieve_ids = query
        .attributes_to_retrieve
        .as_ref()
        .map(fids)
        .unwrap_or_else(|| fields_ids_map.ids().collect());

    let attr_to_highlight = query.attributes_to_highlight.unwrap_or_default();

    let attr_to_crop = query.attributes_to_crop.unwrap_or_default();

    // Attributes in `formatted_options` correspond to the attributes that will be in `_formatted`
    // These attributes are:
    // - the attributes asked to be highlighted or cropped (with `attributesToCrop` or `attributesToHighlight`)
    // - the attributes asked to be retrieved: these attributes will not be highlighted/cropped
    // But these attributes must be also present in displayed attributes
    let formatted_options = compute_formatted_options(
        &attr_to_highlight,
        &attr_to_crop,
        query.crop_length,
        &to_retrieve_ids,
        &fields_ids_map,
    );

    let analyzer = index.analyzer(&rtxn, &query.analyzer)?;


    let mut formatter_builder = MatcherBuilder::new(matching_words, analyzer);
    formatter_builder.crop_marker(query.crop_marker);
    formatter_builder.highlight_prefix(query.highlight_pre_tag);
    formatter_builder.highlight_suffix(query.highlight_post_tag);

    let mut documents = Vec::new();
    let documents_iter = index.documents(&rtxn, documents_ids)?;
    let top_score = document_scores.first().map(|score| ScoreDetails::global_score(score.iter()));
    for ((_id, obkv), score) in documents_iter.into_iter().zip(document_scores.into_iter()) {
        // First generate a document with all the displayed fields
        let displayed_document = make_document(&fields_ids_map, obkv)?;

        // select the attributes to retrieve
        let attributes_to_retrieve = to_retrieve_ids
            .iter()
            .map(|&fid| fields_ids_map.name(fid).expect("Missing field name"));
        let mut document =
            permissive_json_pointer::select_values(&displayed_document, attributes_to_retrieve);

        let (matches_position, formatted) = format_fields(
            &displayed_document,
            &fields_ids_map,
            &formatter_builder,
            &formatted_options,
            query.show_matches_position,
        )?;


        let ranking_score =
            query.show_ranking_score.then(|| ScoreDetails::global_score(score.iter()));
        let ranking_score_details =
            query.show_ranking_score_details.then(|| ScoreDetails::to_json_map(score.iter()));

        let hit = SearchHit {
            document,
            formatted,
            matches_position,
            ranking_score_details,
            ranking_score,
        };
        documents.push(hit);
    }

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = if is_finite_pagination {
        let hits_per_page = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        // If hit_per_page is 0, then pages can't be computed and so we respond 0.
        let total_pages = (number_of_hits + hits_per_page.saturating_sub(1))
            .checked_div(hits_per_page)
            .unwrap_or(0);
        HitsInfo::Pagination {
            hits_per_page,
            page: query.page.unwrap_or(1),
            total_pages,
            total_hits: number_of_hits,
            top_score
        }
    } else {
        HitsInfo::OffsetLimit { limit: query.limit, offset, total_hits: number_of_hits, top_score}
    };

    let (facet_distribution, facet_stats) = match query.facets {
        Some(ref fields) => {
            let mut facet_distribution = index.facets_distribution(&rtxn);

            let max_values_by_facet = index
                .max_values_per_facet(&rtxn)
                .map_err(search_engine::Error::from)?
                .map(|x| x as usize)
                .unwrap_or(DEFAULT_VALUES_PER_FACET);
            facet_distribution.max_values_per_facet(max_values_by_facet);

            let sort_facet_values_by =
                index.sort_facet_values_by(&rtxn).map_err(search_engine::Error::from)?;

            if fields.iter().all(|f| f != "*") {
                let fields: Vec<_> =
                    fields.iter().map(|n| (n, sort_facet_values_by.get(n))).collect();
                facet_distribution.facets(fields);
            }

            let distribution = facet_distribution
                .candidates(candidates)
                .default_order_by(sort_facet_values_by.get("*"))
                .execute()?;
            let stats = facet_distribution.compute_stats()?;
            (Some(distribution), Some(stats))
        }
        None => (None, None),
    };

    let facet_stats = facet_stats.map(|stats| {
        stats.into_iter().map(|(k, (min, max))| (k, FacetStats { min, max })).collect()
    });

    let result = SearchResult {
        hits: documents,
        hits_info,
        query: query.q.unwrap_or_default(),
        processing_time_ms: before_search.elapsed().as_millis(),
        facet_distribution,
        facet_stats,
        query_graph,
        semantic_hit_count,
    };
    Ok(result)
}

pub fn perform_facet_search(
    index: &Index,
    search_query: SearchQuery,
    facet_query: Option<String>,
    facet_name: String,
    search_kind: SearchKind,
) -> Result<FacetSearchResult, SearchSystemHttpError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;

    let (search, _, _, _) = prepare_search(index, &rtxn, &search_query, &search_kind)?;
    let mut facet_search = SearchForFacetValues::new(
        facet_name,
        search,
        matches!(search_kind, SearchKind::Hybrid { .. }),
    );
    if let Some(facet_query) = &facet_query {
        facet_search.query(facet_query);
    }
    if let Some(max_facets) = index.max_values_per_facet(&rtxn)? {
        facet_search.max_values(max_facets as usize);
    }

    Ok(FacetSearchResult {
        facet_hits: facet_search.execute()?,
        facet_query,
        processing_time_ms: before_search.elapsed().as_millis(),
    })
}

fn compute_formatted_options(
    attr_to_highlight: &HashSet<String>,
    attr_to_crop: &[String],
    query_crop_length: usize,
    to_retrieve_ids: &BTreeSet<FieldId>,
    fields_ids_map: &FieldsIdsMap,
) -> BTreeMap<FieldId, FormatOptions> {
    let mut formatted_options = BTreeMap::new();

    add_highlight_to_formatted_options(
        &mut formatted_options,
        attr_to_highlight,
        fields_ids_map,
    );

    add_crop_to_formatted_options(
        &mut formatted_options,
        attr_to_crop,
        query_crop_length,
        fields_ids_map,
    );

    // Should not return `_formatted` if no valid attributes to highlight/crop
    if !formatted_options.is_empty() {
        add_non_formatted_ids_to_formatted_options(&mut formatted_options, to_retrieve_ids);
    }

    formatted_options
}

fn add_highlight_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_highlight: &HashSet<String>,
    fields_ids_map: &FieldsIdsMap,
) {
    for attr in attr_to_highlight {
        let new_format = FormatOptions { highlight: true, crop: None };

        if attr == "*" {
            for id in fields_ids_map.ids() {
                formatted_options.insert(id, new_format);
            }
            break;
        }

        if let Some(id) = fields_ids_map.id(attr) {
            formatted_options.insert(id, new_format);
        }
    }
}

fn add_crop_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_crop: &[String],
    crop_length: usize,
    fields_ids_map: &FieldsIdsMap,
) {
    for attr in attr_to_crop {
        let mut split = attr.rsplitn(2, ':');
        let (attr_name, attr_len) = match split.next().zip(split.next()) {
            Some((len, name)) => {
                let crop_len = len.parse::<usize>().unwrap_or(crop_length);
                (name, crop_len)
            }
            None => (attr.as_str(), crop_length),
        };

        if attr_name == "*" {
            for id in fields_ids_map.ids() {
                formatted_options
                    .entry(id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }

        if let Some(id) = fields_ids_map.id(attr_name) {
            formatted_options
                .entry(id)
                .and_modify(|f| f.crop = Some(attr_len))
                .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
        }
    }
}

fn add_non_formatted_ids_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    to_retrieve_ids: &BTreeSet<FieldId>,
) {
    for id in to_retrieve_ids {
        formatted_options.entry(*id).or_insert(FormatOptions { highlight: false, crop: None });
    }
}

fn make_document(
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
) -> Result<Document, SearchSystemHttpError> {
    let mut document = serde_json::Map::new();

    // recreate the original json
    for (key, value) in obkv.iter() {
        let value = serde_json::from_slice(value)?;
        let key = field_ids_map.name(key).expect("Missing field name").to_string();

        document.insert(key, value);
    }

    // select the attributes to retrieve
    let displayed_attributes = field_ids_map.names();

    let document = permissive_json_pointer::select_values(&document, displayed_attributes);
    Ok(document)
}

fn format_fields<'a>(
    document: &Document,
    field_ids_map: &FieldsIdsMap,
    builder: &'a MatcherBuilder,
    formatted_options: &BTreeMap<FieldId, FormatOptions>,
    compute_matches: bool,
) -> Result<(Option<MatchesPosition>, Document), SearchSystemHttpError> {
    let mut matches_position = compute_matches.then(BTreeMap::new);
    let mut document = document.clone();

    // reduce the formatted option list to the attributes that should be formatted,
    // instead of all the attributes to display.
    let formatting_fields_options: Vec<_> = formatted_options
        .iter()
        .filter(|(_, option)| option.should_format())
        .map(|(fid, option)| (field_ids_map.name(*fid).unwrap(), option))
        .collect();

    // select the attributes to retrieve
    let displayable_names = field_ids_map.names();
    permissive_json_pointer::map_leaf_values(&mut document, displayable_names, |key, value| {
        // To get the formatting option of each key we need to see all the rules that applies
        // to the value and merge them together. eg. If a user said he wanted to highlight `doggo`
        // and crop `doggo.name`. `doggo.name` needs to be highlighted + cropped while `doggo.age` is only
        // highlighted.
        // Warn: The time to compute the format list scales with the number of fields to format;
        // cumulated with map_leaf_values that iterates over all the nested fields, it gives a quadratic complexity:
        // d*f where d is the total number of fields to display and f is the total number of fields to format.
        let format = formatting_fields_options
            .iter()
            .filter(|(name, _option)| {
                search_engine::is_faceted_by(name, key) || search_engine::is_faceted_by(key, name)
            })
            .map(|(_, option)| **option)
            .reduce(|acc, option| acc.merge(option));
        let mut infos = Vec::new();

        *value = format_value(std::mem::take(value), builder, format, &mut infos, compute_matches);

        if let Some(matches) = matches_position.as_mut() {
            if !infos.is_empty() {
                matches.insert(key.to_owned(), infos);
            }
        }
    });

    let selectors = formatted_options
        .keys()
        // This unwrap must be safe since we got the ids from the fields_ids_map just
        // before.
        .map(|&fid| field_ids_map.name(fid).unwrap());
    let document = permissive_json_pointer::select_values(&document, selectors);

    Ok((matches_position, document))
}

fn format_value(
    value: Value,
    builder: &MatcherBuilder,
    format_options: Option<FormatOptions>,
    infos: &mut Vec<MatchBounds>,
    compute_matches: bool,
) -> Value {
    match value {
        Value::String(old_string) => {
            let mut matcher = builder.build(&old_string);
            if compute_matches {
                let matches = matcher.matches();
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(old_string),
            }
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|v| {
                    format_value(
                        v,
                        builder,
                        format_options.map(|format_options| FormatOptions {
                            highlight: format_options.highlight,
                            crop: None,
                        }),
                        infos,
                        compute_matches,
                    )
                })
                .collect(),
        ),
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        format_value(
                            v,
                            builder,
                            format_options.map(|format_options| FormatOptions {
                                highlight: format_options.highlight,
                                crop: None,
                            }),
                            infos,
                            compute_matches,
                        ),
                    )
                })
                .collect(),
        ),
        Value::Number(number) => {
            let s = number.to_string();

            let mut matcher = builder.build(&s);
            if compute_matches {
                let matches = matcher.matches();
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(s),
            }
        }
        value => value,
    }
}

pub(crate) fn parse_filter(facets: &Value) -> Result<Option<Filter>, SearchSystemHttpError> {
    match facets {
        object@ Value::Object(_) => {
            let condition = Filter::from_str(&object.to_string())?;
            Ok(condition)
        },
        Value::String(expr) => {
            let condition = Filter::from_str(expr)?;
            Ok(condition)
        }
        Value::Array(arr) => parse_filter_array(arr),
        v => Err(SearchSystemHttpError::InvalidExpression(
            &["String", "Object", "[String | Object]"],
            v.clone())
        ),
    }
}

fn parse_filter_array(arr: &[Value]) -> Result<Option<Filter>, SearchSystemHttpError> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            object@ Value::Object(_) => ands.push(Either::Right(object.to_string().into())),
            Value::String(s) => ands.push(Either::Right(s.as_str().into())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        object@ Value::Object(_) => ors.push(object.to_string().into()),
                        Value::String(s) => ors.push(s.as_str().into()),
                        v => {
                            return Err(SearchSystemHttpError::InvalidExpression(
                                &["String", "Object", "[String | Object]"],
                                v.clone(),
                            ))
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(SearchSystemHttpError::InvalidExpression(
                    &["String", "Object", "[String | Object]"],
                    v.clone(),
                ))
            }
        }
    }

    Ok(Filter::from_array(ands)?)
}


