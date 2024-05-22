use deserr::Deserr;
use search_engine::OrderBy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Deserr)]
#[serde(rename_all = "camelCase")]
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

impl From<OrderBy> for FacetValuesSort {
    fn from(val: OrderBy) -> Self {
        match val {
            OrderBy::Lexicographic => FacetValuesSort::Alpha,
            OrderBy::Count => FacetValuesSort::Count,
        }
    }
}
