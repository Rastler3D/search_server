use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, Deref};
use std::str::FromStr;

use deserr::{DeserializeError, Deserr, ErrorKind, MergeWithError, ValuePointerRef};
use fst::IntoStreamer;
use search_engine::proximity::ProximityPrecision;
use search_engine::update::Setting;
use search_engine::{Criterion, CriterionError, Index, DEFAULT_VALUES_PER_FACET};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use serde_json::value::RawValue;
use search_engine::update::analyzer_settings::AnalyzerConfig;
use search_engine::update::split_config::{SplitJoinConfig, SplitJoinSettings};
use search_engine::update::typo_config::{TypoConfig, TypoSettings};

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::*;
use crate::facet_values_sort::FacetValuesSort;

/// The maximum number of results that the engine
/// will be able to return in one search call.
pub const DEFAULT_PAGINATION_MAX_TOTAL_HITS: usize = 1000;

fn serialize_with_wildcard<S>(
    field: &Setting<Vec<String>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    match field {
        Setting::Set(value) => Some(value),
        Setting::Reset => Some(&wildcard),
        Setting::NotSet => None,
    }
    .serialize(s)
}

#[derive(Clone, Default, Debug, Serialize, PartialEq, Eq)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Unchecked;

impl<E> Deserr<E> for Unchecked
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        _value: deserr::Value<V>,
        _location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        unreachable!()
    }
}

fn validate_min_word_size_for_typo_setting<E: DeserializeError>(
    s: TypoSettingsView,
    location: ValuePointerRef,
) -> Result<TypoSettingsView, E> {
    if let (Setting::Set(one), Setting::Set(two)) = (s.one_typo, s.two_typo) {
        if one > two {
            return Err(deserr::take_cf_content(E::error::<Infallible>(None, ErrorKind::Unexpected { msg: format!("Настройка `typoTolerance` недействительна. `twoTypos` должны быть больше или равны `oneTypo`.") }, location)));
        }
    }
    Ok(s)
}

fn validate_analyzer<E: DeserializeError>(
    s: AnalyzerSettings,
    location: ValuePointerRef,
) -> Result<AnalyzerSettings, E> {
    let analyzer = serde_json::from_str::<search_engine::update::analyzer_settings::BoxAnalyzer>(s.0.get());
    return match analyzer {
        Ok(_) => Ok(s),
        Err(err) => Err(deserr::take_cf_content(E::error::<Infallible>(None, ErrorKind::Unexpected { msg: format!("Настройка `analyzer` недействительна. Ошибка десериализации: {err}") }, location)))
    }
}


#[derive(Debug, Clone, Default, Serialize, Deserialize, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(try_from(Value) = serde_json::from_value -> serde_json::error::Error, deny_unknown_fields, rename_all = camelCase, validate = validate_analyzer -> DeserrJsonError<InvalidSettingsAnalyzer>)]
pub struct AnalyzerSettings(Box<RawValue>);

impl Eq for AnalyzerSettings {
}

impl PartialEq for AnalyzerSettings{
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}


#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, validate = validate_min_word_size_for_typo_setting -> DeserrJsonError<InvalidSettingsTypoTolerance>)]
pub struct TypoSettingsView {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_typos: Setting<u32>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub one_typo: Setting<u32>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub two_typo: Setting<u32>,
}

#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct SplitJoinSettingsView {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub splits: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub ngram: Setting<usize>
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FacetingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_values_per_facet: Setting<usize>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub sort_facet_values_by: Setting<BTreeMap<String, FacetValuesSort>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct PaginationSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_total_hits: Setting<usize>,
}

impl MergeWithError<search_engine::CriterionError> for DeserrJsonError<InvalidSettingsRankingRules> {
    fn merge(
        _self_: Option<Self>,
        other: search_engine::CriterionError,
        merge_location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        Self::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

impl MergeWithError<serde_json::Error> for DeserrJsonError<InvalidSettingsAnalyzer> {
    fn merge(
        _self_: Option<Self>,
        other: serde_json::Error,
        merge_location: ValuePointerRef,
    ) -> ControlFlow<Self, Self> {
        Self::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>")
)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct Settings<T> {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchableAttributes>)]
    pub searchable_attributes: WildcardSetting,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFilterableAttributes>)]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSortableAttributes>)]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsRankingRules>)]
    pub ranking_rules: Setting<Vec<RankingRuleView>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSynonyms>)]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsProximityPrecision>)]
    pub proximity_precision: Setting<ProximityPrecisionView>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    pub typo_tolerance: Setting<TypoSettingsView>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSplitJoin>)]
    pub split_join: Setting<SplitJoinSettingsView>,
    //#[serde(default, skip_serializing_if = "Setting::is_not_set")]
    //#[deserr(default, error = DeserrJsonError<InvalidSettingsFaceting>)]
    #[serde(default, skip)]
    #[deserr(default, skip, error = DeserrJsonError<InvalidSettingsFaceting>)]
    pub faceting: Setting<FacetingSettings>,
    //#[serde(default, skip_serializing_if = "Setting::is_not_set")]
    //#[deserr(default, error = DeserrJsonError<InvalidSettingsPagination>)]
    #[serde(default, skip)]
    #[deserr(default, skip, error = DeserrJsonError<InvalidSettingsFaceting>)]
    pub pagination: Setting<PaginationSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsAnalyzer>)]
    pub analyzers: Setting<BTreeMap<String, Setting<AnalyzerSettings>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsEmbedders>)]
    pub embedders: Setting<BTreeMap<String, Setting<search_engine::vector::settings::EmbeddingSettings>>>,

    #[serde(skip)]
    #[deserr(skip)]
    pub _kind: PhantomData<T>,
}

impl<T> Settings<T> {
    pub fn hide_secrets(&mut self) {
        let Setting::Set(embedders) = &mut self.embedders else {
            return;
        };

        for mut embedder in embedders.values_mut() {
            let Setting::Set(embedder) = &mut embedder else {
                continue;
            };

            let Setting::Set(api_key) = &mut embedder.api_key else {
                continue;
            };

            Self::hide_secret(api_key);
        }
    }

    fn hide_secret(secret: &mut String) {
        match secret.len() {
            x if x < 10 => {
                secret.replace_range(.., "XXX...");
            }
            x if x < 20 => {
                secret.replace_range(2.., "XXXX...");
            }
            x if x < 30 => {
                secret.replace_range(3.., "XXXXX...");
            }
            _x => {
                secret.replace_range(5.., "XXXXXX...");
            }
        }
    }
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
        Settings {
            searchable_attributes: Setting::Reset.into(),
            filterable_attributes: Setting::Reset,
            sortable_attributes: Setting::Reset,
            ranking_rules: Setting::Reset,
            synonyms: Setting::Reset,
            proximity_precision: Setting::Reset,
            split_join: Setting::Reset,
            typo_tolerance: Setting::Reset,
            faceting: Setting::Reset,
            pagination: Setting::Reset,
            embedders: Setting::Reset,
            analyzers: Setting::Reset,
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            synonyms,
            proximity_precision,
            split_join,
            typo_tolerance,
            faceting,
            pagination,
            embedders,
            analyzers: analyzer,
            ..
        } = self;

        Settings {
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            synonyms,
            proximity_precision,
            typo_tolerance,
            split_join,
            faceting,
            pagination,
            analyzers: analyzer,
            embedders,
            _kind: PhantomData,
        }
    }
}

impl Settings<Unchecked> {
    pub fn check(self) -> Settings<Checked> {

        let searchable_attributes = match self.searchable_attributes.0 {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        Settings {
            searchable_attributes: searchable_attributes.into(),
            filterable_attributes: self.filterable_attributes,
            sortable_attributes: self.sortable_attributes,
            ranking_rules: self.ranking_rules,
            synonyms: self.synonyms,
            proximity_precision: self.proximity_precision,
            split_join: self.split_join,
            typo_tolerance: self.typo_tolerance,
            faceting: self.faceting,
            pagination: self.pagination,
            embedders: self.embedders,
            analyzers: self.analyzers,
            _kind: PhantomData,
        }
    }

    pub fn validate(self) -> Result<Self, search_engine::Error> {
        self.validate_embedding_settings()
    }

    fn validate_embedding_settings(mut self) -> Result<Self, search_engine::Error> {
        let Setting::Set(mut configs) = self.embedders else { return Ok(self) };
        for (name, config) in configs.iter_mut() {
            let config_to_check = std::mem::take(config);
            let checked_config = search_engine::update::validate_embedding_settings(config_to_check, name)?;
            *config = checked_config
        }
        self.embedders = Setting::Set(configs);
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    pub level_group_size: Option<NonZeroUsize>,
    pub min_level_size: Option<NonZeroUsize>,
}

pub fn apply_settings_to_builder(
    settings: &Settings<Checked>,
    builder: &mut search_engine::update::Settings,
) {
    let Settings {
        searchable_attributes,
        filterable_attributes,
        sortable_attributes,
        ranking_rules,
        synonyms,
        proximity_precision,
        split_join,
        typo_tolerance,
        faceting,
        pagination,
        embedders,
        analyzers: analyzer,
        _kind,
    } = settings;

    match searchable_attributes.deref() {
        Setting::Set(ref names) => builder.set_searchable_fields(names.clone()),
        Setting::Reset => builder.reset_searchable_fields(),
        Setting::NotSet => (),
    }

    match filterable_attributes {
        Setting::Set(ref facets) => {
            builder.set_filterable_fields(facets.clone().into_iter().collect())
        }
        Setting::Reset => builder.reset_filterable_fields(),
        Setting::NotSet => (),
    }

    match sortable_attributes {
        Setting::Set(ref fields) => builder.set_sortable_fields(fields.iter().cloned().collect()),
        Setting::Reset => builder.reset_sortable_fields(),
        Setting::NotSet => (),
    }

    match ranking_rules {
        Setting::Set(ref criteria) => {
            builder.set_criteria(criteria.iter().map(|c| c.clone().into()).collect())
        }
        Setting::Reset => builder.reset_criteria(),
        Setting::NotSet => (),
    }


    match synonyms {
        Setting::Set(ref synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
        Setting::Reset => builder.reset_synonyms(),
        Setting::NotSet => (),
    }


    match proximity_precision {
        Setting::Set(ref precision) => builder.set_proximity_precision((*precision).into()),
        Setting::Reset => builder.reset_proximity_precision(),
        Setting::NotSet => (),
    }

    match typo_tolerance {
        Setting::Set(ref typo_settings) => builder.set_typo_config((*typo_settings).into()),
        Setting::Reset => builder.reset_typo_config(),
        Setting::NotSet => (),
    }

    match split_join {
        Setting::Set(ref split_join_settings) => builder.set_split_join_config((*split_join_settings).into()),
        Setting::Reset => builder.reset_split_join_config(),
        Setting::NotSet => (),
    }

    match faceting {
        Setting::Set(FacetingSettings { max_values_per_facet, sort_facet_values_by }) => {
            match max_values_per_facet {
                Setting::Set(val) => builder.set_max_values_per_facet(*val),
                Setting::Reset => builder.reset_max_values_per_facet(),
                Setting::NotSet => (),
            }
            match sort_facet_values_by {
                Setting::Set(val) => builder.set_sort_facet_values_by(
                    val.iter().map(|(name, order)| (name.clone(), (*order).into())).collect(),
                ),
                Setting::Reset => builder.reset_sort_facet_values_by(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            builder.reset_max_values_per_facet();
            builder.reset_sort_facet_values_by();
        }
        Setting::NotSet => (),
    }

    match pagination {
        Setting::Set(ref value) => match value.max_total_hits {
            Setting::Set(val) => builder.set_pagination_max_total_hits(val),
            Setting::Reset => builder.reset_pagination_max_total_hits(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_max_total_hits(),
        Setting::NotSet => (),
    }

    match embedders {
        Setting::Set(value) => builder.set_embedder_settings(value.clone()),
        Setting::Reset => builder.reset_embedder_settings(),
        Setting::NotSet => (),
    }

    match analyzer {
        Setting::Set(value) => {
            let analyzer = value.iter().map(|(x,y)| (x.clone(), y.as_ref().map(Into::into))).collect();
            builder.set_analyzer_settings(analyzer)
        },
        Setting::Reset => builder.reset_embedder_settings(),
        Setting::NotSet => (),
    }
}

pub enum SecretPolicy {
    RevealSecrets,
    HideSecrets,
}

pub fn settings(
    index: &Index,
    rtxn: &crate::heed::RoTxn,
    secret_policy: SecretPolicy,
) -> Result<Settings<Checked>, search_engine::Error> {

    let searchable_attributes = index
        .user_defined_searchable_fields(rtxn)?
        .map(|fields| fields.into_iter().map(String::from).collect());

    let filterable_attributes = index.filterable_fields(rtxn)?.into_iter().collect();

    let sortable_attributes = index.sortable_fields(rtxn)?.into_iter().collect();

    let criteria = index.criteria(rtxn)?;


    let proximity_precision = index.proximity_precision(rtxn)?.map(ProximityPrecisionView::from);

    let synonyms = index.user_defined_synonyms(rtxn)?;

    let typo_config = index.typo_config(rtxn)?.into();
    let split_join_config = index.split_join_config(rtxn)?.into();

    let faceting = FacetingSettings {
        max_values_per_facet: Setting::Set(
            index
                .max_values_per_facet(rtxn)?
                .map(|x| x as usize)
                .unwrap_or(DEFAULT_VALUES_PER_FACET),
        ),
        sort_facet_values_by: Setting::Set(
            index
                .sort_facet_values_by(rtxn)?
                .into_iter()
                .map(|(name, sort)| (name, sort.into()))
                .collect(),
        ),
    };

    let pagination = PaginationSettings {
        max_total_hits: Setting::Set(
            index
                .pagination_max_total_hits(rtxn)?
                .map(|x| x as usize)
                .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS),
        ),
    };
    let analyzers: BTreeMap<_, _> = index
        .analyzer_configs(rtxn)?
        .into_iter()
        .map(|(name, config)| (name, Setting::Set(config.into())))
        .collect();
    let analyzers = if analyzers.is_empty() { Setting::NotSet } else { Setting::Set(analyzers) };
    let embedders: BTreeMap<_, _> = index
        .embedding_configs(rtxn)?
        .into_iter()
        .map(|(name, config)| (name, Setting::Set(config.into())))
        .collect();
    let embedders = if embedders.is_empty() { Setting::NotSet } else { Setting::Set(embedders) };


    let mut settings = Settings {
        searchable_attributes: match searchable_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        }
        .into(),
        filterable_attributes: Setting::Set(filterable_attributes),
        sortable_attributes: Setting::Set(sortable_attributes),
        ranking_rules: Setting::Set(criteria.iter().map(|c| c.clone().into()).collect()),
        proximity_precision: Setting::Set(proximity_precision.unwrap_or_default()),
        synonyms: Setting::Set(synonyms),
        typo_tolerance: Setting::Set(typo_config),
        split_join: Setting::Set(split_join_config),
        faceting: Setting::Set(faceting),
        pagination: Setting::Set(pagination),
        embedders,
        analyzers,
        _kind: PhantomData,
    };

    if let SecretPolicy::HideSecrets = secret_policy {
        settings.hide_secrets()
    }

    Ok(settings)
}

impl From<TypoConfig> for TypoSettingsView {
    fn from(value: TypoConfig) -> Self {
        Self{
            max_typos: Setting::Set(value.max_typos),
            one_typo: Setting::Set(value.word_len_one_typo),
            two_typo: Setting::Set(value.word_len_two_typo),
        }
    }
}
impl From<TypoSettingsView> for TypoSettings {
    fn from(value: TypoSettingsView) -> Self {
        Self{
            max_typos: value.max_typos,
            word_len_one_typo: value.one_typo,
            word_len_two_typo: value.two_typo,
        }
    }
}

impl From<SplitJoinConfig> for SplitJoinSettingsView {
    fn from(value: SplitJoinConfig) -> Self {
        Self{
            ngram: Setting::Set(value.ngram),
            splits: Setting::Set(value.split_take_n)
        }
    }
}
impl From<SplitJoinSettingsView> for SplitJoinSettings {
    fn from(value: SplitJoinSettingsView) -> Self {
        Self{
            ngram: value.ngram,
            split_take_n: value.ngram
        }
    }
}

impl From<&AnalyzerSettings> for search_engine::update::analyzer_settings::AnalyzerSettings {
    fn from(value: &AnalyzerSettings) -> Self {
        Self{
            analyzer: serde_json::from_str(value.0.get()).unwrap()
        }
    }
}
impl From<AnalyzerConfig> for AnalyzerSettings {
    fn from(value: AnalyzerConfig) -> Self {
        Self{
            0: value.analyzer_config
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserr)]
#[deserr(try_from(&String) = FromStr::from_str -> CriterionError)]
pub enum RankingRuleView {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}
impl Serialize for RankingRuleView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", Criterion::from(self.clone())))
    }
}
impl<'de> Deserialize<'de> for RankingRuleView {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RankingRuleView;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "the name of a valid ranking rule (string)")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let criterion = Criterion::from_str(v).map_err(|_| {
                    E::invalid_value(serde::de::Unexpected::Str(v), &"a valid ranking rule")
                })?;
                Ok(RankingRuleView::from(criterion))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
impl FromStr for RankingRuleView {
    type Err = <Criterion as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RankingRuleView::from(Criterion::from_str(s)?))
    }
}
impl fmt::Display for RankingRuleView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt::Display::fmt(&Criterion::from(self.clone()), f)
    }
}
impl From<Criterion> for RankingRuleView {
    fn from(value: Criterion) -> Self {
        match value {
            Criterion::Words => RankingRuleView::Words,
            Criterion::Typo => RankingRuleView::Typo,
            Criterion::Proximity => RankingRuleView::Proximity,
            Criterion::Attribute => RankingRuleView::Attribute,
            Criterion::Sort => RankingRuleView::Sort,
            Criterion::Exactness => RankingRuleView::Exactness,
            Criterion::Asc(x) => RankingRuleView::Asc(x),
            Criterion::Desc(x) => RankingRuleView::Desc(x),
        }
    }
}
impl From<RankingRuleView> for Criterion {
    fn from(value: RankingRuleView) -> Self {
        match value {
            RankingRuleView::Words => Criterion::Words,
            RankingRuleView::Typo => Criterion::Typo,
            RankingRuleView::Proximity => Criterion::Proximity,
            RankingRuleView::Attribute => Criterion::Attribute,
            RankingRuleView::Sort => Criterion::Sort,
            RankingRuleView::Exactness => Criterion::Exactness,
            RankingRuleView::Asc(x) => Criterion::Asc(x),
            RankingRuleView::Desc(x) => Criterion::Desc(x),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Deserr, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = DeserrJsonError<InvalidSettingsProximityPrecision>, rename_all = camelCase, deny_unknown_fields)]
pub enum ProximityPrecisionView {
    #[default]
    ByWord,
    ByAttribute,
}

impl From<ProximityPrecision> for ProximityPrecisionView {
    fn from(value: ProximityPrecision) -> Self {
        match value {
            ProximityPrecision::ByWord => ProximityPrecisionView::ByWord,
            ProximityPrecision::ByAttribute => ProximityPrecisionView::ByAttribute,
        }
    }
}
impl From<ProximityPrecisionView> for ProximityPrecision {
    fn from(value: ProximityPrecisionView) -> Self {
        match value {
            ProximityPrecisionView::ByWord => ProximityPrecision::ByWord,
            ProximityPrecisionView::ByAttribute => ProximityPrecision::ByAttribute,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct WildcardSetting(Setting<Vec<String>>);

impl From<Setting<Vec<String>>> for WildcardSetting {
    fn from(setting: Setting<Vec<String>>) -> Self {
        Self(setting)
    }
}

impl Serialize for WildcardSetting {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_with_wildcard(&self.0, serializer)
    }
}

impl<E: deserr::DeserializeError> Deserr<E> for WildcardSetting {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        Ok(Self(Setting::deserialize_from_value(value, location)?))
    }
}

impl std::ops::Deref for WildcardSetting {
    type Target = Setting<Vec<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

}
