use actix_web as aweb;
use aweb::error::{JsonPayloadError, QueryPayloadError};
use byte_unit::Byte;
use shared_types::document_formats::{DocumentFormatError, PayloadType};
use shared_types::error::{Code, ErrorCode, ResponseError};
use shared_types::index_uid::{IndexUid, IndexUidFormatError};
use serde_json::Value;
use tokio::task::JoinError;

#[derive(Debug, thiserror::Error)]
pub enum SearchSystemHttpError {
    #[error("A Content-Type header is missing. Accepted values for the Content-Type header are: {}",
            .0.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", "))]
    MissingContentType(Vec<String>),
    #[error("Маршрут `/logs/stream` в настоящее время используется кем-то другим.")]
    AlreadyUsedLogRoute,
    #[error("Content-Type `{0}` не поддерживает использование разделителя csv. Разделитель csv может использоваться только с Content-Type `text/csv`.")]
    CsvDelimiterWithWrongContentType(String),
    #[error(
        "Заголовок Content-Type `{0}` недопустим. Допустимыми значениями для заголовка Content-Type являются: {}",
        .1.iter().map(|s| format!("`{}`", s)).collect::<Vec<_>>().join(", ")
    )]
    InvalidContentType(String, Vec<String>),
    #[error("Документ `{0}` не найден.")]
    DocumentNotFound(String),
    #[error("Отправка пустого фильтра запрещена.")]
    EmptyFilter,
    #[error("Неверный синтаксис для параметра фильтра: `ожидалось {}, найдено: {1}`.", .0.join(", "))]
    InvalidExpression(&'static [&'static str], Value),
    #[error("Отсутствует тело запроса {0}.")]
    MissingPayload(PayloadType),
    #[error("Слишком много поисковых запросов, выполняемых одновременно: {0}. Повторите попытку через 10 с.")]
    TooManySearchRequests(usize),
    #[error("Внутренняя ошибка: Ограничитель поиска не работает.")]
    SearchLimiterIsDown,
    #[error("Тело запроса слишком большого размера. Максимально допустимый размер полезной нагрузки составляет {}.",  Byte::from_bytes(*.0 as u64).get_appropriate_unit(true))]
    PayloadTooLarge(usize),
    #[error("Для свопа индексов необходимо указать два индекса. Список `[{}]` содержит {} индексов.",
        .0.iter().map(|uid| format!("\"{uid}\"")).collect::<Vec<_>>().join(", "), .0.len()
    )]
    SwapIndexPayloadWrongLength(Vec<IndexUid>),
    #[error(transparent)]
    IndexUid(#[from] IndexUidFormatError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    HeedError(#[from] shared_types::heed::Error),
    #[error(transparent)]
    IndexScheduler(#[from] index_scheduler::Error),
    #[error(transparent)]
    SearchEngine(#[from] shared_types::search_engine::Error),
    #[error(transparent)]
    Payload(#[from] PayloadError),
    #[error(transparent)]
    FileStore(#[from] file_store::Error),
    #[error(transparent)]
    DocumentFormat(#[from] DocumentFormatError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error("Некорректный запрос: отсутствует параметр `hybrid`, когда присутствуют и `q`, и `vector`.")]
    MissingSearchHybrid,
}

impl ErrorCode for SearchSystemHttpError {
    fn error_code(&self) -> Code {
        match self {
            SearchSystemHttpError::MissingContentType(_) => Code::MissingContentType,
            SearchSystemHttpError::AlreadyUsedLogRoute => Code::BadRequest,
            SearchSystemHttpError::CsvDelimiterWithWrongContentType(_) => Code::InvalidContentType,
            SearchSystemHttpError::MissingPayload(_) => Code::MissingPayload,
            SearchSystemHttpError::InvalidContentType(_, _) => Code::InvalidContentType,
            SearchSystemHttpError::DocumentNotFound(_) => Code::DocumentNotFound,
            SearchSystemHttpError::EmptyFilter => Code::InvalidDocumentFilter,
            SearchSystemHttpError::InvalidExpression(_, _) => Code::InvalidSearchFilter,
            SearchSystemHttpError::PayloadTooLarge(_) => Code::PayloadTooLarge,
            SearchSystemHttpError::TooManySearchRequests(_) => Code::TooManySearchRequests,
            SearchSystemHttpError::SearchLimiterIsDown => Code::Internal,
            SearchSystemHttpError::SwapIndexPayloadWrongLength(_) => Code::InvalidSwapIndexes,
            SearchSystemHttpError::IndexUid(e) => e.error_code(),
            SearchSystemHttpError::SerdeJson(_) => Code::Internal,
            SearchSystemHttpError::HeedError(_) => Code::Internal,
            SearchSystemHttpError::IndexScheduler(e) => e.error_code(),
            SearchSystemHttpError::SearchEngine(e) => e.error_code(),
            SearchSystemHttpError::Payload(e) => e.error_code(),
            SearchSystemHttpError::FileStore(_) => Code::Internal,
            SearchSystemHttpError::DocumentFormat(e) => e.error_code(),
            SearchSystemHttpError::Join(_) => Code::Internal,
            SearchSystemHttpError::MissingSearchHybrid => Code::MissingSearchHybrid,
        }
    }
}

impl From<SearchSystemHttpError> for aweb::Error {
    fn from(other: SearchSystemHttpError) -> Self {
        aweb::Error::from(ResponseError::from(other))
    }
}

impl From<aweb::error::PayloadError> for SearchSystemHttpError {
    fn from(error: aweb::error::PayloadError) -> Self {
        SearchSystemHttpError::Payload(PayloadError::Payload(error))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error(transparent)]
    Payload(aweb::error::PayloadError),
    #[error(transparent)]
    Json(JsonPayloadError),
    #[error(transparent)]
    Query(QueryPayloadError),
    #[error("Json тело запроса неправильно сформировано. `{0}`.")]
    MalformedPayload(serde_json::error::Error),
    #[error("Отсутствует тело запроса в формате json.")]
    MissingPayload,
    #[error("Ошибка при получении тела запроса. `{0}`.")]
    ReceivePayload(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl ErrorCode for PayloadError {
    fn error_code(&self) -> Code {
        match self {
            PayloadError::Payload(e) => match e {
                aweb::error::PayloadError::Incomplete(_) => Code::Internal,
                aweb::error::PayloadError::EncodingCorrupted => Code::Internal,
                aweb::error::PayloadError::Overflow => Code::PayloadTooLarge,
                aweb::error::PayloadError::UnknownLength => Code::Internal,
                aweb::error::PayloadError::Http2Payload(_) => Code::Internal,
                aweb::error::PayloadError::Io(_) => Code::Internal,
                _ => todo!(),
            },
            PayloadError::Json(err) => match err {
                JsonPayloadError::Overflow { .. } => Code::PayloadTooLarge,
                JsonPayloadError::ContentType => Code::UnsupportedMediaType,
                JsonPayloadError::Payload(aweb::error::PayloadError::Overflow) => {
                    Code::PayloadTooLarge
                }
                JsonPayloadError::Payload(_) => Code::BadRequest,
                JsonPayloadError::Deserialize(_) => Code::BadRequest,
                JsonPayloadError::Serialize(_) => Code::Internal,
                _ => Code::Internal,
            },
            PayloadError::Query(err) => match err {
                QueryPayloadError::Deserialize(_) => Code::BadRequest,
                _ => Code::Internal,
            },
            PayloadError::MissingPayload => Code::MissingPayload,
            PayloadError::MalformedPayload(_) => Code::MalformedPayload,
            PayloadError::ReceivePayload(_) => Code::Internal,
        }
    }
}

impl From<JsonPayloadError> for PayloadError {
    fn from(other: JsonPayloadError) -> Self {
        match other {
            JsonPayloadError::Deserialize(e)
                if e.classify() == serde_json::error::Category::Eof
                    && e.line() == 1
                    && e.column() == 0 =>
            {
                Self::MissingPayload
            }
            JsonPayloadError::Deserialize(e)
                if e.classify() != serde_json::error::Category::Data =>
            {
                Self::MalformedPayload(e)
            }
            _ => Self::Json(other),
        }
    }
}

impl From<QueryPayloadError> for PayloadError {
    fn from(other: QueryPayloadError) -> Self {
        Self::Query(other)
    }
}

impl From<PayloadError> for aweb::Error {
    fn from(other: PayloadError) -> Self {
        aweb::Error::from(ResponseError::from(other))
    }
}
