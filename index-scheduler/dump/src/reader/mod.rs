use std::fs::File;
use std::io::{BufReader, Read};

use flate2::bufread::GzDecoder;
use serde::Deserialize;
use tempfile::TempDir;

use self::v1::{V1IndexReader, V1Reader};
use crate::{Result, Version};

mod v1;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type UpdateFile = dyn Iterator<Item = Result<Document>>;

pub enum DumpReader {
    Current(V1Reader),
}

impl DumpReader {
    pub fn open(dump: impl Read) -> Result<DumpReader> {
        let path = TempDir::new()?;
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(path.path())?;

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MetadataVersion {
            pub dump_version: Version,
        }
        let mut meta_file = File::open(path.path().join("metadata.json"))?;
        let MetadataVersion { dump_version } = serde_json::from_reader(&mut meta_file)?;

        match dump_version {
            Version::V1 => {
                Ok(v1::V1Reader::open(path)?.into())
            }

        }
    }

    pub fn version(&self) -> crate::Version {
        match self {
            DumpReader::Current(current) => current.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            DumpReader::Current(current) => current.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            DumpReader::Current(current) => current.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<DumpIndexReader>> + '_>> {
        match self {
            DumpReader::Current(current) => {
                let indexes = Box::new(current.indexes()?.map(|res| res.map(DumpIndexReader::from)))
                    as Box<dyn Iterator<Item = Result<DumpIndexReader>> + '_>;
                Ok(indexes)
            }
        }
    }

    pub fn tasks(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(v1::Task, Option<Box<UpdateFile>>)>> + '_>> {
        match self {
            DumpReader::Current(current) => Ok(current.tasks()),
        }
    }

    pub fn keys(&mut self) -> Result<Box<dyn Iterator<Item = Result<v1::Key>> + '_>> {
        match self {
            DumpReader::Current(current) => Ok(current.keys()),
        }
    }

    pub fn features(&self) -> Result<Option<v1::RuntimeTogglableFeatures>> {
        match self {
            DumpReader::Current(current) => Ok(current.features()),
        }
    }
}

impl From<V1Reader> for DumpReader {
    fn from(value: V1Reader) -> Self {
        DumpReader::Current(value)
    }
}


pub enum DumpIndexReader {
    Current(v1::V1IndexReader),
}

impl DumpIndexReader {
    pub fn new_v6(v6: v1::V1IndexReader) -> DumpIndexReader {
        DumpIndexReader::Current(v6)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            DumpIndexReader::Current(v6) => v6.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            DumpIndexReader::Current(v6) => v6
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v1::Settings<v1::Checked>> {
        match self {
            DumpIndexReader::Current(v6) => v6.settings(),
        }
    }
}

impl From<V1IndexReader> for DumpIndexReader {
    fn from(value: V1IndexReader) -> Self {
        DumpIndexReader::Current(value)
    }
}
