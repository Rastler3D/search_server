use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use flate2::write::GzEncoder;
use flate2::Compression;
use shared_types::features::RuntimeTogglableFeatures;
use shared_types::keys::Key;
use shared_types::settings::{Checked, Settings};
use serde_json::{Map, Value};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::reader::Document;
use crate::{IndexMetadata, Metadata, Result, TaskDump, CURRENT_DUMP_VERSION};

pub struct DumpWriter {
    dir: TempDir,
}

impl DumpWriter {
    pub fn new(instance_uuid: Option<Uuid>) -> Result<DumpWriter> {
        let dir = TempDir::new()?;

        if let Some(instance_uuid) = instance_uuid {
            fs::write(
                dir.path().join("instance_uid.uuid"),
                instance_uuid.as_hyphenated().to_string(),
            )?;
        }

        let metadata = Metadata {
            dump_version: CURRENT_DUMP_VERSION,
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            dump_date: OffsetDateTime::now_utc(),
        };
        fs::write(dir.path().join("metadata.json"), serde_json::to_string(&metadata)?)?;

        std::fs::create_dir(dir.path().join("indexes"))?;

        Ok(DumpWriter { dir })
    }

    pub fn create_index(&self, index_name: &str, metadata: &IndexMetadata) -> Result<IndexWriter> {
        IndexWriter::new(self.dir.path().join("indexes").join(index_name), metadata)
    }

    pub fn create_keys(&self) -> Result<KeyWriter> {
        KeyWriter::new(self.dir.path().to_path_buf())
    }

    pub fn create_tasks_queue(&self) -> Result<TaskWriter> {
        TaskWriter::new(self.dir.path().join("tasks"))
    }

    pub fn create_experimental_features(&self, features: RuntimeTogglableFeatures) -> Result<()> {
        Ok(std::fs::write(
            self.dir.path().join("experimental-features.json"),
            serde_json::to_string(&features)?,
        )?)
    }

    pub fn persist_to(self, mut writer: impl Write) -> Result<()> {
        let gz_encoder = GzEncoder::new(&mut writer, Compression::default());
        let mut tar_encoder = tar::Builder::new(gz_encoder);
        tar_encoder.append_dir_all(".", self.dir.path())?;
        let gz_encoder = tar_encoder.into_inner()?;
        gz_encoder.finish()?;
        writer.flush()?;

        Ok(())
    }
}

pub struct KeyWriter {
    keys: BufWriter<File>,
}

impl KeyWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        let keys = File::create(path.join("keys.jsonl"))?;
        Ok(KeyWriter { keys: BufWriter::new(keys) })
    }

    pub fn push_key(&mut self, key: &Key) -> Result<()> {
        self.keys.write_all(&serde_json::to_vec(key)?)?;
        self.keys.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(mut self) -> Result<()> {
        self.keys.flush()?;
        Ok(())
    }
}

pub struct TaskWriter {
    queue: BufWriter<File>,
    update_files: PathBuf,
}

impl TaskWriter {
    pub(crate) fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let queue = File::create(path.join("queue.jsonl"))?;
        let update_files = path.join("update_files");
        std::fs::create_dir(&update_files)?;

        Ok(TaskWriter { queue: BufWriter::new(queue), update_files })
    }

    /// Pushes tasks in the dump.
    /// If the tasks has an associated `update_file` it'll use the `task_id` as its name.
    pub fn push_task(&mut self, task: &TaskDump) -> Result<UpdateFile> {
        self.queue.write_all(&serde_json::to_vec(task)?)?;
        self.queue.write_all(b"\n")?;

        Ok(UpdateFile::new(self.update_files.join(format!("{}.jsonl", task.uid))))
    }

    pub fn flush(mut self) -> Result<()> {
        self.queue.flush()?;
        Ok(())
    }
}

pub struct UpdateFile {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl UpdateFile {
    pub(crate) fn new(path: PathBuf) -> UpdateFile {
        UpdateFile { path, writer: None }
    }

    pub fn push_document(&mut self, document: &Document) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.write_all(&serde_json::to_vec(document)?)?;
            writer.write_all(b"\n")?;
        } else {
            let file = File::create(&self.path).unwrap();
            self.writer = Some(BufWriter::new(file));
            self.push_document(document)?;
        }
        Ok(())
    }

    pub fn flush(self) -> Result<()> {
        if let Some(mut writer) = self.writer {
            writer.flush()?;
        }
        Ok(())
    }
}

pub struct IndexWriter {
    documents: BufWriter<File>,
    settings: File,
}

impl IndexWriter {
    pub(self) fn new(path: PathBuf, metadata: &IndexMetadata) -> Result<Self> {
        std::fs::create_dir(&path)?;

        let metadata_file = File::create(path.join("metadata.json"))?;
        serde_json::to_writer(metadata_file, metadata)?;

        let documents = File::create(path.join("documents.jsonl"))?;
        let settings = File::create(path.join("settings.json"))?;

        Ok(IndexWriter { documents: BufWriter::new(documents), settings })
    }

    pub fn push_document(&mut self, document: &Map<String, Value>) -> Result<()> {
        serde_json::to_writer(&mut self.documents, document)?;
        self.documents.write_all(b"\n")?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.documents.flush()?;
        Ok(())
    }

    pub fn settings(mut self, settings: &Settings<Checked>) -> Result<()> {
        self.settings.write_all(&serde_json::to_vec(&settings)?)?;
        Ok(())
    }
}


