fn main() {
    #[cfg(feature = "mini-dashboard")]
    mini_dashboard::setup_mini_dashboard().expect("Could not load the mini-dashboard assets");
}

#[cfg(feature = "mini-dashboard")]
mod mini_dashboard {
    use std::env;
    use std::fs::{create_dir_all, File, OpenOptions};
    use std::io::{Cursor, Read, Write};
    use std::path::PathBuf;

    use anyhow::Context;
    use cargo_toml::Manifest;
    use reqwest::blocking::get;
    use sha1::{Digest, Sha1};
    use static_files::resource_dir;

    pub fn setup_mini_dashboard() -> anyhow::Result<()> {
        resource_dir("dist").build()?;

        Ok(())
    }
}
