use std::path::PathBuf;

use clap::Parser;
use merge::Merge;
use serde::{Deserialize, Serialize};

#[derive(Parser, Serialize, Deserialize, Debug, Merge)]
pub(crate) struct FileConfig {
    // #[arg(short, long)]
    // #[merge(strategy = merge::option::overwrite_none)]
    // pub(crate) name: Option<String>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct CliArgsConfig {
    #[command(flatten)]
    pub(crate) file: FileConfig,
    pub(crate) torrent_file: PathBuf,

    #[arg(short, long)]
    pub(crate) config_file: Option<PathBuf>,
}
