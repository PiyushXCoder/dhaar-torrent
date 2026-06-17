use std::path::PathBuf;

use clap::Parser;
use merge::Merge;
use serde::{Deserialize, Serialize};

#[derive(Parser, Serialize, Deserialize, Debug, Merge)]
pub struct FileConfig {
    // #[arg(short, long)]
    // #[merge(strategy = merge::option::overwrite_none)]
    // pub name: Option<String>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CliArgsConfig {
    #[command(flatten)]
    pub file: FileConfig,
    pub torrent_file: PathBuf,

    #[arg(short, long)]
    pub config_file: Option<PathBuf>,
}
