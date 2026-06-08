use std::fs;

use clap::Parser;
use home_config::HomeConfig;
use merge::Merge;

pub(crate) mod models;

pub(crate) fn get_configuration() -> models::CliArgsConfig {
    let mut config_from_cliargs = models::CliArgsConfig::parse();

    let config_file_data = match &config_from_cliargs.config_file {
        Some(config_file) => fs::read_to_string(config_file).expect("Failed to read config file"),
        None => {
            let home_config_file = HomeConfig::with_config_dir("dhaar-torrent", "config.toml");
            home_config_file
                .read_to_string()
                .expect("Failed to read config file")
        }
    };

    let config_from_file: models::FileConfig =
        toml::from_str(&config_file_data).expect("Failed to parse config file");
    config_from_cliargs.merge(config_from_file);
    config_from_cliargs
}

impl models::CliArgsConfig {
    pub(crate) fn merge(&mut self, other: models::FileConfig) {
        self.file.merge(other);
    }
}
