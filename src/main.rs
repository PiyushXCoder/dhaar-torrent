use crate::config::get_configuration;

pub mod bencode;
mod config;

#[tokio::main]
async fn main() {
    get_configuration();
}
