use crate::config::get_configuration;

mod config;

#[tokio::main]
async fn main() {
    get_configuration();
}
