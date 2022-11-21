use std::error::Error;
use std::io::Write;
use std::time::Instant;

use chrono::Local;
use config::{Config, File};
use env_logger::Builder;
use log::{info, LevelFilter};

use crate::arm_to_intervention::connect_arms_to_interventions;
use crate::drug_mapping::{find_drugs, read_descriptions};
use crate::meddra_mapping::find_pts;

mod arm_to_intervention;
mod db;
mod drug_mapping;
mod meddra_mapping;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_logger();

    info!("Count Dracula awakens ...");
    let start = Instant::now();

    info!("Reading Settings.toml");
    let settings = Config::builder()
        .add_source(File::with_name("Settings"))
        .build()
        .expect("Error reading Settings.toml");

    info!("Initializing DB pool");
    let pool = db::init_db_pool(&settings);

    find_drugs(&pool).await?;
    connect_arms_to_interventions(&pool).await?;
    read_descriptions(&pool).await?;

    let skip_meddra = settings
        .get_bool("skip_meddra")
        .expect("Could not read skip_meddra from the settings file");
    if !skip_meddra {
        let precision = settings
            .get_int("meddra_precision")
            .expect("Could not read meddra_precision from settings file");
        find_pts(&pool, precision).await?;
    } else {
        info!("Skipping MedDRA standardization step")
    }

    print_end(start);
    Ok(())
}

fn init_logger() {
    let mut builder = Builder::from_default_env();

    builder
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}] {}",
                record.level(),
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
}

fn print_end(start: Instant) {
    let elapsed_secs = start.elapsed().as_secs();
    let elapsed_min = elapsed_secs / 60;
    let hours = elapsed_secs / 3600;
    let minutes = elapsed_min % 60;
    let seconds = elapsed_secs % 60;

    info!(
        "Having feasted for {} hours {} minutes {} seconds, Dracula now goes back to sleep",
        hours, minutes, seconds
    );
}
