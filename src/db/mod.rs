use std::collections::HashMap;
use std::time::Instant;

use config::Config;
use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::types::ToSql;
use deadpool_postgres::tokio_postgres::Error;
use deadpool_postgres::tokio_postgres::Row;
use deadpool_postgres::{ClientWrapper, Manager, ManagerConfig, Pool, RecyclingMethod};
use log::debug;
use tokio_postgres::NoTls;

pub fn init_db_pool(config: &Config) -> Pool {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.port(config.get_int("pg_port").unwrap() as u16);
    pg_config.host(&*config.get_str("pg_host").unwrap());
    pg_config.user(&*config.get_str("pg_user").unwrap());
    pg_config.dbname(&*config.get_str("pg_dbname").unwrap());
    pg_config.password(&*config.get_str("pg_password").unwrap());
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr: Manager<NoTls> = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool: Pool = Pool::new(mgr, 6);
    pool
}

pub async fn execute(
    query_name: &str,
    client: &Object<ClientWrapper, Error>,
    queries: &HashMap<String, String>,
) {
    let start = Instant::now();

    debug!("Executing the {} query... ", query_name);

    let result = client
        .execute(queries.get(query_name).unwrap().as_str(), &[])
        .await
        .expect(&msg(query_name));

    let seconds = start.elapsed().as_secs_f32();

    debug!(
        "{} rows affected in {:.2}s executing the {} query",
        result, seconds, query_name
    );
}

pub async fn query(
    query_name: &str,
    client: &Object<ClientWrapper, Error>,
    queries: &HashMap<String, String>,
    params: &[&(dyn ToSql + Sync)],
) -> Vec<Row> {
    let start = Instant::now();

    debug!("Executing the {} query... ", query_name);

    let result = client
        .query(queries.get(query_name).unwrap().as_str(), params)
        .await
        .expect(&msg(query_name));

    let seconds = start.elapsed().as_secs_f32();

    debug!(
        "{} results returned in {:.2}s for the {} query",
        result.len(),
        seconds,
        query_name
    );
    result
}

fn msg(query: &str) -> String {
    format!("Error executing {}", query)
}
