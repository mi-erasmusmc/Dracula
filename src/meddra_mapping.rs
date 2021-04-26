use std::collections::BTreeMap;
use std::error::Error;
use std::fs;

use deadpool_postgres::Pool;
use log::{debug, info};
use rawsql::Loader;

use crate::db::{execute, query};

pub async fn find_pts(pool: &Pool, precision: i64) -> Result<(), Box<dyn Error>> {
    info!("Starting MedDRA standardization");

    let client = pool.get().await?;
    let queries = Loader::get_queries_from("./sql/meddra_mapping.sql")
        .unwrap()
        .queries;

    info!("Finding exact matches ... takes about 15 minutes to write 6 million records");
    let q = fs::read_to_string("./sql/create_reported_events_table.sql")?;
    debug!("{}", q);
    client.batch_execute(q.as_str()).await?;

    let fast_precision: f32 = precision as f32 * 1.6;
    let dam_lev_cutoff: usize = (precision * 5) as usize;
    info!(
        "Fuzzy search using a [{}] sift3 cutoff of and a [{}] damerau levenshtein max distance",
        fast_precision, dam_lev_cutoff
    );

    execute("drop_table", &client, &queries).await;
    execute("create_table", &client, &queries).await;

    let socs = query("find_all_socs", &client, &queries, &[]).await;

    let mut row_number: i8 = 0;
    let total_rows = socs.len();
    for soc_row in socs {
        row_number += 1;
        let soc: String = soc_row.get(0);
        info!("[{}/{}] Mapping {}", row_number, total_rows, soc);
        let terms_to_map = query("find_unknown_terms", &client, &queries, &[&soc]).await;
        if terms_to_map.is_empty() {
            continue;
        }
        let pts = query("find_all_pts", &client, &queries, &[&soc]).await;

        let mut insert_values: String = String::from("");
        for term_row in &terms_to_map {
            let mut comparisons: BTreeMap<usize, (&str, i32)> = BTreeMap::new();
            let term: &str = term_row.get(0);
            let term_clean = term
                .replace("other", "")
                .replace(" nos", "")
                .replace(" any", "")
                .replace("specify", "")
                .replace("  ", " ");
            for pt_row in &pts {
                let pt = pt_row.get("pt_name");
                let pt_code: i32 = pt_row.get("pt_code");
                let fast_distance = distance::sift3(&term_clean, pt);
                if fast_distance < fast_precision {
                    let distance = distance::damerau_levenshtein(&term_clean, pt);
                    if distance < dam_lev_cutoff {
                        comparisons.insert(distance, (pt, pt_code));
                    }
                }
            }
            if !comparisons.is_empty() {
                let pt = comparisons.iter().next().unwrap();
                debug!(
                    "matched  {0: <25}  to  {1: <25}  {2: <20}",
                    term, pt.1 .0, pt.0
                );
                // quotation marks must be escaped
                let fixed_original = term.replace("'", "''");
                let fixed_standard = pt.1 .0.replace("'", "''");
                let value = format!(
                    "(\'{}\', \'{}\', \'{}\', {}),",
                    fixed_original, soc, fixed_standard, pt.1 .1
                );
                insert_values.push_str(&*value);
            }
        }
        // remove trailing comma
        insert_values.pop();
        let q = format!(
            "INSERT INTO ctgov.rg_meddra_map(original, soc, standard, pt_code) VALUES {};",
            insert_values
        );
        debug!("{}", q);
        let r1 = client.execute(q.as_str(), &[]).await.unwrap();
        info!(
            "[{}/{}] Mapped {} out of {} unknown terms",
            row_number,
            total_rows,
            r1,
            terms_to_map.len()
        );
    }

    info!("Adding preferred term codes (pt_code) to reported events table");
    execute("insert_wild_pt_code", &client, &queries).await;

    Ok(())
}
