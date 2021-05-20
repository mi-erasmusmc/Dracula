use std::collections::BTreeMap;
use std::error::Error;
use std::fs;

use deadpool_postgres::tokio_postgres::Row;
use deadpool_postgres::Pool;
use log::{debug, info};
use pbr::ProgressBar;
use rawsql::Loader;

use crate::db::{execute, query};

pub async fn find_pts(pool: &Pool, precision: i64) -> Result<(), Box<dyn Error>> {
    info!("Starting MedDRA standardization");
    let client = pool.get().await?;
    let queries = Loader::get_queries_from("./sql/meddra_mapping.sql")
        .unwrap()
        .queries;

    info!("Finding exact matches ... takes about 6 minutes to write 6 million records");
    let q = fs::read_to_string("./sql/create_reported_events_table.sql")?;
    client.batch_execute(q.as_str()).await?;

    let fast_precision: f32 = precision as f32 * 1.6;
    let dam_lev_cutoff: usize = (precision * 5) as usize;
    info!(
        "Fuzzy search using a [{}] sift3 cutoff of and a [{}] damerau levenshtein max distance",
        fast_precision, dam_lev_cutoff
    );

    execute("update_general_disorders", &client, &queries).await;
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
        let pts = query("find_pts", &client, &queries, &[&soc]).await;

        make_comparisons(terms_to_map, pts, fast_precision, dam_lev_cutoff, &pool).await?;
    }

    execute("catch_terms_including_the_word_or", &client, &queries).await;
    execute("insert_wild_pt_code_1", &client, &queries).await;
    execute("insert_wild_pt_code_2", &client, &queries).await;
    let terms_to_map = query("find_all_unknown_terms", &client, &queries, &[]).await;
    let pts = query("find_all_pts", &client, &queries, &[]).await;
    info!(
        "Going to check {} unmapped adverse events against all {} MedDRA preferred terms ... ",
        terms_to_map.len(),
        pts.len()
    );
    make_comparisons(terms_to_map, pts, 1.6, 5, &pool).await?;

    info!("Adding preferred term codes (pt_code) to reported events table");
    execute("insert_wild_pt_code_1", &client, &queries).await;
    execute("insert_wild_pt_code_2", &client, &queries).await;

    Ok(())
}

async fn make_comparisons(
    terms_to_map: Vec<Row>,
    pts: Vec<Row>,
    fast_precision: f32,
    dam_lev_cutoff: usize,
    pool: &Pool,
) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await?;
    let mut insert_values: String = String::from("");
    let mut pb = ProgressBar::new((terms_to_map.len() / 100) as u64);
    pb.set_width(Some(80));
    let mut counter: i8 = 0;
    for term_row in &terms_to_map {
        counter += 1;
        if counter % 100 == 0 {
            pb.inc();
        }
        let mut comparisons: BTreeMap<usize, (&str, i64)> = BTreeMap::new();
        let term: &str = term_row.get(0);
        let term_clean = term
            .replace("other", "")
            .replace(" nos", "")
            .replace(" any", "")
            .replace("specify", "")
            .replace("  ", " ");
        for pt_row in &pts {
            let pt = pt_row.get("pt_name");
            let pt_code: i64 = pt_row.get("pt_code");
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
                "(\'{}\', \'{}\', {}),",
                fixed_original, fixed_standard, pt.1 .1
            );
            insert_values.push_str(&*value);
        }
    }
    // remove trailing comma
    insert_values.pop();
    let q = format!(
        "INSERT INTO ctgov.rg_meddra_map(original, standard, pt_code) VALUES {};",
        insert_values
    );
    pb.finish();
    println!();
    let r1 = client.execute(q.as_str(), &[]).await.unwrap();
    info!("Mapped {} out of {} unknown terms", r1, terms_to_map.len());

    Ok(())
}
