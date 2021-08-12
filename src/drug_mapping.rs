use std::collections::HashMap;
use std::env;
use std::error::Error;

use deadpool_postgres::tokio_postgres::Row;
use deadpool_postgres::Pool;
use log::{debug, info, warn};
use pbr::ProgressBar;
use rawsql::Loader;

use crate::db::execute;

pub async fn find_drugs(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("./sql/drug_mapping.sql")
        .unwrap()
        .queries;

    let client = pool.get().await?;

    load_art57(&pool).await;

    info!("Creating tables");
    execute("drop_mapping_table", &client, &queries).await;
    execute("drop_join_table", &client, &queries).await;
    execute("create_mapping_table", &client, &queries).await;
    execute("mapping_clean_index", &client, &queries).await;
    execute("create_join_table", &client, &queries).await;
    execute("remove_junk", &client, &queries).await;

    direct_match(&queries, &pool).await;

    info!("Regex cleaning of data");
    execute("remove_q2w", &client, &queries).await;
    execute("remove_non_alpha_numeric", &client, &queries).await;
    execute("sham_to_placebo", &client, &queries).await;
    execute("remove_qd", &client, &queries).await;
    execute("remove_group", &client, &queries).await;
    execute("remove_cohort", &client, &queries).await;
    execute("remove_arm", &client, &queries).await;
    execute("remove_spaces", &client, &queries).await;
    execute("trim", &client, &queries).await;
    execute("remove_junk", &client, &queries).await;

    direct_match(&queries, &pool).await;

    execute("pre_and_append_spaces", &client, &queries).await;

    let ttys = get_ttys();
    for tty in ttys {
        info!(
            "Going to check if titles or names contain any RxNorm {}",
            tty.1
        );
        match_words(&pool, &tty.0, &queries).await;
    }

    execute("remove_spaces", &client, &queries).await;
    execute("trim", &client, &queries).await;
    execute("remove_junk", &client, &queries).await;

    direct_match(&queries, &pool).await;

    info!("Creating join tables for mapped rxcuis");
    execute("drop_join_table_1", &client, &queries).await;
    execute("drop_join_table_2", &client, &queries).await;
    execute("drop_join_table_3", &client, &queries).await;
    execute("drop_join_table_4", &client, &queries).await;

    execute("join_table_1", &client, &queries).await;
    execute("join_table_2", &client, &queries).await;
    execute("join_table_3", &client, &queries).await;
    execute("join_table_4", &client, &queries).await;

    Ok(())
}

async fn load_art57(pool: &Pool) {
    info!("Loading article 57 data");
    let client = pool.get().await.unwrap();
    let pwd = env::current_dir().unwrap();
    let pwd = pwd.to_str().unwrap();
    let path = format!("'{}/{}'", pwd, "resources/art57_rxnorm.tsv");
    debug!("{}", path);
    let query = format!(
        "DROP TABLE IF EXISTS ctgov.article57_rxnorm;
        CREATE TABLE ctgov.article57_rxnorm (name TEXT,	ingredient TEXT,	rxcui TEXT);
        COPY ctgov.article57_rxnorm FROM {} WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b'",
        path
    );

    client.batch_execute(query.as_str()).await.unwrap();
}

pub async fn read_descriptions(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::get_queries_from("./sql/drug_mapping.sql")
        .unwrap()
        .queries;

    let client = pool.get().await?;
    execute("drop_rg_desc_map", &client, &queries).await;
    execute("rg_desc_map", &client, &queries).await;
    execute("drop_join_table_rg", &client, &queries).await;
    execute("create_join_table_rg", &client, &queries).await;
    execute("remove_non_alpha_numeric_rg", &client, &queries).await;
    execute("pre_and_append_spaces_rg", &client, &queries).await;

    let ttys = get_ttys();
    for tty in ttys {
        info!(
            "Going to check if descriptions contain any RxNorm {}",
            tty.1
        );
        let q = queries.get("find_rxconso_terms_for_tty").unwrap();
        let rxnorm = client.query(q.as_str(), &[&tty.0]).await.unwrap();
        let mapping = client
            .query(
                "SELECT DISTINCT id, description as clean
                                   FROM ctgov.rg_desc_mapping WHERE description IS NOT NULL",
                &[],
            )
            .await
            .unwrap();
        let mut insert_values: String = String::from("");
        let mut map: HashMap<String, String> = HashMap::new();

        do_matching(rxnorm, &mapping, &mut insert_values, &mut map);

        if map.len() == 0 {
            warn!("Did not find any matches, something is probably wrong");
            continue;
        }
        info!("Found {} matches, updating db", map.len());
        // removing trailing comma
        insert_values.pop();
        let q = format!(
            "INSERT INTO ctgov.description_mapping_rxcui(rg_desc_mapping_id,rxcui) VALUES {};",
            insert_values
        );
        client.execute(q.as_str(), &[]).await.unwrap();

        let mut update_queries: String = String::from("");
        for (ing, ids) in map {
            let query = format!(
                "UPDATE ctgov.rg_desc_mapping SET description = replace(description, '{}', '') WHERE id IN ({});",
                ing, ids
            );
            update_queries.push_str(&*query);
        }
        client.batch_execute(update_queries.as_str()).await.unwrap();
    }

    info!("Attaching all the found rxnorm concepts to result groups");
    execute("description_mapping_into_rg_rx_table", &client, &queries).await;
    execute("dg_rg_rx_table", &client, &queries).await;
    execute("i_rg_rx_table", &client, &queries).await;
    execute("io_rg_rx_table", &client, &queries).await;
    execute("i_over_dg_rg_rx_table", &client, &queries).await;
    execute("io_over_dg_rg_rx_table", &client, &queries).await;

    info!("Creating the final result group to rxcui table");
    execute("drop_final_rg_in_table", &client, &queries).await;
    execute("create_final_rg_in_table", &client, &queries).await;

    Ok(())
}

fn get_ttys() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PIN", "Precise Ingredients"),
        ("IN", "Ingredients"),
        ("BN", "Brand Names"),
    ]
}

async fn direct_match(queries: &HashMap<String, String>, pool: &Pool) {
    info!("Looking for matches...");
    let client = pool.get().await.unwrap();
    execute("direct_match_rxnconso", &client, &queries).await;
    execute("direct_match_art57", &client, &queries).await;
    // TODO: Check how much this would add in value
    //execute("synonyms", &client, &queries).await;
    execute("remove_matches", &client, &queries).await;
}

async fn match_words(pool: &Pool, tty: &&str, queries: &HashMap<String, String>) {
    let client = pool.get().await.unwrap();

    let q = queries.get("find_rxconso_terms_for_tty").unwrap();
    let rxnorm = client.query(q.as_str(), &[&tty]).await.unwrap();

    let q = queries.get("find_terms_to_map").unwrap();
    let mapping = client.query(q.as_str(), &[]).await.unwrap();

    // A long string with all the mapped values we will insert
    let mut insert_values: String = String::from("");
    // A map of terms and ids to use in the update statement
    let mut map: HashMap<String, String> = HashMap::new();

    do_matching(rxnorm, &mapping, &mut insert_values, &mut map);
    if map.len() == 0 {
        warn!("Did not find any matches, something is probably wrong");
        return;
    }
    info!("Found {} matches, updating db", map.len());

    // concat all the update statements to one long string which we send to the database in one go
    let mut update_queries: String = String::from("");
    for (ing, ids) in map {
        let query = format!(
            "UPDATE ctgov.drug_mapping SET clean = replace(clean, '{}', '') WHERE id IN ({});",
            ing, ids
        );
        update_queries.push_str(&*query);
    }

    // removing trailing comma
    insert_values.pop();
    let q = format!(
        "INSERT INTO ctgov.drug_mapping_rxcui(drug_mapping_id,rxcui) VALUES {};",
        insert_values
    );
    let results = tokio::join!(
        client.batch_execute(update_queries.as_str()),
        client.execute(q.as_str(), &[])
    );

    // Unwrap the query result in case an error occurred
    results.0.unwrap();
    results.1.unwrap();

    info!("Executed the update statements in the drug_mapping table");
}

fn do_matching(
    rxnorm: Vec<Row>,
    mapping: &Vec<Row>,
    insert_values: &mut String,
    map: &mut HashMap<String, String>,
) {
    let total: i64 = (rxnorm.len() * mapping.len()) as i64;
    info!("Making {} string comparisons ...", total);
    let mut pb = ProgressBar::new((total / 1000000) as u64);
    pb.set_width(Some(80));
    let mut counter: i64 = 0;
    for r in rxnorm {
        let drug: String = r.get("str");
        let drug_with_spaces = format!(" {} ", drug);
        for m in mapping {
            counter += 1;
            if counter % 1000000 == 0 {
                pb.inc();
            }
            let source: String = m.get("clean");
            if source.contains(&drug_with_spaces) {
                let id: i64 = m.get("id");
                let cui: i32 = r.get("rxcui");

                let value = format!("({},{}),", id, cui);
                insert_values.push_str(&*value);

                if map.contains_key(&drug) {
                    let mut addition: String = String::from(",");
                    addition.push_str(&*id.to_string());
                    map.get_mut(&drug).unwrap().push_str(&*addition);
                } else {
                    map.insert(drug.to_owned(), id.to_string());
                }
            }
        }
    }
    pb.finish();
    println!();
}
