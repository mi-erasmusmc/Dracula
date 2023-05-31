use std::collections::HashMap;
use std::error::Error;

use deadpool::managed::Object;
use deadpool_postgres::tokio_postgres::Row;
use deadpool_postgres::Manager;
use deadpool_postgres::Pool;
use log::info;
use rawsql::Loader;

use crate::db::execute;

pub async fn find_drugs(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::read_queries_from("./sql/drug_mapping.sql").unwrap();

    let client = pool.get().await?;

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

    match_words(&pool, &queries).await;

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

pub async fn read_descriptions(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let queries = Loader::read_queries_from("./sql/drug_mapping.sql").unwrap();

    let client = pool.get().await?;
    execute("drop_rg_desc_map", &client, &queries).await;
    execute("rg_desc_map", &client, &queries).await;
    execute("drop_join_table_rg", &client, &queries).await;
    execute("create_join_table_rg", &client, &queries).await;
    execute("remove_non_alpha_numeric_rg", &client, &queries).await;
    execute("pre_and_append_spaces_rg", &client, &queries).await;

    let chembl_drugs: Vec<Row> = all_chembl(&client, &queries).await;
    let mut reload = true;
    let mut mapping = client
        .query("SELECT DISTINCT id, description as clean FROM ctgov.rg_desc_mapping WHERE description IS NOT NULL", &[])
        .await
        .unwrap();

    for drug in chembl_drugs {
        if reload {
            mapping = client
                .query("SELECT DISTINCT id, description as clean FROM ctgov.rg_desc_mapping WHERE description IS NOT NULL", &[])
                .await
                .unwrap();
            reload = false;
        }

        let mut insert_values: String = String::from("");
        let mut map: HashMap<String, String> = HashMap::new();

        do_matching(drug, &mapping, &mut insert_values, &mut map);

        if map.len() == 0 {
            continue;
        }
        info!("Found matches, updating db");
        reload = true;
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

    info!("Attaching all the found concepts to result groups");
    execute("description_mapping_into_rg_rx_table", &client, &queries).await;
    execute("dg_rg_rx_table", &client, &queries).await;
    execute("i_rg_rx_table", &client, &queries).await;
    execute("io_rg_rx_table", &client, &queries).await;
    execute("i_over_dg_rg_rx_table", &client, &queries).await;
    execute("io_over_dg_rg_rx_table", &client, &queries).await;

    info!("Creating the final result group to rxcui table");
    execute("drop_final_rg_in_table", &client, &queries).await;
    execute("create_final_rg_in_table", &client, &queries).await;
    execute("get_dose_1", &client, &queries).await;

    Ok(())
}

async fn direct_match(queries: &HashMap<String, String>, pool: &Pool) {
    info!("Looking for direct matches...");
    let client = pool.get().await.unwrap();
    execute("direct_match_chembl", &client, &queries).await;
    execute("remove_matches", &client, &queries).await;
}

async fn match_words(pool: &Pool, queries: &HashMap<String, String>) {
    let client = pool.get().await.unwrap();
    let chembl_drugs = all_chembl(&client, &queries).await;
    let mut reload = true;
    let mut q = queries.get("find_terms_to_map").unwrap();
    let mut mapping = client.query(q.as_str(), &[]).await.unwrap();

    for drug in chembl_drugs {
        if reload {
            q = queries.get("find_terms_to_map").unwrap();
            mapping = client.query(q.as_str(), &[]).await.unwrap();
            reload = false;
        }

        // A long string with all the mapped values we will insert
        let mut insert_values: String = String::from("");
        // A map of terms and ids to use in the update statement
        let mut map: HashMap<String, String> = HashMap::new();

        do_matching(drug, &mapping, &mut insert_values, &mut map);
        if map.len() == 0 {
            continue;
        }
        info!("Found matches, updating db");
        reload = true;

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
}

async fn all_chembl(client: &Object<Manager>, queries: &HashMap<String, String>) -> Vec<Row> {
    let q = queries.get("all_chembl").unwrap();
    client.query(q.as_str(), &[]).await.unwrap()
}

fn do_matching(
    c: Row,
    mapping: &Vec<Row>,
    insert_values: &mut String,
    map: &mut HashMap<String, String>,
) {
    let drug: String = c.get("synonyms");
    let drug_with_spaces = format!(" {} ", drug);
    info!("{}", drug_with_spaces);
    for m in mapping {
        let source: String = m.get("clean");
        if source.contains(&drug_with_spaces) {
            let id: i64 = m.get("id");
            let cui: i64 = c.get("molregno");

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
