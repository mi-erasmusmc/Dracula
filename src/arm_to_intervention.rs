use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use deadpool_postgres::tokio_postgres::Row;
use deadpool_postgres::Pool;
use log::info;
use pbr::ProgressBar;
use rawsql::Loader;

use crate::db::execute;

pub async fn connect_arms_to_interventions(pool: &Pool) -> Result<(), Box<dyn Error>> {
    let cumulative_groups_names = vec![
        String::from("total"),
        String::from("all@patients"),
        String::from("overall participants"),
        String::from("all participants"),
    ];

    let client = pool.get().await?;
    let queries = Loader::read_queries_from("./sql/arms_to_interventions.sql").unwrap();

    execute("drop_table", &client, &queries).await;
    execute("create_table", &client, &queries).await;
    execute("drop_table_rg_int", &client, &queries).await;
    execute("create_table_result_group_intervention", &client, &queries).await;

    let q = queries.get("find_studies").unwrap();
    let result = client.query(q.as_str(), &[]).await?;
    let total_trials = result.len();
    info!("Processing {} Clinical Trials", total_trials);
    let mut pb = ProgressBar::new((total_trials / 1000) as u64);
    pb.set_width(Some(80));

    let studies: Vec<(String, Option<String>)> = result
        .iter()
        .map(|r| (r.get("nct_id"), r.get("model")))
        .collect();

    let q = queries.get("match").unwrap();
    let stmt = q.as_str();
    let mut counter: u16 = 0;

    for study_tup in studies {
        counter += 1;
        if counter % 1000 == 0 {
            pb.inc();
        }
        let study_id = study_tup.0;
        let study_model = study_tup.1.unwrap_or(String::from("Unkown"));

        let q = queries.get("find_result_groups").unwrap();
        let result = client.query(q.as_str(), &[&study_id]).await?;
        let result_groups: Vec<Group>;
        if result.len() > 1 {
            result_groups = result
                .iter()
                .map(|r| Group::from(r))
                .filter(|rg| {
                    !cumulative_groups_names.contains(
                        &rg.title
                            .as_ref()
                            .unwrap_or(&"no group title".to_string())
                            .to_lowercase(),
                    )
                })
                .collect();
        } else {
            result_groups = result.iter().map(|r| Group::from(r)).collect();
        }

        if study_model.eq_ignore_ascii_case("single group assignment") {
            attach_all_interventions(&pool, &queries, &study_id, &result_groups).await
        }

        let q = queries.get("find_design_groups").unwrap();
        let result = client.query(q.as_str(), &[&study_id]).await?;
        let design_groups: Vec<Group> = result.iter().map(|r| Group::from(r)).collect();

        if design_groups.len() == 0 {
            no_design_group(&pool, &queries, &stmt, &study_id, &result_groups)
                .await
                .unwrap();
        } else if design_groups.len() == 1 {
            let dg = design_groups.get(0).unwrap();
            for rg in result_groups {
                if !rg
                    .title
                    .as_ref()
                    .unwrap_or(&String::from("no title"))
                    .contains("placebo")
                {
                    update(
                        &pool,
                        &stmt,
                        &study_id,
                        &&rg,
                        &dg,
                        String::from("only one dg"),
                    )
                    .await;
                } else {
                    let dg = Group {
                        id: None,
                        title: None,
                        description: None,
                        intervention: None,
                    };
                    update(
                        &pool,
                        &stmt,
                        &study_id,
                        &&rg,
                        &dg,
                        String::from("only dg but this is placebo"),
                    )
                    .await;
                }
            }
        } else if result_groups.len() == design_groups.len() {
            rg_and_dg_of_equal_len(&pool, &stmt, &study_id, &design_groups, &result_groups).await?;
        } else if result_groups.len() < design_groups.len() {
            for rg in &result_groups {
                for dg in &design_groups {
                    if rg
                        .title
                        .as_ref()
                        .unwrap_or(&String::from("no title"))
                        .eq_ignore_ascii_case(
                            dg.title
                                .as_ref()
                                .unwrap_or(&String::from("no title"))
                                .as_str(),
                        )
                    {
                        update(
                            &pool,
                            &stmt,
                            &study_id,
                            &rg,
                            &dg,
                            String::from("direct hit fewer result groups"),
                        )
                        .await;
                    }
                }
            }
        } else {
            for rg in &result_groups {
                let mut found: u8 = 0;
                let rg_title = rg.title.clone().unwrap_or(String::from(""));
                for dg in &design_groups {
                    let dg_title = dg.title.clone().unwrap_or(String::from(""));
                    if dg_title.eq_ignore_ascii_case(&rg_title) {
                        continue;
                    }
                    if rg_title.contains(&dg_title) || dg_title.contains(&rg_title) {
                        found += 1;
                    }
                }
                if found != 1 {
                    let mut comparsions: BTreeMap<usize, &Group> = BTreeMap::new();
                    for dg in &design_groups {
                        let dgt = &dg
                            .title
                            .as_ref()
                            .unwrap_or(&String::from("no title"))
                            .clone();
                        let dg_title = &dgt
                            .replace("arm", "")
                            .replace("group", "")
                            .replace("ii", "2");
                        let rg_title = &rg
                            .title
                            .as_ref()
                            .unwrap_or(&String::from("no title"))
                            .clone()
                            .replace("arm", "")
                            .replace("group", "")
                            .replace("ii", "2");
                        let placebo = "placebo";
                        if (dg_title.contains(&placebo) && !rg_title.contains(&placebo))
                            || (rg_title.contains(&placebo)) && !dg_title.contains(&placebo)
                        {
                            continue;
                        }
                        let distance = distance::damerau_levenshtein(dg_title, rg_title);
                        if comparsions.is_empty()
                            || !comparsions.contains_key(&distance)
                            || comparsions.iter().next().unwrap().0 != &distance
                        {
                            comparsions.insert(distance, dg);
                        }
                    }
                    if !comparsions.is_empty() {
                        let dg = comparsions.iter().next().unwrap().1;
                        update(
                            &pool,
                            &stmt,
                            &study_id,
                            &rg,
                            &dg,
                            String::from("pattern match one to many"),
                        )
                        .await;
                    }
                }
            }
        }
    }
    pb.finish();
    println!();
    execute("populate_remaining", &client, &queries).await;
    Ok(())
}

async fn rg_and_dg_of_equal_len(
    pool: &Pool,
    stmt: &str,
    study_id: &String,
    design_groups: &Vec<Group>,
    result_groups: &Vec<Group>,
) -> Result<(), Box<dyn Error>> {
    for rg in result_groups {
        let mut found = false;
        for dg in design_groups {
            if rg
                .title
                .as_ref()
                .unwrap_or(&String::from("no title"))
                .eq_ignore_ascii_case(
                    dg.title
                        .as_ref()
                        .unwrap_or(&String::from("no title"))
                        .as_str(),
                )
                || rg
                    .title
                    .as_ref()
                    .unwrap_or(&String::from("No title"))
                    .eq_ignore_ascii_case(
                        dg.intervention
                            .as_ref()
                            .unwrap_or(&vec![Intervention {
                                id: 0,
                                name: String::from(""),
                            }])
                            .get(0)
                            .unwrap()
                            .name
                            .as_str(),
                    )
            {
                found = true;
                update(
                    &pool,
                    &stmt,
                    &study_id,
                    &rg,
                    &dg,
                    String::from("direct hit"),
                )
                .await;
            }
        }
        if !found {
            let mut matches: i8 = 0;
            let mut matching_dg: Option<&Group> = None;
            for dg in design_groups {
                if rg
                    .title
                    .as_ref()
                    .unwrap_or(&String::from("no title"))
                    .to_lowercase()
                    .contains(
                        &dg.title
                            .as_ref()
                            .unwrap_or(&String::from("no title"))
                            .to_lowercase(),
                    )
                    || dg
                        .title
                        .as_ref()
                        .unwrap_or(&String::from("no title"))
                        .to_lowercase()
                        .contains(
                            &rg.title
                                .as_ref()
                                .unwrap_or(&String::from("no title"))
                                .to_lowercase(),
                        )
                {
                    matches += 1;
                    matching_dg = Some(dg);
                }
            }
            if matches == 1 {
                found = true;
                update(
                    &pool,
                    &stmt,
                    &study_id,
                    &rg,
                    matching_dg.unwrap(),
                    String::from("direct hit"),
                )
                .await;
            }
        }

        if !found
            && rg
                .title
                .as_ref()
                .unwrap_or(&String::from("no title"))
                .chars()
                .count()
                > 2
        {
            let mut comparsions: BTreeMap<usize, &Group> = BTreeMap::new();
            for dg in design_groups {
                let dgt = &dg
                    .title
                    .as_ref()
                    .unwrap_or(&String::from("no title"))
                    .clone();
                if dgt.chars().count() > 2 {
                    let dg_title = &dgt.replace("arm", "").replace("group", "");
                    let rg_title = &rg
                        .title
                        .as_ref()
                        .unwrap_or(&String::from("no title"))
                        .clone()
                        .replace("arm", "")
                        .replace("group", "");
                    let distance = distance::damerau_levenshtein(dg_title, rg_title);
                    if distance < 7
                        && (comparsions.is_empty()
                            || !comparsions.contains_key(&distance)
                            || comparsions.iter().next().unwrap().0 != &distance)
                    {
                        comparsions.insert(distance, dg);
                    }
                }
            }
            if !comparsions.is_empty() {
                let dg = comparsions.iter().next().unwrap().1;
                update(
                    &pool,
                    &stmt,
                    &study_id,
                    &rg,
                    &dg,
                    String::from("pattern match"),
                )
                .await;
            }
        }
    }

    Ok(())
}

async fn attach_all_interventions(
    pool: &Pool,
    queries: &HashMap<String, String>,
    study: &String,
    rgs: &Vec<Group>,
) {
    let client = pool.get().await.unwrap();
    let q = queries.get("find_interventions").unwrap();
    let interventions = client.query(q.as_str(), &[&study]).await.unwrap();
    if !interventions.is_empty() {
        for rg in rgs {
            for i in &interventions {
                let id: i32 = i.get("id");
                client
                    .execute(
                        "INSERT INTO ctgov.result_group_intervention VALUES($1,$2)",
                        &[&rg.id, &id],
                    )
                    .await
                    .unwrap();
            }
        }
    }
}

async fn no_design_group(
    pool: &Pool,
    queries: &HashMap<String, String>,
    stmt: &str,
    study: &String,
    result_groups: &Vec<Group>,
) -> Result<(), Box<dyn Error>> {
    let client = pool.get().await.unwrap();
    let rg = result_groups.get(0).unwrap();
    let dg = Group {
        id: None,
        title: None,
        description: None,
        intervention: None,
    };
    update(
        &pool,
        &stmt,
        &study,
        &rg,
        &dg,
        String::from("no design groups"),
    )
    .await;

    if result_groups.len() == 1 {
        attach_all_interventions(&pool, &queries, &study, &result_groups).await;
        return Ok(());
    }
    let q = queries.get("find_interventions").unwrap();
    let interventions = client.query(q.as_str(), &[&study]).await?;
    if interventions.is_empty() {
        return Ok(());
    }
    if result_groups.len() > 1 {
        for rg in result_groups {
            let mut found = false;
            let rg_id = rg.id.clone().unwrap_or(0);
            let title = rg.title.clone().unwrap_or(String::from(""));
            let descr = rg.description.clone().unwrap_or(String::from(""));
            let placebo = String::from("placebo");
            for i in &interventions {
                let name: String = i.get("name");
                let i_id: i32 = i.get("id");
                if title.contains(&name) || descr.contains(&name) {
                    found = true;
                    client
                        .execute(
                            "INSERT INTO ctgov.result_group_intervention VALUES($1,$2)",
                            &[&rg_id, &i_id],
                        )
                        .await
                        .unwrap();
                }
            }
            if !found {
                if interventions.len() == 1 && !title.contains(&placebo) {
                    let i_id: i32 = interventions.get(0).unwrap().get("id");
                    client
                        .execute(
                            "INSERT INTO ctgov.result_group_intervention VALUES($1,$2)",
                            &[&rg_id, &i_id],
                        )
                        .await
                        .unwrap();
                }
            }
        }
    }
    Ok(())
}

async fn update(pool: &Pool, stmt: &str, study: &String, rg: &&Group, dg: &Group, method: String) {
    let client = pool.get().await.unwrap();
    client
        .execute(
            stmt,
            &[
                &study,
                &dg.id,
                &dg.title,
                &dg.description,
                &rg.id,
                &rg.title,
                &rg.description,
                &method,
            ],
        )
        .await
        .unwrap();
    match &dg.intervention {
        None => {}
        Some(ins) => {
            let mut insert_values: String = String::from("");
            for i in ins {
                let value = format!("({},{}),", rg.id.unwrap(), i.id);
                insert_values.push_str(&*value);
            }
            // removing trailing comma
            insert_values.pop();
            let q = format!(
                "INSERT INTO ctgov.result_group_intervention(rg_id,intervention_id) VALUES {};",
                insert_values
            );
            client.execute(q.as_str(), &[]).await.unwrap();
        }
    }
}

#[derive(Debug)]
struct Intervention {
    id: i32,
    name: String,
}

impl From<&str> for Intervention {
    fn from(str: &str) -> Self {
        let parts: Vec<&str> = str.split(" || ").collect();
        Self {
            id: parts.get(1).unwrap_or(&"0").parse().unwrap_or(0),
            name: parts.get(0).unwrap_or(&"").to_string(),
        }
    }
}

struct Group {
    id: Option<i32>,
    title: Option<String>,
    description: Option<String>,
    intervention: Option<Vec<Intervention>>,
}

impl From<&Row> for Group {
    fn from(row: &Row) -> Self {
        let opt: Option<String> = row.get("interventions");
        let interventions = match opt {
            None => None,
            Some(o) => {
                let i: Vec<&str> = o.split(" ||| ").collect();
                // Due to the way we query for the interventions we need to do this 'empty' check
                let check: Vec<&str> = i.get(0).unwrap().split(" || ").collect();
                if check.get(0).unwrap().is_empty() {
                    None
                } else {
                    let interventions = i.iter().map(|i| Intervention::from(i.clone())).collect();
                    Some(interventions)
                }
            }
        };
        Self {
            id: row.get("id"),
            title: row.get("title"),
            description: row.get("description"),
            intervention: interventions,
        }
    }
}
