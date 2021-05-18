Dracula - A Clinical Trials Drug - Adverse Event Mapper
=======================

**Dracula is a tool to be used in combination with the [CTTI AACT database](https://aact.ctti-clinicaltrials.org/). It
does two things: First, it generates an additional table called 'rg_intervention' that links the reported adverse events
from the 'result_groups' table to an RxNorm ID of the drugs provided to the specific group for which this adverse event
occurred. Second it adds an additional pt_code column to the reported_events table containing the MedDRA preferred term
code for the specific adverse event.**

Note:
The master branch of this project should work, but the project as whole is still under construction. Although a decent
effort is made to make this mapping decent, Dracula is not perfect. Contributions, comments and suggestions are very
much welcome.

### REQUIREMENTS

- Dracula builds on the [CTTI AACT database](https://aact.ctti-clinicaltrials.org/) system, you will need to download and
  start that database before doing this :-)

- You will need the RxNORM database, in particular both the RXNCONSO table and the RXNREL table. The RxNorm data is
  available
  [here](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html) (db is free but registration is required).
  The app expects the two tables to be located within the aact ctgov schema.

- If you also to wish to perform additional standardization on the outcome side you require the MedDRA db. Some effort
  is required to obtain the MedDRA db so this step is optional. If you do have the MedDRA db be sure it is in the same
  db in a schema named 'meddra', we will be using the 'medhier' and 'llt' tables.

- To build and run the app you will need [Rust](www.rust-lang.org)

- This app has been developed and tested using MacOS Big Sur, PostgreSQL version 13.1, and Rust 1.49.0.

### HOW TO RUN

- Make sure the db is configured in accordance to your needs in the Settings.toml file.

- Use `cargo run --release` in the present folder to build and run the app.

- Running time is approximately 1 hour.

### OPTIONS

There are two extra settings that you can configure in the `Settings.toml` file.

1. *skip_meddra* If you want to skip the MedDRA step set this to true.
2. *meddra_precision* The meddra normalization relies on some fuzzy matching. Tell Dracula how fuzzy you would like to
   accept the results on a scale of 1-5 with 5 being super wild (wilder is also a bit slower).

#### TODO:

- This and that :-)


  