Dracula - A Clinical Trials Drug - Adverse Event Mapper
=======================

**Dracula is a tool to be used in combination with the [CTTI AACT database](https://aact.ctti-clinicaltrials.org/). It
does two things: First, it generates an additional table called `result_group_ingredient` that links the reported
adverse events from the `result_groups` table to a standardized drug identifier for the drugs provided to the specific
group in which this adverse event occurred. Second, it adds a `pt_code` column to the `reported_events` table
containing the MedDRA preferred term code for each adverse event.**

Note: Although a decent effort is made to produce accurate mappings, Dracula is not perfect. Contributions, comments
and suggestions are very much welcome.

### REQUIREMENTS

- Dracula builds on the [CTTI AACT database](https://aact.ctti-clinicaltrials.org/). You will need to download
  and start that database before running this tool.

- You will need either RxNorm or ChEMBL as your drug reference database (configured via `drug_ref` in `Settings.toml`):

  - **RxNorm**: you need the RXNCONSO and RXNREL tables, available
    [here](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html) (free but registration required).
    The app expects these tables in a schema called `rxnorm`. The `result_group_ingredient` table will contain
    RxNorm CUIs.

  - **ChEMBL**: you need the ChEMBL database, available
    [here](https://www.ebi.ac.uk/chembl/). The app expects the `molecule_dictionary`, `compound_structures`,
    and `molecule_synonyms` tables in a schema called `chembl`. The `result_group_ingredient` table will contain
    ChEMBL `molregno` values along with SMILES and InChI keys where available.

- Optionally, for standardization of adverse event terms, you will need the MedDRA database. This step can be skipped
  if you do not have access. If you do have the MedDRA db, place it in a schema named `meddra` — the `medhier`
  and `llt` tables are used.

- To build and run the app you will need [Rust](https://www.rust-lang.org).

- This app has been developed and tested using macOS, Postgres versions 13–14, and Rust versions 1.49–1.82.

### HOW TO RUN

1. Configure your database connection and options in `Settings.toml`.
2. Run `cargo run --release` from the project root.
3. Running time is approximately 1 hour.

### OPTIONS

All options are set in `Settings.toml`.

| Setting | Description |
|---|---|
| `pg_host` | Postgres host |
| `pg_port` | Postgres port |
| `pg_user` | Postgres user |
| `pg_password` | Postgres password |
| `pg_dbname` | Name of the AACT database |
| `drug_ref` | Drug reference database: `rxnorm` or `chembl` |
| `skip_meddra` | Set to `true` to skip the MedDRA adverse event standardization step |
| `meddra_precision` | Fuzziness of MedDRA term matching, 1 (strict) to 5 (loose). Only used when `skip_meddra` is `false` |
