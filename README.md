# analytics_engineer_exercise
sample analytics engineering exercise using dbt
- using PostgreSQL database.
- seperate schemas for stage, raw_vault, and info_mart.
- each file will have models in stage and raw_vault.
- combination of files happens in the info_mart.

Current limitations (to be addressed in future iterations):
- create a business_vault layer for any SCD type tables, add effective_to logic for simpler querying in the info_mart layer.
- create a dynamic macro to convert all columns needed for the external_leads model. Possibly just have user define column mapping and let the macro do the rest. This should minimize effort of adding new sources. Also make macro transformations more robust to account for possible different data formats from different sources.
- turn info mart models into tables (incremental preferred).
- touch base with SME's about current column mappings.