# mysql-migrator
MySQL to Postgres migration script

# Usage

```bash
./migrate.sh \
    --mysql user:pass@127.0.0.1:3306/db_name \
    --postgres user:pass@127.0.0.1:5432/dst_db_name \
    --work-dir $(pwd)/wdir
```

Script creates working directory with the following content:
```
    .
    ├── pgloader                 # Contains everything needed for schema migration
    │   ├── run.sh               # PgLoader startup script. Could be used to run schema migration
    │   ├── migrate.load         # PgLoader migration script. Used by PgLoader during migration
    │   ├── pgloader.log         # PgLoader log file. Could be used to understand what's happend during schema migration
    │   └── pgloader_summary.log # PgLoader summary log file. Contains migration statistics
    │
    └── conduit                  # Contains everything needed for data migration
        ├── badger.db            # Conduit stores current state in database. This folder contains badger built-in database data
        ├── conduit.yaml         # Conduit main configuration script
        ├── connectors           # Directory with third-party connectors. If you're using custom conduit
        │                        #   from this repository, this folder could be empty
        ├── processors           # Directory with third-party processors. If you're using custom conduit
        │                        #   from this repository, this folder could be empty
        ├── pipelines            # Directory with conduit pipeline configuration. Used by conduit during migration
        └── run.sh               # Conduit startup script. Could be used to run data migration
```

Tool requires mysql utility to connect to mysql datasource.
That's required to list and validate mysql schema before migration.
No modifications will be made.

# Requirements

1. You need to install PgLoader manually: https://pgloader.readthedocs.io/en/latest/ref/mysql.html
2. MySQL Server requirements ([source](https://github.com/conduitio-labs/conduit-connector-mysql?tab=readme-ov-file#requirements-and-compatibility)):
   * Binary Log (binlog) must be enabled
   * Binlog format must be set to ROW
   * Binlog row image must be set to FULL
   * Tables must have sortable primary keys
3. For Snapshot and CDC Conduit modes, the following privileges are required on MySQL user:
   * SELECT
   * LOCK TABLES
   * RELOAD
   * REPLICATION CLIENT
   * REPLICATION SLAVE
4. All triggers should be disabled in Postgres database during snapshot migration.
   That's required because Conduit transfers data in large batches, without regard to indexes.
   Indexes could be enabled after CDC migration startup.
   See Conduit MySQL connector [documentation](https://github.com/conduitio-labs/conduit-connector-mysql?tab=readme-ov-file#source) for more info.

# Migration script options

TODO
