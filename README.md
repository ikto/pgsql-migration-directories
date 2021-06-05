# PgSql Migration Directories

A connection adapter for [Pg Migration Directories](https://github.com/ikto/pg-migration-directories) based on **pgsql** PHP extension.

## Requirements (environment)

- PHP 7.1 or higher
- **pgsql** PHP extension
- [Pg Migration Directories](https://github.com/ikto/pg-migration-directories) library

## How to use

```php
use IKTO\PgSqlMigrationDirectories\Adapter\PgSqlConnectionAdapter;

// Connecting to the database.
$dbh = pg_connect('host=127.0.0.1 port=5432 dbname=pgi_test user=postgres password=postgres', PGSQL_CONNECT_FORCE_NEW);
$connection_adapter = new PgSqlConnectionAdapter($dbh);
// ... and the pass connection adapter to managed db object
```
