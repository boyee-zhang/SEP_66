# Release 450 (7 Jun 2024)

## General

* Add support for defining a number of rows to be pushed from a filter into
  values with the `optimizer.push-filter-into-values-max-row-count`
  configuration property. ({issue}`22105`)
* Improve performance of the {func}`first_value` and {func}`last_value`
  functions. ({issue}`22092`)
* Improve performance for large clusters under heavy workloads. ({issue}`22039`)
* Improve performance of queries with simple predicates. This optimization can
  be disabled using the `experimental.columnar-filter-evaluation.enabled`
  configuration property or the `columnar_filter_evaluation_enabled` session
  property. ({issue}`21375`)
* Fix failure when loading the [](/admin/event-listeners-openlineage). ({issue}`22228`)

## JDBC driver

* Add support for the `assumeNullCatalogMeansCurrent` connection property. When
  enabled, a `null` value for the `catalog` parameter in `DatabaseMetaData`
  methods is assumed to mean the current catalog. If no current catalog is
  set, the behaviour is unmodified. ({issue}`20866`)

## BigQuery connector

* Add support for name caching when the
  `bigquery.case-insensitive-name-matching` configuration property is enabled. ({issue}`10740`)

## Cassandra connector

* Fix incorrect behavior that uses one more partition than expected when
  specifying a value for the `cassandra.partition-size-for-batch-select`
  configuration property. ({issue}`21940`)

## Delta Lake connector

* Add support for concurrent `UPDATE`, `MERGE`, and `DELETE` queries. ({issue}`21727`)
* Fix failure when reading a `TIMESTAMP` value after the year 9999. ({issue}`22184`)

## Hive connector

* Add support for `integer` to `varchar` and `decimal` to `varchar` type 
  coercion in unpartitioned tables. ({issue}`22246`, {issue}`22293`)
* Add support for `double` to `varchar` type coercion in unpartitioned tables
  using Parquet files. ({issue}`22277`)
* Add support for `float` to `varchar` type coercion. ({issue}`22291`)

## MariaDB connector

* Improve performance of listing table columns. ({issue}`22241`)

## MySQL connector

* Improve performance of listing table columns. ({issue}`22241`)

## Pinot connector

* Add support for the
  [`enableNullHandling` query option](https://docs.pinot.apache.org/developers/advanced/null-value-support#advanced-null-handling-support). ({issue}`22214`)
* Fix failure when using dynamic tables. ({issue}`22301`)

## Redshift connector

* Improve performance of listing table columns. ({issue}`22241`)

## SingleStore connector

* Improve performance of listing table columns. ({issue}`22241`)
