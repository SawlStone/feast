# ClickHouse offline store (contrib)

## Description

The ClickHouse offline store provides support for reading [ClickHouseSources](../data-sources/clickhouse.md).
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. A Pandas dataframes will be uploaded to ClickHouse as a table in order to complete join operations.

## Disclaimer

The ClickHouse offline store does not achieve full test coverage.
Please do not assume complete stability.

## Getting started
In order to use this offline store, you'll need to run `pip install 'feast[clickhouse]'`. You can get started by then running `feast init -t clickhouse`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
  type: clickhouse
  host: DB_HOST
  port: DB_PORT
  database: DB_NAME
  user: DB_USERNAME
  password: DB_PASSWORD
online_store:
    path: data/online_store.db
```
{% endcode %}

The full set of configuration options is available in [ClickHouseOfflineStoreConfig](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse.ClickhouseOfflineStoreConfig).

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the ClickHouse offline store.

|                                                                    | Postgres |
| :----------------------------------------------------------------- |:---------|
| `get_historical_features` (point-in-time correct join)             | yes      |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes      |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes      |
| `offline_write_batch` (persist dataframes to offline store)        | yes      |
| `write_logged_features` (persist logged features to offline store) | no       |

Below is a matrix indicating which functionality is supported by `ClickHouseRetrievalJob`.

|                                                       | ClickHouse |
| ----------------------------------------------------- |------------|
| export to dataframe                                   | yes        |
| export to arrow table                                 | yes        |
| export to arrow batches                               | no         |
| export to SQL                                         | yes        |
| export to data lake (S3, GCS, etc.)                   | yes        |
| export to data dataframe                              | yes        |
| export as Spark dataframe                             | no         |
| local execution of Python-based on-demand transforms  | yes        |
| remote execution of Python-based on-demand transforms | no         |
| persist results in the offline store                  | yes        |
| preview the query plan before execution               | yes        |
| read partitioned data                                 | yes        |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).
