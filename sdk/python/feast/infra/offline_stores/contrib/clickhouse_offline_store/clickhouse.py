import contextlib
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet
from pydantic.typing import Literal
from pytz import utc

from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import build_point_in_time_query
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.clickhouse.clickhouse_config import ClickHouseConfig
from feast.infra.utils.clickhouse.connection_utils import (
    clickhouse_connect,
    df_to_clickhouse_table,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage

from .clickhouse_source import ClickHouseSource, SavedDatasetClickHouseStorage


class ClickHouseOfflineStoreConfig(ClickHouseConfig):
    type: Literal[
        "offline_stores.clickhouse.ClickHouseOfflineStore"
    ] = "offline_stores.clickhouse.ClickHouseOfflineStore"


class ClickHouseOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="clickhouse")
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: "datetime",
        end_date: "datetime",
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, ClickHouseOfflineStoreConfig)
        assert isinstance(data_source, ClickHouseSource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(_append_alias(join_key_columns, "a"))
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        a_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "a")
        )
        b_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "b")
        )

        query = f"""
            SELECT
                {b_field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE a."{timestamp_field}"
                BETWEEN parseDateTime64BestEffortOrNull('{start_date}', {len(str(start_date.microsecond))}, '{str(start_date.tzinfo)}')
                AND parseDateTime64BestEffortOrNull('{end_date}', {len(str(start_date.microsecond))}, '{str(start_date.tzinfo)}')
            ) b
            WHERE _feast_row = 1
            """
        return ClickHouseRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="clickhouse")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: "datetime",
        end_date: "datetime",
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, ClickHouseOfflineStoreConfig)
        assert isinstance(data_source, ClickHouseSource)
        from_expression = data_source.get_table_query_string()

        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
                    SELECT {field_string}
                    FROM {from_expression} AS paftoq_alias
                    WHERE "{timestamp_field}"
                    BETWEEN parseDateTime64BestEffortOrNull('{start_date}', {len(str(start_date.microsecond))}, '{str(start_date.tzinfo)}')
                    AND parseDateTime64BestEffortOrNull('{end_date}', {len(str(start_date.microsecond))}, '{str(start_date.tzinfo)}')
                """

        return ClickHouseRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="clickhouse")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            _upload_entity_df(config, entity_df, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            # Build a query context containing all information
            # required to template the ClickHouse SQL query
            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            try:
                yield build_point_in_time_query(
                    query_context,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
            finally:
                if table_name:
                    with clickhouse_connect(config.offline_store) as client:
                        client.execute(f"DROP TABLE IF EXISTS {table_name}")

        return ClickHouseRetrievalJob(
            query=query_generator,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        """Optional method to have Feast support logging your online features."""
        # Implementation here.
        raise NotImplementedError()

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        """Optional method to have Feast support the offline push api for your offline store."""
        assert isinstance(config.offline_store, ClickHouseOfflineStoreConfig)
        assert isinstance(feature_view.batch_source, ClickHouseSource)

        pa_schema, column_names = offline_utils.get_pyarrow_schema_from_batch_source(
            config, feature_view.batch_source
        )
        if column_names != table.column_names:
            raise ValueError(
                f"The input pyarrow table has schema {table.schema} with the incorrect columns {table.column_names}. "
                f"The schema is expected to be {pa_schema} with the columns (in this exact order) to be {column_names}."
            )

        if table.schema != pa_schema:
            table = table.cast(pa_schema)

        table_name = feature_view.batch_source.table
        with clickhouse_connect(config.offline_store) as conn:
            conn.execute(f"INSERT INTO {table_name} VALUES", table.to_pylist())


class ClickHouseRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        assert isinstance(storage, SavedDatasetClickHouseStorage)

        df_to_clickhouse_table(
            config=self.config.offline_store,
            df=self.to_df(),
            table_name=storage.clickhouse_options._table,
        )

    def to_remote_storage(self) -> List[str]:
        pass

    def _to_df_internal(self) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        with self._query_generator() as query:
            with clickhouse_connect(self.config.offline_store) as client:
                df = client.query_dataframe(query)
                return df

    def to_sql(self) -> str:
        """Returns the underlying SQL query."""
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self) -> pa.Table:
        with self._query_generator() as query:
            with clickhouse_connect(self.config.offline_store) as client:
                data, columns = client.execute(
                    query,
                    columnar=True,
                    with_column_types=True,
                )
                fields = [i[0] for i in columns]
                if not data:
                    # if data is empty need to add emtpy lists
                    # to synchronize len of fields and len of arrays
                    data_transposed: List[List[Any]] = []
                    if not data:
                        for col in range(len(fields)):
                            data_transposed.append([])
                            for row in range(len(data)):
                                data_transposed[col].append(data[row][col])
                    data = data_transposed
                table = pa.Table.from_arrays(arrays=data, names=fields)
                return table


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        # get pandas dataframe consisting of 1 row (LIMIT 1) and generate the schema out of it
        entity_df_sample = ClickHouseRetrievalJob(
            f"SELECT * FROM ({entity_df}) LIMIT 1",
            config,
            full_feature_names=False,
        ).to_df()
        return dict(zip(entity_df_sample.columns, entity_df_sample.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


# leave here
def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        with clickhouse_connect(config.offline_store) as client:
            res: List[Tuple["datetime", "datetime"]] = client.execute(
                f"SELECT MIN({entity_df_event_timestamp_col}) AS min, "
                f"MAX({entity_df_event_timestamp_col}) AS max "
                f"FROM ({entity_df})  as tmp_alias"
            )
            entity_df_event_timestamp_range = res[0]
    else:
        raise InvalidEntityType(type(entity_df))
    return entity_df_event_timestamp_range


# leave here
def _upload_entity_df(
    config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Clickhouse
        df_to_clickhouse_table(config.offline_store, entity_df, table_name)
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query),
        # create a ClickHouse table out of it
        with clickhouse_connect(config) as client:
            client.execute(
                f"CREATE TABLE {table_name} AS ({entity_df}) engine = Memory;"
            )
    else:
        raise InvalidEntityType(type(entity_df))


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    return [f'{alias}."{field_name}"' for field_name in field_names]


# Copied from the Feast Redshift offline store implementation
# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:
# MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/redshift.py

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" as VARCHAR) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR)
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {% if featureview.entities %}"{{ featureview.entities | join('", "') }}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}"{{ featureview.entities | join('", "')}}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.timestamp_field }}" as event_timestamp,
        {{ '"' ~ featureview.created_timestamp_column ~ '" as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS sub
    WHERE "{{ featureview.timestamp_field }}" <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.timestamp_field }}" >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - {{ featureview.ttl }} + interval 1 second
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS subquery
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS entity_dataframe
    ON TRUE
        -- AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        -- AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} + interval 1 second
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery."{{ entity }}" = entity_dataframe."{{ entity }}"
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY "{{featureview.name}}__entity_row_unique_id"
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM "{{ featureview.name }}__base"
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup"
            USING ("{{featureview.name}}__entity_row_unique_id", event_timestamp, created_timestamp)
        {% endif %}
    ) AS sub
    WHERE row_number = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT base.*
    FROM "{{ featureview.name }}__base" as base
    INNER JOIN "{{ featureview.name }}__latest"
    USING(
        "{{featureview.name}}__entity_row_unique_id",
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT "{{ final_output_feature_names | join('", "')}}"
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,"{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"
        {% endfor %}
    FROM "{{ featureview.name }}__cleaned"
) AS "{{featureview.name}}" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
"""
