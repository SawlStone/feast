import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.utils.clickhouse.connection_utils import clickhouse_connect
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import clickhouse_type_to_feast_value_type
from feast.value_type import ValueType


@typechecked
class ClickHouseSource(DataSource):
    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Create a ClickHouseSource from an existing table or query."""
        if table is None and query is None:
            raise ValueError('No "table" or "query" argument provided.')

        self._clickhouse_options = ClickHouseOptions(
            name=name, query=query, table=table
        )

        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, ClickHouseSource):
            raise TypeError(
                "Comparisons should only involve ClickHouseSource class objects."
            )

        return (
            super().__eq__(other)
            and self._clickhouse_options._query == other._clickhouse_options._query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table(self):
        return self.bigquery_options.table

    @property
    def query(self):
        return self.bigquery_options.query

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        clickhouse_options = json.loads(data_source.custom_options.configuration)

        return ClickHouseSource(
            name=clickhouse_options["name"],
            query=clickhouse_options["query"],
            table=clickhouse_options["table"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source.ClickHouseSource",
            field_mapping=self.field_mapping,
            custom_options=self._clickhouse_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
        )
        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self._clickhouse_options._table:
            return f"{self._clickhouse_options._table}"
        else:
            return f"({self._clickhouse_options._query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return clickhouse_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        with clickhouse_connect(config.offline_store) as client:
            query = f"""SELECT database, table, name, type
            FROM system.columns
            WHERE database = '{config.offline_store.database}'
            """
            data = client.execute(query)
            return ((i[2], i[3]) for i in data)


class ClickHouseOptions:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, clickhouse_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(clickhouse_options_proto.configuration.decode("utf8"))
        clickhouse_options = cls(
            name=config["name"], query=config["query"], table=config["table"]
        )

        return clickhouse_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        clickhouse_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query, "table": self._table}
            ).encode()
        )
        return clickhouse_options_proto


class SavedDatasetClickHouseStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    clickhouse_options: ClickHouseOptions

    def __init__(self, table_ref: str):
        self.clickhouse_options = ClickHouseOptions(
            table=table_ref, name=None, query=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetClickHouseStorage(
            table_ref=ClickHouseOptions.from_proto(storage_proto.custom_storage)._table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=self.clickhouse_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return ClickHouseSource(table=self.clickhouse_options._table)
