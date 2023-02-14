import logging
from typing import Dict

import pandas as pd
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse import (
    ClickHouseOfflineStoreConfig,
    ClickHouseSource,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

logger = logging.getLogger(__name__)


CLICKHOUSE_USER = "test"
CLICKHOUSE_PASSWORD = "test"
CLICKHOUSE_DB = "test"


@pytest.fixture(scope="session")
def clickhouse_container():
    container = (
        DockerContainer("clickhouse/clickhouse-server:latest")
        .with_exposed_ports(9000)
        .with_env("CLICKHOUSE_USER", CLICKHOUSE_USER)
        .with_env("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .with_env("CLICKHOUSE_DB", CLICKHOUSE_DB)
    )

    container.start()

    log_string_to_wait_for = "database system is ready to accept connections"
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=30,
        interval=10,
    )
    logger.info("Waited for %s seconds until postgres container was up", waited)

    yield container
    container.stop()


class ClickHouseDataSourceCreator(DataSourceCreator):
    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(
            project_name,
        )

        self.project_name = project_name
        self.container = fixture_request.getfixturevalue("clickhouse_container")
        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.clickhouse_offline_store.tests' "
                "must be include into pytest plugins"
            )

        self.offline_store_config = ClickHouseOfflineStoreConfig(
            type="clickhouse",
            host="localhost",
            port=self.container.get_exposed_port(9000),
            database=self.container.env["CLICKHOUSE_DB"],
            db_schema="public",
            user=self.container.env["CLICKHOUSE_USER"],
            password=self.container.env["CLICKHOUSE_PASSWORD"],
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
        **kwargs,
    ) -> DataSource:
        destination_name = self.get_prefixed_table_name(destination_name)
        return ClickHouseSource(
            name=destination_name,
            query=f"SELECT * FROM {destination_name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_offline_store_config(self) -> ClickHouseOfflineStoreConfig:
        assert self.offline_store_config
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_saved_dataset_destination(self):
        # FIXME: ...
        return None

    def teardown(self):
        ...
