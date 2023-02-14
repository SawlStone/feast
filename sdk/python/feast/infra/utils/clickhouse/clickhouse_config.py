from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class ClickHouseConfig(FeastConfigBaseModel):
    user: StrictStr = "default"
    password: StrictStr = "12345"
    host: StrictStr = "127.0.0.1"
    port: int = 9000
    database: StrictStr
    # db_schema: StrictStr
