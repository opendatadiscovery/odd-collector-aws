import datetime
import traceback as tb
from dataclasses import asdict, dataclass
from typing import Optional

from deltalake import DeltaTable
from funcy import complement, isnone, last, partial, select_values, silent, walk

from odd_collector_aws.domain.plugin import DeltaTableConfig, S3DeltaPlugin

from .logger import logger
from .models.table import DTable


def from_ms(ms) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


def add_utc_timezone(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(tzinfo=datetime.timezone.utc)


def handle_values(obj: dict, handler: tuple[str, callable]) -> dict[str, Optional[any]]:
    key, callback = handler
    return key, silent(callback)(obj.get(key))


@dataclass
class StorageOptions:
    DEFAULT_REGION = "us-east-1"

    aws_access_key_id: str = None
    aws_secret_access_key: str = None
    aws_region: str = None
    aws_session_token: str = None
    aws_storage_allow_http: bool = None
    endpoint_url: str = None

    @classmethod
    def from_config(cls, config: S3DeltaPlugin) -> "StorageOptions":
        return cls(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            aws_region=config.aws_region or cls.DEFAULT_REGION,
            aws_session_token=config.aws_session_token,
            aws_storage_allow_http="true" if config.aws_storage_allow_http else None,
            endpoint_url=config.endpoint_url,
        )

    def to_dict(self) -> dict[str, str]:
        return select_values(complement(isnone), asdict(self))


class DeltaClient:
    def __init__(self, config: S3DeltaPlugin) -> None:
        self.storage_options: StorageOptions = StorageOptions.from_config(config)

    def get_table(self, delta_table_config: DeltaTableConfig) -> DTable:
        # sourcery skip: raise-specific-error
        try:
            table = DeltaTable(
                delta_table_config.path, storage_options=self.storage_options.to_dict()
            )

            metadata = {}

            try:
                logger.debug(f"Getting actions list for {delta_table_config.path}")
                actions = table.get_add_actions(flatten=True).to_pydict()

                metadata |= walk(
                    partial(handle_values, actions),
                    {"size_bytes": sum, "num_records": sum, "modification_time": last},
                )
            except Exception as e:
                logger.error(
                    f"Failed to get actions list for {delta_table_config.path}"
                )

            try:
                logger.debug(f"Getting metadata for {delta_table_config.path}")
                delta_metadata = table.metadata()
                metadata |= {
                    "id": delta_metadata.id,
                    "name": delta_metadata.name,
                    "description": delta_metadata.description,
                    "partition_columns": ",".join(delta_metadata.partition_columns),
                    "configuration": delta_metadata.configuration,
                    "created_time": delta_metadata.created_time,
                }
            except Exception as e:
                logger.debug(tb.format_exc())
                logger.error(
                    f"Failed to get metadata for {delta_table_config.path}. {e}"
                )

            return DTable(
                table_uri=table.table_uri,
                schema=table.schema(),
                num_rows=metadata.get("num_records"),
                metadata=metadata,
                created_at=silent(from_ms)(metadata.get("created_time")),
                updated_at=silent(add_utc_timezone)(metadata.get("modification_time")),
            )
        except Exception as e:
            raise Exception(
                f"Failed to get delta table {delta_table_config.path}. {e}"
            ) from e
