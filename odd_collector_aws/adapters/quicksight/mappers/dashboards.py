from typing import Dict, Any

from odd_models.models import DataEntity, DataEntityType, DataConsumer

from . import metadata_extractor
from oddrn_generator import QuicksightGenerator


def map_quicksight_dashboard(
    raw_table_data: Dict[str, Any], account_id: str, region_name: str
) -> DataEntity:
    oddrn_gen = QuicksightGenerator(
        cloud_settings={"account": account_id, "region": region_name},
        dashboards=raw_table_data["Name"],
    )

    dataset_ids = [k.split("/")[-1] for k in raw_table_data["Version"]["DataSetArns"]]
    consumers = []
    for id in dataset_ids:
        oddrn_gen.set_oddrn_paths(datasets=id)
        consumers.append(oddrn_gen.get_oddrn_by_path("datasets"))

    return DataEntity(
        oddrn=oddrn_gen.get_oddrn_by_path("dashboards"),
        name=raw_table_data["Name"],
        type=DataEntityType.DASHBOARD,
        created_at=raw_table_data["CreatedTime"].isoformat(),
        updated_at=raw_table_data["LastPublishedTime"].isoformat(),
        metadata=[metadata_extractor.extract_dashboard_metadata(raw_table_data)],
        data_consumer=DataConsumer(inputs=consumers),
    )
