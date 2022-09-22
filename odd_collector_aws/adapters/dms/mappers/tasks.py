from odd_models.models import DataEntity, DataEntityType, DataTransformer
from typing import Dict, Any
from oddrn_generator.generators import DmsGenerator


def map_dms_task(
        raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    endpoints_arn_dict: Dict[str, DataEntity] = mapper_args["endpoints_arn_dict"]
    trans = DataTransformer(
        inputs=[endpoints_arn_dict.get(raw_job_data.get('SourceEndpointArn')).oddrn],

        outputs=[endpoints_arn_dict.get(raw_job_data.get('TargetEndpointArn')).oddrn],

    )
    data_entity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tasks", raw_job_data['ReplicationTaskIdentifier']),
        name=raw_job_data['ReplicationTaskIdentifier'],
        owner=None,
        type=DataEntityType.JOB,
    )
    data_entity.data_transformer = trans
    return data_entity
