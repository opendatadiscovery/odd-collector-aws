from oddrn_generator.generators import MssqlGenerator
from typing import Dict, Any, Union
from oddrn_generator.server_models import AWSCloudModel, AbstractServerModel


def get_endpoint_oddrn(endpoint_node: Dict[str, Any], server_obj: Union[AWSCloudModel, AbstractServerModel]) -> str:
    engine_name = endpoint_node.get('EngineName')
    if engine_name == 'sqlserver':
        stats: dict = endpoint_node.get('MicrosoftSQLServerSettings')
        gen = MssqlGenerator(
            host_settings=f"{stats['ServerName']}:{stats['Port']}", databases=stats['DatabaseName']
        )
        return gen.get_data_source_oddrn()
    elif engine_name == 's3':
        stats: dict = endpoint_node.get('S3Settings')
        return (
            "//s3/cloud/aws"
            f"/account/{server_obj.account}"
            f"/region/{server_obj.region}"
            f"/bucket/{stats.get('BucketName')}"
            f"/folder/{stats.get('BucketFolder')}"
        )
    else:
        return 'UNKNOWN'
