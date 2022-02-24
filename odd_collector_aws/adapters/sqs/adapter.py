import boto3
from botocore.exceptions import ClientError
from .sqs_generator import SqsGenerator
from odd_models.models import DataEntityList, DataEntity, DataEntityType
from typing import Any, Dict, List
from odd_collector_aws.domain.plugin import SQSPlugin



class SqsAdapter:
    def __init__(self, config: SQSPlugin) -> None:
        self._account_id = boto3.client("sts",
                                        aws_access_key_id=config.aws_access_key_id,
                                        aws_secret_access_key=config.aws_secret_access_key,
                                        region_name= config.aws_region
                                    ).get_caller_identity()["Account"]
        self._sqs_client = boto3.client("sqs",
                                        aws_access_key_id=config.aws_access_key_id,
                                        aws_secret_access_key=config.aws_secret_access_key,
                                        region_name= config.aws_region
                                    )
       
        self.__oddrn_generator = SqsGenerator(
            cloud_settings={"region": config.aws_region,
                            "account": self._account_id}
        )
    
    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def get_data_entities(self) -> List[DataEntity] :
        try:
            sqs_queues = []
            for queue in self._sqs_client.list_queues()['QueueUrls']:
                queue_name = queue.split('/')[-1]
                sqs_queues.append(
                    DataEntity(
                        name = queue_name,
                        oddrn=self.__oddrn_generator.get_oddrn_by_path('queue', queue_name),
                        type=DataEntityType.UNKNOWN

                    )
                )
        except ClientError:
            raise
        else:
            return sqs_queues

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()


