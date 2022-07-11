from typing import Optional, List, Iterable
from funcy import lmapcat, lflatten

from odd_collector_aws.adapters.s3.clients.s3_client import S3Client
from odd_collector_aws.adapters.s3.s3_dataset_service import S3DatasetService
from odd_collector_aws.adapters.sagemaker.client.client import Client
from odd_collector_aws.adapters.sagemaker.domain import Association
from odd_collector_aws.adapters.sagemaker.domain import Experiment
from odd_collector_aws.adapters.sagemaker.domain import Trial
from odd_collector_aws.adapters.sagemaker.domain import TrialComponent
from odd_collector_aws.adapters.sagemaker.domain.artifact import (
    create_image,
    create_dummy_dataset_artifact,
    create_model,
)
from odd_collector_aws.adapters.sagemaker.logger import logger
from odd_collector_aws.aws.aws_client import AwsClient
from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.domain.plugin import SagemakerPlugin, DatasetConfig
from odd_collector_aws.domain.to_data_entity import ToDataEntity
from odd_collector_aws.errors import InvalidFileFormatWarning
from odd_collector_aws.utils.parse_s3_url import parse_s3_url


class SagemakerClient(Client):
    def __init__(self, config: SagemakerPlugin):
        super().__init__()

        self.s3_dataset_client = S3DatasetService(S3Client(config))

        aws_client = AwsClient(config)
        self.client = aws_client.get_client("sagemaker")
        self.account_id = aws_client.get_account_id()

    def get_experiments(
        self, experiments_name: Optional[List[str]]
    ) -> Iterable[Experiment]:
        pconf = PaginatorConfig(
            op_name="search",
            list_fetch_key="Results",
            kwargs={
                "Resource": "Experiment",
                **self.__get_search_expression(experiments_name),
            },
        )

        for experiment in self._fetch(pconf):
            experiment = experiment.get("Experiment")
            trials = self.get_trials(experiment.get("ExperimentName"))

            yield Experiment.parse_obj({**experiment, "Trials": trials})

    def get_trials(self, experiment_name: str):
        pconf = PaginatorConfig(
            op_name="list_trials",
            list_fetch_key="TrialSummaries",
            kwargs={"ExperimentName": experiment_name},
        )

        for res in self._fetch(pconf):
            trial_components = self.get_trial_components(res.get("TrialName"))

            yield Trial.parse_obj({**res, "TrialComponents": trial_components})

    def get_trial_components(self, trial_name: str):
        pconf = PaginatorConfig(
            op_name="list_trial_components",
            parameters={},
            list_fetch_key="TrialComponentSummaries",
            kwargs={"TrialName": trial_name},
        )

        for res in self._fetch(pconf):
            yield self.get_trial_component_description(res.get("TrialComponentName"))

    def get_trial_component_description(
        self, trial_component_name: str
    ) -> TrialComponent:
        description = self.client.describe_trial_component(
            TrialComponentName=trial_component_name
        )

        arn = description.get("TrialComponentArn")
        return TrialComponent.parse_obj(
            {
                **description,
                "InputArtifacts": self._get_input_artifacts(arn),
                "OutputArtifacts": self._get_output_artifacts(arn),
            }
        )

    def _is_artifact(self, entity_type: str):
        return entity_type == "artifact"

    def _to_artifact(self, arn: str, association_type: str) -> List[ToDataEntity]:
        arn_name = arn.split(":")[5]
        entity_type = arn_name.split("/")[0]

        if not self._is_artifact(entity_type):
            return []

        artifact = self._describe_artifact(arn)
        s3_url: str = artifact["Source"]["SourceUri"]

        if association_type == "DataSet":
            try:
                bucket, path = parse_s3_url(s3_url)

                if path.endswith(".py"):
                    return []

                ds_config = DatasetConfig(bucket=bucket, path=path)

                return self.s3_dataset_client.get_datasets(ds_config)
            except InvalidFileFormatWarning as e:
                logger.error(e)
                return []
            except Exception as e:
                logger.error(e)
                return [
                    create_dummy_dataset_artifact(
                        uri=s3_url, arn=artifact["ArtifactArn"]
                    )
                ]
        if association_type == "Image":
            return [create_image(uri=s3_url)]
        if association_type == "Model":
            return [create_model(uri=s3_url, arn=artifact["ArtifactArn"])]

        return []

    def _describe_artifact(self, arn: str):
        try:
            return self.client.describe_artifact(ArtifactArn=arn)
        except Exception as e:
            logger.warning(e, exc_info=True)

    def _get_input_artifacts(self, arn: str):
        return lflatten(
            self._to_artifact(assoc.source_arn, assoc.source_type)
            for assoc in self._get_associations(dest_arn=arn)
        )

    def _get_output_artifacts(self, arn: str):
        return lflatten(
            self._to_artifact(assoc.destination_arn, assoc.destination_type)
            for assoc in self._get_associations(src_arn=arn)
        )

    def _get_associations(
        self, src_arn: Optional[str] = None, dest_arn: Optional[str] = None
    ) -> List[Association]:
        kwargs = {"SourceArn": src_arn} if src_arn else {"DestinationArn": dest_arn}
        pconf = PaginatorConfig(
            op_name="list_associations",
            parameters={},
            list_fetch_key="AssociationSummaries",
            kwargs=kwargs,
        )
        return lmapcat(Association.parse_obj, self._fetch(pconf))

    def _fetch(self, conf: PaginatorConfig):
        paginator = self.client.get_paginator(conf.op_name)

        for res in paginator.paginate(**conf.kwargs):
            yield from res.get(conf.list_fetch_key)

    @staticmethod
    def __get_search_expression(experiments_name: Optional[List[str]]):
        if experiments_name:
            return {
                "SearchExpression": {
                    "Filters": [
                        {
                            "Name": "ExperimentName",
                            "Operator": "In",
                            "Value": ",".join(experiments_name),
                        }
                    ]
                }
            }

        return {}
