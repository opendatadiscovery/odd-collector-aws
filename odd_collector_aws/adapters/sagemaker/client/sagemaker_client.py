from typing import Generator, Any, Optional, List

import boto3

from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.utils import safe_list_get
from .client import Client
from ..domain.artifact import Artifact, create_artifact
from ..domain.experiment import Experiment
from ..domain.trial import Trial
from ..domain.trial_component import TrialComponent


class SagemakerClient(Client):
    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str
    ):
        super().__init__()
        boto3.setup_default_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        self.client = boto3.client("sagemaker")
        self.__account_id = boto3.client("sts").get_caller_identity()["Account"]

    @property
    def account_id(self):
        return self.__account_id

    def __get_search_expression(self, experiments_name: Optional[List[str]]):
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

    def get_experiments(
        self, experiments_name: Optional[List[str]]
    ) -> Generator[Experiment, Any, Any]:
        search_expression = self.__get_search_expression(experiments_name)

        pconf = PaginatorConfig(
            op_name="search",
            parameters={},
            list_fetch_key="Results",
            kwargs={"Resource": "Experiment", **search_expression},
        )

        for experiment in self.__fetch(pconf):
            experiment = experiment.get("Experiment")
            trials = self.__get_trials(experiment.get("ExperimentName"))

            yield Experiment.parse_obj({**experiment, "Trials": trials})

    def __get_trials(self, experiment_name: str):
        pconf = PaginatorConfig(
            op_name="list_trials",
            parameters={},
            list_fetch_key="TrialSummaries",
            kwargs={"ExperimentName": experiment_name},
        )

        for res in self.__fetch(pconf):
            trial_components = self.__get_trial_components(res.get("TrialName"))

            yield Trial.parse_obj({**res, "TrialComponents": trial_components})

    def __get_trial_component_description(
        self, trial_component_name: str
    ) -> TrialComponent:
        description = self.client.describe_trial_component(
            TrialComponentName=trial_component_name
        )

        inputs = description.get("InputArtifacts")
        outputs = description.get("OutputArtifacts")

        input_artifacts = [
            self.__to_artifact(name, data) for name, data in inputs.items()
        ]
        output_artifacts = [
            self.__to_artifact(name, data) for name, data in outputs.items()
        ]

        return TrialComponent.parse_obj(
            {
                **description,
                "InputArtifacts": input_artifacts,
                "OutputArtifacts": output_artifacts,
            }
        )

    def __get_trial_components(self, trial_name: str):
        pconf = PaginatorConfig(
            op_name="list_trial_components",
            parameters={},
            list_fetch_key="TrialComponentSummaries",
            kwargs={"TrialName": trial_name},
        )

        for res in self.__fetch(pconf):
            yield self.__get_trial_component_description(res.get("TrialComponentName"))

    def __to_artifact(self, name, data) -> Artifact:
        uri = data.get("Value")
        media_type = data.get("MediaType")

        if name == "SageMaker.ImageUri":
            return create_artifact(name=name, uri=uri)

        res = self.client.list_artifacts(SourceUri=uri).get("ArtifactSummaries")
        head = safe_list_get(res, 0)
        return create_artifact(
            name=name,
            uri=uri,
            media_type=media_type,
            arn=head.get("ArtifactArn") if head else None,
        )

    def __fetch(self, conf: PaginatorConfig):
        paginator = self.client.get_paginator(conf.op_name)

        return (
            item
            for res in paginator.paginate(**conf.kwargs)
            for item in res.get(conf.list_fetch_key)
        )
