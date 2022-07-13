from typing import List, Dict, Optional

from odd_models.models import DataEntity
from oddrn_generator.generators import S3Generator, SagemakerGenerator

from odd_collector_aws.adapters.s3.domain.dataset import S3Dataset
from odd_collector_aws.adapters.sagemaker.domain.artifact import (
    Artifact,
    as_input,
    as_output,
    DummyDatasetArtifact,
)
from odd_collector_aws.adapters.sagemaker.domain.experiment import Experiment
from odd_collector_aws.adapters.sagemaker.domain.trial import Trial
from odd_collector_aws.adapters.sagemaker.domain.trial_component import (
    TrialComponent,
    add_input,
    add_output,
)
from odd_collector_aws.errors import MappingError
from odd_collector_aws.use_cases.s3_use_case import S3UseCase
from odd_collector_aws.utils import parse_s3_url


# TODO: Flyweight pattern
class DataEntityCache(object):
    def __init__(self):
        self.entities: Dict[str, DataEntity] = {}

    def get(self, arn: str) -> DataEntity:
        return self.entities.get(arn)

    def save(self, arn: str, data_entity) -> None:
        self.entities[arn] = data_entity

    def get_all(self) -> List[DataEntity]:
        return list(self.entities.values())


class ExperimentMapper:
    cache: Dict[str, DataEntity]
    experiment_data_entity: DataEntity = None

    def __init__(
        self,
        oddrn_gen: SagemakerGenerator,
        s3_oddrn_gen: S3Generator,
        s3_use_case: S3UseCase,
        cache: Optional[DataEntityCache] = None,
    ):
        self._repo = cache or DataEntityCache()

        self._oddrn_gen = oddrn_gen
        self._s3_use_case = s3_use_case
        self._s3_oddrn_gen = s3_oddrn_gen

    def map_experiment(self, experiment: Experiment) -> List[DataEntity]:
        # Set oddrn context by experiment name
        try:
            self._oddrn_gen.set_oddrn_paths(experiments=experiment.experiment_name)

            # Create experiment data entity
            experiment_de = experiment.to_data_entity(self._oddrn_gen)

            self._repo.save(experiment.arn, experiment_de)

            # Map experiment trials
            for trial in experiment.trials:
                self.map_trial(trial, experiment_de)

            return list(self._repo.get_all())
        except Exception as e:
            raise MappingError from e

    def map_trial(self, trial: Trial, experiment_data_entity: DataEntity) -> None:
        # Set oddrn context by trial name
        self._oddrn_gen.set_oddrn_paths(trials=trial.trial_name)

        # Transform to DataEntity
        trial_data_entity = trial.to_data_entity(self._oddrn_gen)

        self._repo.save(trial.arn, trial_data_entity)

        for trial_component in trial.trial_components:
            # get trial component DataEntity and Artifacts DataEntities chained with him
            self.map_trial_component(trial_component, trial_data_entity)

        experiment_data_entity.data_entity_group.entities_list.append(
            trial_data_entity.oddrn
        )

    def map_trial_component(
        self, trial_component: TrialComponent, trial_data_entity: DataEntity
    ) -> None:
        """
        Creates DataEntity for TrialComponent and self Input/Output Artifacts.
        Returns them as list of DataEntities
        """

        # Set oddrn context by TrialComponent(Job) nane
        self._oddrn_gen.set_oddrn_paths(jobs=trial_component.trial_component_name)

        data_entity = trial_component.to_data_entity(self._oddrn_gen, [], [])
        self._repo.save(trial_component.arn, data_entity)

        for artifact in trial_component.input_artifacts:
            self.map_input(trial_component.arn, artifact)

        for artifact in trial_component.output_artifacts:
            self.map_output(trial_component.trial_component_arn, artifact)

    def map_input(self, trial_component_arn: str, artifact: Artifact) -> None:
        trial_component_data_entity = self._repo.get(trial_component_arn)
        input_data_entity = self._handle_artifact(
            artifact, trial_component_data_entity.oddrn
        )

        add_input(trial_component_data_entity, input_data_entity.oddrn)
        as_input(input_data_entity, trial_component_data_entity.oddrn)

    def map_output(
        self,
        trial_component_arn: str,
        artifact: Artifact,
    ) -> None:
        """
        Maps TrialComponent's output Artifacts
        """
        trial_component_data_entity = self._repo.get(trial_component_arn)
        output_data_entity = self._handle_artifact(
            artifact, trial_component_data_entity.oddrn
        )

        add_output(trial_component_data_entity, output_data_entity.oddrn)
        as_output(output_data_entity, trial_component_data_entity.oddrn)

    def _handle_artifact(self, artifact, trial_component_arn) -> DataEntity:
        if de := self._repo.get(artifact.arn):
            data_entity = de
        else:
            if isinstance(artifact, S3Dataset):
                self._s3_oddrn_gen.set_oddrn_paths(buckets=artifact.bucket)
                data_entity = artifact.to_data_entity(self._s3_oddrn_gen)
            elif isinstance(artifact, DummyDatasetArtifact):
                bucket, key = parse_s3_url(artifact.uri)
                self._s3_oddrn_gen.set_oddrn_paths(buckets=bucket, keys=key)
                data_entity = artifact.to_data_entity(self._s3_oddrn_gen)
            else:
                data_entity = artifact.to_data_entity(
                    self._oddrn_gen, trial_component_arn
                )
            self._repo.save(artifact.arn, data_entity)
        return data_entity
