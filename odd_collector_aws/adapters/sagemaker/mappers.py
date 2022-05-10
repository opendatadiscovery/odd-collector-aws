from functools import partial
from typing import List, Dict, Type
from xmlrpc.client import Boolean

from odd_models.models import DataEntity, DataEntityType
from oddrn_generator.generators import SagemakerGenerator

from odd_collector_aws.adapters.sagemaker.domain.trial_component import TrialComponent

from .domain.artifact import Artifact
from .domain.experiment import Experiment
from .domain.trial import Trial


class ArtifactsDataEntitiesCache(object):
    """Cache for artifacts  (Models, Datasets, ...)

    Some jobs can be linked with same artifacts.
    Store them and return if exists
    """

    def __init__(self):
        self.items: Dict[str,] = {}

    def upsert(self, artifact, oddrn) -> DataEntity:
        if oddrn not in self.items:
            self.items[oddrn] = artifact.to_data_entity(oddrn)

        return self.items[oddrn]


def _get_name(name, delimeter: str = "/") -> str:
    return name.split(delimeter)[-1]


def map_artifacts(
    artifacts: List[Artifact],
    cache: ArtifactsDataEntitiesCache,
    oddrn_gen: Type[SagemakerGenerator],
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []

    for artifact in artifacts:
        name = _get_name(artifact.arn or artifact.uri)
        oddrn = oddrn_gen.get_oddrn_by_path("artifacts", new_value=name)

        data_entities.append(cache.upsert(artifact, oddrn))

    return data_entities


def map_inputs(
    artifacts: List[Artifact],
    cache: ArtifactsDataEntitiesCache,
    oddrn_gen: Type[SagemakerGenerator],
) -> List[DataEntity]:
    output_oddrn = oddrn_gen.get_oddrn_by_path("jobs")
    data_entities = map_artifacts(artifacts, cache, oddrn_gen)

    for de in data_entities:
        if de.type == DataEntityType.MICROSERVICE:
            de.data_input.outputs.append(output_oddrn)

    return data_entities


def map_outputs(
    artifacts: List[Artifact],
    cache: ArtifactsDataEntitiesCache,
    oddrn_gen: Type[SagemakerGenerator],
) -> List[DataEntity]:
    input_oddrn = oddrn_gen.get_oddrn_by_path("jobs")
    data_entities = map_artifacts(artifacts, cache, oddrn_gen)

    for de in data_entities:
        if de.type == DataEntityType.ML_MODEL:
            de.data_consumer.inputs.append(input_oddrn)
        else:
            de.dataset.parent_oddrn = input_oddrn

    return data_entities


def map_trial_component(
    trial_component: TrialComponent,
    oddrn_gen: SagemakerGenerator,
    artifacts_cache: ArtifactsDataEntitiesCache,
) -> List[DataEntity]:
    name = trial_component.trial_component_name
    inputs = trial_component.input_artifacts
    outputs = trial_component.output_artifacts

    oddrn = oddrn_gen.get_oddrn_by_path("jobs", new_value=name)
    data_entity = trial_component.to_data_entity(oddrn)

    inputs_data_entities = map_inputs(inputs, artifacts_cache, oddrn_gen)
    outputs_data_entities = map_outputs(outputs, artifacts_cache, oddrn_gen)

    inputs_oddrn = [i.oddrn for i in inputs_data_entities]
    outputs_oddrn = [o.oddrn for o in outputs_data_entities]

    # Add links to upstream/downstream
    data_entity.data_transformer.inputs.extend(inputs_oddrn)
    data_entity.data_transformer.outputs.extend(outputs_oddrn)

    return [data_entity, *inputs_data_entities, *outputs_data_entities]


def map_trial(
    trial: Trial,
    *,
    oddrn_gen: SagemakerGenerator,
    artifacts_cache: ArtifactsDataEntitiesCache,
) -> List[DataEntity]:
    result: List[DataEntity] = []

    # Set oddrn generator for TRIAL
    oddrn_gen.set_oddrn_paths(trials=trial.trial_name)

    # Transform to DataEntity
    trial_de = trial.to_data_entity(oddrn_gen=oddrn_gen)
    result.append(trial_de)

    uniq_trial_components_oddrn = set()
    for tc in trial.trial_components:
        entities = map_trial_component(tc, oddrn_gen, artifacts_cache)
        new_entities = [
            e for e in entities if e.oddrn not in uniq_trial_components_oddrn
        ]
        result.extend(new_entities)

        uniq_trial_components_oddrn.update({e.oddrn for e in entities})

    trial_de.data_entity_group.entities_list = list(uniq_trial_components_oddrn)

    return result


#  TODO: SRP, 1. create oddrn, append, map
#  TODO: Clean for loop
def map_experiment(experiment: Experiment, oddrn_gen: SagemakerGenerator):
    artifacts_cache = ArtifactsDataEntitiesCache()
    data_entities: List[DataEntity] = []

    experiment_oddrn = oddrn_gen.get_oddrn_by_path(
        "experiments", new_value=experiment.experiment_name
    )

    experiment_de = experiment.to_data_entity(experiment_oddrn)

    data_entities.append(experiment_de)

    map_experiment_trial = partial(
        map_trial, oddrn_gen=oddrn_gen, artifacts_cache=artifacts_cache
    )

    for trial in experiment.trials:
        des = map_experiment_trial(trial)

        data_entities.extend(des)

    return data_entities
