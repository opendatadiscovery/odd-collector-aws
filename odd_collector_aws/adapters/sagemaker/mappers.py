from functools import partial
from typing import List, Dict, Type

from odd_models.models import DataEntity, DataEntityType
from oddrn_generator.generators import SagemakerGenerator

from .domain.artifact import Artifact
from .domain.experiment import Experiment
from .domain.trial import Trial


class ArtifactsDataEntitiesCache:
    def __init__(self):
        self.items: Dict[
            str,
        ] = {}

    def upsert(self, data_entity: DataEntity) -> DataEntity:
        oddrn = data_entity.oddrn
        if oddrn not in self.items:
            self.items[oddrn] = data_entity

        return self.items[oddrn]


def _get_last(str: str, delimeter: str = "/") -> str:
    return str.split(delimeter)[-1]


def map_artifacts(
    artifacts: List[Artifact],
    cache: ArtifactsDataEntitiesCache,
    oddrn_gen: Type[SagemakerGenerator],
) -> List[DataEntity]:
    items: List[DataEntity] = []

    for artifact in artifacts:
        name = _get_last(artifact.arn) if artifact.arn else _get_last(artifact.uri)
        oddrn_gen.set_oddrn_paths(artifacts=name)

        de = artifact.to_data_entity(oddrn=oddrn_gen.get_oddrn_by_path("artifacts"))
        items.append(cache.upsert(de))

    return items


def map_inputs(
    artifacts: List[Artifact],
    cache: ArtifactsDataEntitiesCache,
    oddrn_gen: Type[SagemakerGenerator],
) -> List[DataEntity]:
    trial_oddrn = oddrn_gen.get_oddrn_by_path("trials")
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


def map_trial(
    trial: Trial,
    *,
    oddrn_gen: SagemakerGenerator,
    artifacts_cache: ArtifactsDataEntitiesCache,
) -> List[DataEntity]:
    result: List[DataEntity] = []

    # Set oddrn generator for TRIAL
    oddrn_gen.set_oddrn_paths(trials=trial.trial_name)
    trial_oddrn = oddrn_gen.get_oddrn_by_path("trials")

    # Transform to DataEntity
    trial_de = trial.to_data_entity(oddrn_gen=oddrn_gen)
    result.append(trial_de)

    uniq_trial_components_oddrn = set()
    for tc in trial.trial_components:
        # Set .../jobs/jobName to oddrn generator
        oddrn_gen.set_oddrn_paths(jobs=tc.trial_component_name)

        job_oddrn = oddrn_gen.get_oddrn_by_path("jobs")
        job_oddrn = tc.to_data_entity(job_oddrn)

        inputs_de = map_inputs(tc.input_artifacts, artifacts_cache, oddrn_gen)
        outputs_de = map_outputs(tc.output_artifacts, artifacts_cache, oddrn_gen)

        inputs_oddrn = [i.oddrn for i in inputs_de]
        outputs_oddrn = [o.oddrn for o in outputs_de]

        job_oddrn.data_transformer.inputs.extend(inputs_oddrn)
        job_oddrn.data_transformer.outputs.extend(outputs_oddrn)

        entities = [job_oddrn, *inputs_de, *outputs_de]

        result.extend(e for e in entities if e.oddrn not in uniq_trial_components_oddrn)

        uniq_trial_components_oddrn.update(
            {job_oddrn.oddrn, *inputs_oddrn, *outputs_oddrn}
        )

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
