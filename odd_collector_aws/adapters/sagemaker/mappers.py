from typing import List, Dict

from odd_models.models import DataEntity, DataEntityType

from .domain.artifact import Artifact
from .domain.experiment import Experiment
from .domain.trial import Trial


class ArtifactsDataEntitiesCache:
    def __init__(self):
        self.items: Dict[
            str,
        ] = dict()

    def upsert(self, data_entity: DataEntity) -> DataEntity:
        oddrn = data_entity.oddrn
        if oddrn not in self.items:
            self.items[oddrn] = data_entity

        return self.items[oddrn]


def map_artifacts(
        artifacts: List[Artifact], cache: ArtifactsDataEntitiesCache, trial_oddrn: str
) -> List[DataEntity]:
    items: List[DataEntity] = []

    for artifact in artifacts:

        if artifact.arn is None:
            oddrn = f"{trial_oddrn}/artifact/{artifact.uri.split('/')[-1]}"
        else:
            oddrn = f"{trial_oddrn}/artifact/{artifact.arn.split('/')[-1]}"

        de = artifact.to_data_entity(oddrn)
        items.append(cache.upsert(de))

    return items


def map_inputs(
        artifacts: List[Artifact], cache: ArtifactsDataEntitiesCache, trial_oddrn: str, output_oddrn: str
) -> List[DataEntity]:
    data_entities = map_artifacts(artifacts, cache, trial_oddrn)

    for de in data_entities:
        if de.type == DataEntityType.MICROSERVICE:
            de.data_input.outputs.append(output_oddrn)

    return data_entities


def map_outputs(
        artifacts: List[Artifact],
        cache: ArtifactsDataEntitiesCache,
        input_oddrn: str,
        trial_oddrn: str,
) -> List[DataEntity]:
    data_entities = map_artifacts(artifacts, cache, trial_oddrn)

    for de in data_entities:
        if de.type == DataEntityType.ML_MODEL:
            de.data_consumer.inputs.append(input_oddrn)
        else:
            de.dataset.parent_oddrn = input_oddrn

    return data_entities


def map_trial(
        trial: Trial, experiment_oddrn: str, artifacts_cache: ArtifactsDataEntitiesCache
) -> List[DataEntity]:
    result: List[DataEntity] = []

    trial_oddrn = f"{experiment_oddrn}/trial/{trial.trial_name}"
    trial_de = trial.to_data_entity(trial_oddrn, experiment_oddrn)
    result.append(trial_de)

    uniq_trial_components_oddrn = set()

    for tc in trial.trial_components:
        tc_oddrn = f"{trial_oddrn}/job/{tc.trial_component_name}"
        tc_de = tc.to_data_entity(tc_oddrn)

        inputs_de = map_inputs(tc.input_artifacts, artifacts_cache, trial_oddrn, tc_oddrn)
        outputs_de = map_outputs(
            tc.output_artifacts, artifacts_cache, tc_oddrn, trial_oddrn
        )

        inputs_oddrn = [i.oddrn for i in inputs_de]
        outputs_oddrn = [o.oddrn for o in outputs_de]

        tc_de.data_transformer.inputs.extend(inputs_oddrn)
        tc_de.data_transformer.outputs.extend(outputs_oddrn)

        entities = [tc_de, *inputs_de, *outputs_de]

        result.extend([e for e in entities if e.oddrn not in uniq_trial_components_oddrn])

        uniq_trial_components_oddrn.update({tc_de.oddrn, *inputs_oddrn, *outputs_oddrn})

    trial_de.data_entity_group.entities_list = list(uniq_trial_components_oddrn)

    return result


def map_experiment(experiment: Experiment, base_oddrn: str):
    artifacts_cache = ArtifactsDataEntitiesCache()
    result: List[DataEntity] = []

    experiment_oddrn = f"{base_oddrn}/{experiment.experiment_name}"
    experiment_de = experiment.to_data_entity(experiment_oddrn)

    result.append(experiment_de)

    for trial in experiment.trials:
        des = map_trial(trial, experiment_oddrn, artifacts_cache)

        result.extend(des)

    return result
