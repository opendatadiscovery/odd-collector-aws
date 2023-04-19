from collections import defaultdict
from typing import Dict, List
from funcy import concat, lpluck_attr

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType


class HandleNestedStructure:
    def __init__(self, datasets, list_of_oddrns, oddrn_generator):
        self.datasets = datasets
        self.list_of_oddrns = list_of_oddrns[::-1]
        self.oddrn_generator = oddrn_generator
        self.replaced_oddrn_path = ""
        self.delimiter = "/"
        self.backslashes = "\\\\"

    def get_all_data_entities(self) -> List[DataEntity]:
        """Get all data entity according to datasets."""
        result = []
        for dataset in self.datasets:
            result += self.get_data_entity_list(dataset.bucket, dataset.path)
        return result

    def get_data_entity_list(self, bucket: str, s3_path: str) -> List[DataEntity]:
        """Get combined list with DataEntity objects."""
        self.oddrn_generator.set_oddrn_paths(
            buckets=bucket,
            keys=s3_path
        )
        oddrn_by_path = self.oddrn_generator.get_oddrn_by_path('keys')
        self.replaced_oddrn_path = oddrn_by_path.replace(self.backslashes, self.delimiter)
        filtered_list_of_oddrns = list(filter(lambda s: s.startswith(oddrn_by_path), self.list_of_oddrns))
        parsed_oddrns = self._parse_oddrns(filtered_list_of_oddrns, s3_path, oddrn_by_path)
        generated_data_entities = self._generate_data_entity(parsed_oddrns, s3_path)
        result = self._combine_data_entities(generated_data_entities)
        return result

    def _generate_folder_entity(self, path: str, entities: List[str]):
        """Generate DataEntity by given args."""
        data_entity = DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path('keys', path),
            name=path,
            type=DataEntityType.DATABASE_SERVICE,
            data_entity_group=DataEntityGroup(entities_list=entities)
        )
        return data_entity

    def _parse_oddrns(self, oddrns: List[str], s3_path: str, main_oddrn: str) -> Dict[str, List[str]]:
        """
        Create dictionary from list of created oddrns from files,
        where `key` is path, and `value` is a list of files
        related to the same path.
        """
        result = defaultdict(list)
        second_part_of_oddrn = [i.split(main_oddrn)[-1].replace(self.backslashes, self.delimiter) for i in oddrns]
        for i in second_part_of_oddrn:
            index = second_part_of_oddrn.index(i)
            if len(i.split(self.delimiter)) == 1:
                result[s3_path].append(oddrns[index])
            else:
                parent_path, _ = i.rsplit(self.delimiter, 1)
                new_path = f"{s3_path}{parent_path}/"
                result[new_path].append(oddrns[index])

        return result

    def _generate_data_entity(self, entities: Dict[str, List[str]], s3_path: str) -> List[DataEntity]:
        """Create the list of DataEntity objects."""
        result = []
        previous_path = ''
        for i in entities:
            data_entity = entities[i]
            if i == s3_path:
                list_of_oddrns = lpluck_attr("oddrn", concat(result))[::-1]
                extra_data_entities = [x for x in list_of_oddrns if
                                       x.split(self.replaced_oddrn_path)[1].count(self.delimiter) == 1]
                data_entity += extra_data_entities

            if i in previous_path:
                data_entity.append(previous_path)
            current_entity = self._generate_folder_entity(i, data_entity)
            result.append(current_entity)
            previous_path = current_entity.oddrn

        return result

    def _combine_data_entities(self, data_entities: List[DataEntity]) -> List[DataEntity]:
        """Combine oddrn from nested folder with parent."""
        oddrns = lpluck_attr("oddrn", concat(data_entities))
        step = 2
        for oddrn in oddrns:
            if oddrn == self.replaced_oddrn_path:
                continue
            else:
                make_path = oddrn.rsplit(self.delimiter, step)[0] + self.delimiter
                get_index = oddrns.index(make_path)
                if oddrn not in data_entities[get_index].data_entity_group.entities_list:
                    data_entities[get_index].data_entity_group.entities_list.append(oddrn)

        return data_entities
