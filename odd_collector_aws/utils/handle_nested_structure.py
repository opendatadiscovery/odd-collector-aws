from collections import defaultdict
from typing import Dict, List
from funcy import concat, lpluck_attr

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator.generators import Generator
from oddrn_generator.utils import unescape, DELIMITER


class HandleNestedStructure:

    def get_all_data_entities(
            self,
            list_of_oddrns: List[str],
            datasets: List,
            oddrn_generator: Generator,
            s3_folders: Dict[str, List[str]]
    ) -> List[DataEntity]:
        """Get all data entity according to datasets."""
        result = []
        for dataset in datasets:
            result += self.get_data_entity_list(
                dataset.bucket, dataset.path, oddrn_generator, list_of_oddrns, s3_folders[dataset.path]
            )
        return result

    def get_data_entity_list(
            self,
            bucket: str,
            s3_path: str,
            oddrn_generator: Generator,
            list_of_oddrns: List[str],
            s3_folders: List[str]
    ) -> List[DataEntity]:
        """Get combined list with DataEntity objects."""
        oddrn_generator.set_oddrn_paths(
            buckets=bucket,
            keys=s3_path
        )
        oddrn_by_path = oddrn_generator.get_oddrn_by_path('keys')
        parent_oddrn = unescape(oddrn_by_path)
        filtered_list_of_oddrns = list(
            filter(lambda s: s.startswith(oddrn_by_path), list_of_oddrns)
        )
        folder_files_map = self._parse_oddrns(filtered_list_of_oddrns, s3_path, oddrn_by_path, s3_folders)
        generated_data_entities = self._generate_data_entity(
            folder_files_map, s3_path, parent_oddrn, oddrn_generator
        )
        result = self._combine_data_entities(generated_data_entities, parent_oddrn)
        return result

    def _generate_folder_entity(self, path: str, entities: List[str], oddrn_generator: Generator):
        """Generate DataEntity by given args."""
        data_entity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path('keys', path),
            name=path,
            type=DataEntityType.FILE,
            data_entity_group=DataEntityGroup(entities_list=entities)
        )
        return data_entity

    def _parse_oddrns(
            self,
            oddrns: List[str],
            s3_path: str,
            main_oddrn: str,
            s3_folders: List[str]
    ) -> Dict[str, List[str]]:
        """
        Create dictionary from list of created oddrns from files,
        where `key` is path, and `value` is a list of files
        related to the same path.
        @return {'path_to_folder': ['oddrn_of_file']}
        """
        result = defaultdict(list)
        second_part_of_oddrn = [unescape(oddrn.split(main_oddrn)[-1]) for oddrn in oddrns]
        for i in second_part_of_oddrn:
            index = second_part_of_oddrn.index(i)
            if len(i.split(DELIMITER)) == 1:
                result[s3_path].append(oddrns[index])
            else:
                parent_path, _ = i.rsplit(DELIMITER, 1)
                new_path = f"{s3_path}{parent_path}/"
                result[new_path].append(oddrns[index])

        for folder in s3_folders:
            if folder not in list(result.keys()):
                result[folder] = []

        return result

    def _generate_data_entity(
            self,
            folder_files_map: Dict[str, List[str]],
            s3_path: str,
            parent_oddrn: str,
            oddrn_generator: Generator
    ) -> List[DataEntity]:
        """
        Create the list of DataEntity objects.
        @param: entities {'path_to_folder': ['oddrn_of_file']}
        @return: [DataEntity,...,DataEntity]
        """
        result = []
        previous_path = ''
        for folder_path in folder_files_map:
            oddrn_of_files = folder_files_map[folder_path].copy()
            if folder_path == s3_path:
                list_of_oddrns = list(reversed(lpluck_attr("oddrn", concat(result))))
                oddrn_of_sub_folders = [oddrn for oddrn in list_of_oddrns if
                                        self._has_one_delimiter(oddrn, parent_oddrn)]
                oddrn_of_files += oddrn_of_sub_folders
                previous_path = ''
            if folder_path in previous_path:
                oddrn_of_files.append(previous_path)
            current_entity = self._generate_folder_entity(folder_path, oddrn_of_files, oddrn_generator)
            result.append(current_entity)
            previous_path = current_entity.oddrn

        return result

    def _has_one_delimiter(self, oddrn: str, parent_oddrn: str) -> bool:
        max_counter = 1
        return oddrn.split(parent_oddrn)[1].count(DELIMITER) == max_counter

    def _combine_data_entities(
            self,
            data_entities: List[DataEntity],
            parent_oddrn: str
    ) -> List[DataEntity]:
        """Combine oddrn from nested folder with parent."""
        oddrns = lpluck_attr("oddrn", concat(data_entities))
        step = 2
        for oddrn in oddrns:
            if oddrn == parent_oddrn:
                continue
            else:
                make_path = oddrn.rsplit(DELIMITER, step)[0] + DELIMITER
                get_index = oddrns.index(make_path)
                if oddrn not in data_entities[get_index].data_entity_group.entities_list:
                    data_entities[get_index].data_entity_group.entities_list.append(oddrn)

        return data_entities
