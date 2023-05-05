import unittest
from funcy import lpluck_attr
from odd_collector_aws.utils.create_generator import create_generator
from oddrn_generator.generators import S3Generator
from oddrn_generator.utils import unescape

from odd_collector_aws.domain.dataset_config import DatasetConfig
from odd_collector_aws.domain.plugin import S3Plugin
from odd_collector_aws.utils.handle_nested_structure import HandleNestedStructure
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType


class TestHandleNestedStructure(unittest.TestCase):
    def setUp(self) -> None:
        self.handle_nested_structure = HandleNestedStructure()
        self.datasets = [
            DatasetConfig(bucket="first_bucket_name", path="first_path/"),
            DatasetConfig(bucket="second_bucket_name", path="second_path/"),
        ]
        self.s3_creds = {
            "type": "s3",
            "name": "s3_adapter",
            "aws_secret_access_key": "test",
            "aws_access_key_id": "test",
            "datasets": self.datasets,
        }
        self.s3_plugin = S3Plugin(**self.s3_creds)
        self.s3_config = {
            "default_pulling_interval": 10,
            "token": "token",
            "platform_host_url": "http://localhost:8080",
            "plugins": [self.s3_plugin],
        }
        self.oddrn_generator = create_generator(S3Generator, self.s3_plugin)
        self.list_of_oddrn = [
            "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\folder_b_b\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_a\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_b\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_2\\\\test_1_1_1_1\\\\test_1_1_1_1_1\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_2\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_3\\\\test_1_1_3_1\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_3\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\Sample-Spreadsheet-100-rows.csv",
            "//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\Sample-Spreadsheet-100-rows.csv",
        ]

        self.s3_folders = {
            "first_path/": [
                "first_path/",
                "first_path/folder_b/",
                "first_path/folder_b/folder_b_b/",
                "first_path/folder_a/",
                "first_path/folder_a/folder_a_a/",
                "first_path/folder_a/folder_a_b/",
            ],
            "second_path/": [
                "second_path/",
                "second_path/test_1_1/",
                "second_path/test_1_1/test_1_1_2/",
                "second_path/test_1_1/test_1_1_2/test_1_1_1_1/",
                "second_path/test_1_1/test_1_1_2/test_1_1_1_1/test_1_1_1_1_1/",
                "second_path/test_1_1/test_1_1_3/",
                "second_path/test_1_1/test_1_1_3/test_1_1_3_1/",
            ],
        }

        self.expected_result_of_parse_oddrns_method = {
            "first_path/": [
                "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\Sample-Spreadsheet-100-rows.csv"
            ],
            "first_path/folder_a/": [],
            "first_path/folder_a/folder_a_a/": [
                "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_a\\\\Sample-Spreadsheet-100-rows.csv"
            ],
            "first_path/folder_a/folder_a_b/": [
                "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_b\\\\Sample-Spreadsheet-100-rows.csv"
            ],
            "first_path/folder_b/": [
                "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\Sample-Spreadsheet-100-rows.csv"
            ],
            "first_path/folder_b/folder_b_b/": [
                "//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\folder_b_b\\\\Sample-Spreadsheet-100-rows.csv"
            ],
        }
        self.expected_result_of_generate_data_entity = [
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/', name='first_path/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/', name='first_path/folder_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=[], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_a/', name='first_path/folder_a/folder_a_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_a\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_b/', name='first_path/folder_a/folder_a_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/', name='first_path/folder_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/folder_b_b/', name='first_path/folder_b/folder_b_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\folder_b_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None))
        ]

        self.expected_result_of_combine_data_entities = [
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/', name='first_path/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/', name='first_path/folder_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_a/', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_b/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_a/', name='first_path/folder_a/folder_a_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_a\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_b/', name='first_path/folder_a/folder_a_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/', name='first_path/folder_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/folder_b_b/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/folder_b_b/', name='first_path/folder_b/folder_b_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\folder_b_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None))
        ]

        self.expected_result_of_get_all_data_entities = [
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/folder_b_b/', name='first_path/folder_b/folder_b_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\folder_b_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/', name='first_path/folder_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_b\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/folder_b_b/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_a/', name='first_path/folder_a/folder_a_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_a\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_b/', name='first_path/folder_a/folder_a_b/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\folder_a\\\\folder_a_b\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/', name='first_path/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_b/', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/', name='first_path/folder_a/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_a/', '//s3/cloud/aws/buckets/first_bucket_name/keys/first_path/folder_a/folder_a_b/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/test_1_1_1_1/test_1_1_1_1_1/', name='second_path/test_1_1/test_1_1_2/test_1_1_1_1/test_1_1_1_1_1/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_2\\\\test_1_1_1_1\\\\test_1_1_1_1_1\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/', name='second_path/test_1_1/test_1_1_2/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_2\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/test_1_1_1_1/test_1_1_1_1_1/', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/test_1_1_1_1/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_3/test_1_1_3_1/', name='second_path/test_1_1/test_1_1_3/test_1_1_3_1/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_3\\\\test_1_1_3_1\\\\Sample-Spreadsheet-100-rows.csv'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_3/', name='second_path/test_1_1/test_1_1_3/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\test_1_1_3\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_3/test_1_1_3_1/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/', name='second_path/test_1_1/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\test_1_1\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_3/', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/', name='second_path/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path\\\\Sample-Spreadsheet-100-rows.csv', '//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/'], group_oddrn=None)),
            DataEntity(oddrn='//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/test_1_1_1_1/', name='second_path/test_1_1/test_1_1_2/test_1_1_1_1/', version=None, description=None, owner=None, metadata=None, tags=None, updated_at=None, created_at=None, type=DataEntityType.FILE, dataset=None, data_transformer=None, data_transformer_run=None, data_quality_test=None, data_quality_test_run=None, data_input=None, data_consumer=None, data_entity_group=DataEntityGroup(entities_list=['//s3/cloud/aws/buckets/second_bucket_name/keys/second_path/test_1_1/test_1_1_2/test_1_1_1_1/test_1_1_1_1_1/'], group_oddrn=None))
        ]

    def test_parse_oddrns(self):
        for dataset in self.datasets:
            self.oddrn_generator.set_oddrn_paths(
                buckets=dataset.bucket, keys=dataset.path
            )
            oddrn_by_path = self.oddrn_generator.get_oddrn_by_path("keys")
            filtered_list_of_oddrns = list(
                filter(lambda s: s.startswith(oddrn_by_path), self.list_of_oddrn)
            )
            result = self.handle_nested_structure._parse_oddrns(
                filtered_list_of_oddrns,
                dataset.path,
                oddrn_by_path,
                self.s3_folders[dataset.path],
            )

            self.assertEqual(len(self.s3_folders[dataset.path]), len(result.keys()))

    def test_generate_data_entity(self):
        s3_path = self.datasets[0].path
        bucket = self.datasets[0].bucket
        self.oddrn_generator.set_oddrn_paths(buckets=bucket, keys=s3_path)
        unescaped_oddrn = unescape(self.oddrn_generator.get_oddrn_by_path("keys"))

        result = self.handle_nested_structure._generate_data_entity(
            self.expected_result_of_parse_oddrns_method,
            s3_path,
            unescaped_oddrn,
            self.oddrn_generator,
        )

        # Check if the list of paths equals to the list of names from the DataEntity.
        self.assertEqual(
            list(self.expected_result_of_parse_oddrns_method.keys()),
            lpluck_attr("name", result),
        )
        self.assertEqual(self.expected_result_of_generate_data_entity, result)

    def test_combine_data_entities(self):
        s3_path = self.datasets[0].path
        bucket = self.datasets[0].bucket
        self.oddrn_generator.set_oddrn_paths(buckets=bucket, keys=s3_path)
        unescaped_oddrn = unescape(self.oddrn_generator.get_oddrn_by_path("keys"))

        result = self.handle_nested_structure._combine_data_entities(
            self.expected_result_of_generate_data_entity, unescaped_oddrn
        )

        expected_oddrns = lpluck_attr(
            "oddrn", self.expected_result_of_combine_data_entities
        )
        result_oddrns = lpluck_attr("oddrn", result)

        result_oddrn_entity_list = {
            i.oddrn: i.data_entity_group.entities_list for i in result
        }
        expected_oddrn_entity_list = {
            i.oddrn: i.data_entity_group.entities_list
            for i in self.expected_result_of_combine_data_entities
        }

        self.assertEqual(expected_oddrns, result_oddrns)
        self.assertEqual(expected_oddrn_entity_list, result_oddrn_entity_list)
        self.assertEqual(self.expected_result_of_combine_data_entities, result)

    def test_data_entity_list(self):
        bucket = self.datasets[0].bucket
        s3_path = self.datasets[0].path

        result = self.handle_nested_structure.get_data_entity_list(
            bucket,
            s3_path,
            self.oddrn_generator,
            self.list_of_oddrn,
            self.s3_folders[s3_path],
        )
        length_of_expected_data_entities = len(self.s3_folders[s3_path])
        self.assertEqual(length_of_expected_data_entities, len(result))

    def test_get_all_data_entities(self):
        result = self.handle_nested_structure.get_all_data_entities(
            self.list_of_oddrn, self.datasets, self.oddrn_generator, self.s3_folders
        )

        self.assertEqual(self.expected_result_of_get_all_data_entities, result)
