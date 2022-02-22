import logging
from typing import Any, Dict
import boto3
import pyarrow
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow.lib import Schema
import pyarrow.csv as csv 
import pyarrow.dataset as ds
import pyarrow.compute as pc
import os

N_FILES_SIZE_ESTIMATION = 50


class S3SchemaRetriever:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_region: str) -> None:
        self.__fs = fs.S3FileSystem(
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
            region=aws_region,
            endpoint_override=os.environ['MINIO_URL'] if 'MINIO_URL' in os.environ and os.environ['MINIO_URL'] else None,
        )
        self.__s3_client = boto3.client(
            's3',
            endpoint_url=os.environ['MINIO_URL'] if 'MINIO_URL' in os.environ and os.environ['MINIO_URL'] else None,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
    def build_s3ds(self, path: str):
        keys = self.__s3_client.list_objects_v2(Bucket=path.split('/')[0], Prefix="/".join(path.split("/")[1:]))['Contents']
        for key in keys:
            if key['Key'].endswith('/'):
                continue
            if key['Key'].endswith('.csv') or key['Key'].endswith('.csv.gz'):
                return CSVS3Dataset(self.__fs, self.__s3_client, path)
            if key['Key'].endswith('.tsv') or key['Key'].endswith('.tsv.gz'):
                return TSVS3Dataset(self.__fs, self.__s3_client, path)
            if key['Key'].endswith('.parquet'):
                return ParquetS3Dataset(self.__fs, self.__s3_client, path)
        logging.warn(f"Usupported data format in {path}")


class S3Dataset:
    def __init__(self, fs: fs.S3FileSystem, s3_client, path: str) -> None:
        self.partitioning = ds.partitioning(flavor="hive")
        self.format = ''
        self.fs = fs
        self.s3_client = s3_client
        self.path = path

    def __set_ds(self):
        self.data = ds.dataset(self.path, filesystem=self.fs, format=self.format, partitioning=self.partitioning)

    def get_schema(self) -> Schema:
        logging.debug(f"Fetching schema for {self.path}")
        try:
            self.__set_ds()
            return self.data.schema
        except (pyarrow.ArrowInvalid, OSError) as e:
            logging.warn(f"unable to pars dataset in {self.path} with {self.format} format")

    def __extract_all_entries(self):
        entries = {}
        data = self.data
        path = self.path
        entries['Files'] = len(data.files)
        entries['Bucket'] = path.split('/')[0]
        entries['Key'] = "/".join(path.split("/")[1:])
        entries['Region'] = data.filesystem.region
        li = []
        for file in data.files[0:N_FILES_SIZE_ESTIMATION]:
            li.append(data.filesystem.get_file_info(file).size)
        entries['Avg. file size'] = round(sum(li) / len(li))
        entries['Estimated dataset size'] = str(
            round(entries['Avg. file size'] * entries['Files'] / 1024 / 1024 / 1024, 2)) + ' GB'
        entries['Partition style'] = 'Directory partitioned'
        if data.partitioning.schema != data.schema:
            entries['Partition style'] = 'Hive style partitioned'
            entries['Partitioning columns'] = data.partitioning.schema.names

        return entries

    def get_metadata(self):
        return self.__extract_all_entries()

    def get_rows(self):
        pass


class CSVS3Dataset(S3Dataset):
    def __init__(self, fs: fs.S3FileSystem, s3_client, path: str):
        super().__init__(fs, s3_client, path)
        self.format = 'csv'

    def __extract_all_entries(self):
        entries = super().__extract_all_entries()
        entries['File format'] = self.format
        return entries


class ParquetS3Dataset(S3Dataset):
    def __init__(self, fs: fs.S3FileSystem, s3_client, path: str):
        super().__init__(fs, s3_client, path)
        self.format = 'parquet'

    def __extract_all_entries(self):
        entries = super().__extract_all_entries()
        entries['File format'] = self.format
        return entries
    
    def get_rows(self):
        return self.data.count_rows()


class TSVS3Dataset(S3Dataset):
    def __init__(self, fs: fs.S3FileSystem, s3_client, path: str):
        super().__init__(fs, s3_client, path)
        self.format = ds.CsvFileFormat(parse_options=csv.ParseOptions(delimiter="\t"))

    def __extract_all_entries(self):
        entries = super().__extract_all_entries()
        entries['File format'] = 'tsv'
        return entries