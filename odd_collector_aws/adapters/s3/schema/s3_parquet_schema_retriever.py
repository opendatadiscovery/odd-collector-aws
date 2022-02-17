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
        self.__s3_client = boto3.client('s3',
                            endpoint_url=os.environ['MINIO_URL'] if 'MINIO_URL' in os.environ and os.environ['MINIO_URL'] else None,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region
                            )
        self.__partitioning = ds.partitioning(flavor="hive")

    def __set_ds(self, path: str, ext):
        self.__data = ds.dataset(path, filesystem=self.__fs, format=ext , partitioning=self.__partitioning)

    def get_schema(self, path: str, ext) -> Schema:
        logging.debug(f"Fetching schema for {path}")
        try:
            self.__set_ds(path,ext)
            return self.__data.schema
        except (pyarrow.ArrowInvalid, OSError) as e:
            logging.warn(f"unable to pars dataset in {path} with {ext} format") 

    def get_format(self, path: str):
        keys =  self.__s3_client.list_objects_v2(Bucket=path.split('/')[0], Prefix="/".join(path.split("/")[1:]))['Contents']
        for key in keys:
            if key['Key'].endswith('/'):
                continue
            if key['Key'].endswith('.csv') or key['Key'].endswith('.csv.gz'):
                return 'csv'
            if key['Key'].endswith('.tsv') or key['Key'].endswith('.tsv.gz'):
                return ds.CsvFileFormat(parse_options=csv.ParseOptions(delimiter="\t"))
            if key['Key'].endswith('.parquet'):
                return 'parquet'
        logging.warn(f"Usupported data format in {path}")

    def get_metadata(self, path: str, ext):
        return self.__extract_all_entries(self.__data, path, ext)

    def __extract_all_entries(self,
                              data, path: str, ext) -> Dict[str, Any]:
        entries = {}
        entries['Rows'] = data.count_rows() if ext == 'parquet' else None
        entries['Files'] = len(data.files)
        entries['Bucket'] = path.split('/')[0]
        entries['Key'] = "/".join(path.split("/")[1:])
        entries['Region'] =data.filesystem.region
        li = []
        for file in data.files[0:50]:
            li.append(data.filesystem.get_file_info(file).size)
        entries['Avg. file size'] =  round(sum(li)/len(li)) 
        entries['Estimated dataset size'] =str(round(entries['Avg. file size'] * entries['Files']/ 1024/1024/1024, 2)) + ' GB'
        entries['File format'] = ext if ext in ['parquet', 'csv'] else 'tsv'
        entries['Partition style'] = 'Directory partitioned'
        if data.partitioning.schema != data.schema:
            entries['Partition style'] = 'Hive style partitioned'
            entries['Partitioning columns'] = data.partitioning.schema.names

        # table = data.head(1000).to_pandas()
        return entries
    
