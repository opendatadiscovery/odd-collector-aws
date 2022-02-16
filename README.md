# odd-collector-aws
Aggregation of AWS adapters for [ODD Platform](https://github.com/opendatadiscovery/odd-platform)

`odd-collector-aws` uses [odd-collector-sdk](https://github.com/opendatadiscovery/odd-collector-sdk).


## Domain
___
Main class for AWS plugins is `AwsPlugin` inherited from `odd-collector-sdk.domain.Plugin`. 
```python
class AwsPlugin(Plugin):
    aws_secret_access_key: str
    aws_access_key_id: str
    aws_region: str
```
Due to Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

## Implemented adapters
___
### __Athena__
```python
class AthenaPlugin(AwsPlugin):
    type: Literal["athena"]
```
### __DynamoDB__
```python
class DynamoDbPlugin(AwsPlugin):
    type: Literal["dynamodb"]
    exclude_tables: Optional[List[str]] = []
```
### __Glue__
```python
class GluePlugin(AwsPlugin):
    type: Literal["glue"]
```
### __Quicksight__
```python
class QuicksightPlugin(AwsPlugin):
    type: Literal["quicksight"]
```
### __S3__
```python
class S3Plugin(AwsPlugin):
    type: Literal["s3"]
    buckets: Optional[List[str]] = []
```
### __Sagemaker__
```python
class SagemakerPlugin(AwsPlugin):
    type: Literal["sagemaker_featurestore"]
```

## Building
```bash
docker build .
```

## Example of docker-compose.yaml
Custom `.env` file for docker-compose.yaml
```
AWS_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
PLATFORM_HOST_URL=http://odd-platform:8080
```

Custom `collector-config.yaml`x
```yaml
default_pulling_interval: 10
token: ""
plugins:
  - type: glue
    name: test_glue_adapter
  - type: s3
    name: test_s3_adapter
    buckets: ['some_bucket_name']
```

docker-compose.yaml
```yaml
version: "3.8"
services:
  # --- ODD Platform ---
  database:
    ...

  odd-platform:
    ...
  
  odd-collector-aws:
    image: 'image_name'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - AWS_REGION=${AWS_REGION}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
    depends_on:
      - odd-platform
```