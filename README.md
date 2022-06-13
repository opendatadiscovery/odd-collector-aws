[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
# odd-collector-aws
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview
 - [Implemented adapters](#implemented-adapters)
 - [How to build](#building)
 - [Config example](#config-example)

## Implemented adapters
 - [Athena](#athena)
 - [DynamoDB](#dynamodb)
 - [Glue](#glue)
 - [Kinesis](#kinesis)
 - [Quicksight](#quicksight)
 - [S3](#s3)
 - [Sagemaker](#sagemaker)
 - [SagemakerFeaturestore](#sagemaker-featurestore)
 - [SQS](#sqs)

### __Athena__
```yaml
type: athena
name: athena
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __DynamoDB__
```yaml
type: dynamodb
name: dynamodb
exclude_tables: str[]
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str[]
```
### __Glue__
```yaml
type: glue
name: glue
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __Kinesis__
```yaml
type: kinesis
name: kinesis
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
aws_account_id: str
```
### __Quicksight__
```yaml
type: quicksight
name: quicksight
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __S3__
```yaml
type: s3
name: s3
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
datasets:
  - bucket: str
    path: str
    each_file_as_dataset: Optional[bool]
```

### __Sagemaker__
```yaml
type: sagemaker
name: sagemaker
experiments: str[]
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __Sagemaker Featurestore__
```yaml
type: sagemaker_featurestore
name: sagemaker_featurestore
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __SQS__
```yaml
type: sqs
name: sqs
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

## Building
```bash
docker build .
```

## Config example
Due to the Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

Custom `.env` file for docker-compose.yaml
```
AWS_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
PLATFORM_HOST_URL=http://odd-platform:8080
```

Custom `collector-config.yaml`
```yaml
provider_oddrn: collector
platform_host_url: "http://localhost:8080"
default_pulling_interval: 10
token: ""
plugins:
  - type: glue
    name: test_glue_adapter
  - type: s3
    name: test_s3_adapter
    paths: ['some_bucket_name']
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
