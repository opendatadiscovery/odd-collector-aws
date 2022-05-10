[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
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
```yaml
type: athena
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __DynamoDB__
```yaml
type: dynamodb
exclude_tables: str[]
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str[]
```
### __Glue__
```yaml
type: glue
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __Quicksight__
```yaml
type: quicksight
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```
### __S3__
```yaml
type: s3
paths: str[]
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __Sagemaker__
```yaml
type: sagemaker
experiments: str[]
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __Sagemaker Featurestore__
```yaml
type: sagemaker_featurestore
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __Quicksight__
```yaml
type: quicksight
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
```

### __SQS__
```yaml
type: sqs
aws_secret_access_key: str
aws_access_key_id: str
aws_region: str
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
