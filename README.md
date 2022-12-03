[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/for-you.svg)](https://forthebadge.com)
# odd-collector-aws
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview
 - [Implemented adapters](#implemented-adapters)
 - [How to build](#building)
 - [Docker compose example](#docker-compose-example)

## Implemented adapters
| Service                                                      | Config example                                        |
|--------------------------------------------------------------|-------------------------------------------------------|
| Athena                 <a name="athena"></a>                 | [config](config_examples/athena.yaml)                 |
| DynamoDB               <a name="dynamodb"></a>               | [config](config_examples/dynamodb.yaml)               |
| Glue                   <a name="glue"></a>                   | [config](config_examples/glue.yaml)                   |
| Kinesis                <a name="kinesis"></a>                | [config](config_examples/kinesis.yaml)                |
| Quicksight             <a name="quicksight"></a>             | [config](config_examples/quicksight.yaml)             |
| S3                     <a name="s3"></a>                     | [config](config_examples/s3.yaml)                     |
| Sagemaker              <a name="sagemaker"></a>              | [config](config_examples/sagemaker.yaml)              |
| SQS                    <a name="sqs"></a>                    | [config](config_examples/sqs.yaml)                    |
| SagemakerFeaturestore  <a name="sagemaker_featurestore"></a> | [config](config_examples/sagemaker_featurestore.yaml) |



## Building
```bash
docker build .
```

## Docker compose example
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
platform_host_url: "http://localhost:8080"
default_pulling_interval: 10
token: ""
plugins:
  - type: s3
    name: test_s3_adapter
    datasets:
      - bucket: bucket_name
        path: some_data
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
    image: 'ghcr.io/opendatadiscovery/odd-collector-aws:latest'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - AWS_REGION=${AWS_REGION}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
      - LOGLEVEL='DEBUG'
    depends_on:
      - odd-platform
```
