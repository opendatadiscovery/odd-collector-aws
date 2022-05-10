from odd_collector_aws.adapters.sagemaker.domain.artifact import (
    create_artifact,
    Image,
    Model,
    Dataset,
)


def test_create_artifact():
    image_artifact = {
        "name": "SageMaker.ImageUri",
        "uri": "1111.dkr.ecr.us-east-1.amazonaws.com/predict-flight-delay/components/preprocessing-serving:debug",
    }

    artifact = create_artifact(**image_artifact)
    assert artifact.name == "preprocessing-serving:debug"
    assert (
        artifact.uri
        == "1111.dkr.ecr.us-east-1.amazonaws.com/predict-flight-delay/components/preprocessing-serving:debug"
    )
    assert isinstance(artifact, Image)

    model_artifact = {
        "name": "SageMaker.ModelArtifact",
        "uri": "s3://sagemaker-flight-delay-prediction-demo/pipelines-1111/output/model.tar.gz",
    }

    artifact = create_artifact(**model_artifact)
    assert artifact.name == "model"
    assert (
        artifact.uri
        == "s3://sagemaker-flight-delay-prediction-demo/pipelines-1111/output/model.tar.gz"
    )
    assert isinstance(artifact, Model)

    input_artifact = {
        "name": "input",
        "uri": "s3://bucket/input-data/sample/data.csv",
    }
    artifact = create_artifact(**input_artifact)
    assert artifact.artifact_type == "Dataset"
    assert artifact.name == "input"
    assert artifact.uri == "s3://bucket/input-data/sample/data.csv"
    assert isinstance(artifact, Dataset)
