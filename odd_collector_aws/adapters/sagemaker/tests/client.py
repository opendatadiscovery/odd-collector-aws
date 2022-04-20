from datetime import datetime
from typing import Any, Generator

from ..client.client import Client
from ..domain.experiment import Experiment
from ..domain.source import Source
from ..domain.trial import Trial
from ..domain.trial_component import TrialComponent


class TestClient(Client):
    def get_experiments(self) -> Generator[Experiment, Any, Any]:
        tc1 = TrialComponent.parse_obj(
            {
                "TrialComponentName": "pipelines-7s1cdjmwshwl-evaluate-g5c6r0u9m4-aws-processing-job",
                "TrialComponentArn": "arn:aws:sagemaker:us-east-1:311638508164:experiment-trial-component/pipelines-7s1cdjmwshwl-evaluate-g5c6r0u9m4-aws-processing-job",
                "DisplayName": "pipelines-7s1cdjmwshwl-evaluate-g5c6r0u9m4-aws-processing-job",
                "Source": {
                    "SourceArn": "arn:aws:sagemaker:us-east-1:311638508164:processing-job/pipelines-7s1cdjmwshwl-evaluate-g5c6r0u9m4",
                    "SourceType": "SageMakerProcessingJob",
                },
                "Status": {
                    "PrimaryStatus": "Completed",
                    "Message": "Status: Completed, exit message: null, failure reason: null",
                },
                "StartTime": datetime.datetime.now(),
                "EndTime": datetime.datetime.now(),
                "CreationTime": datetime.datetime.now(),
                "CreatedBy": {},
                "LastModifiedTime": datetime.datetime.now(),
                "LastModifiedBy": {},
                "Parameters": {
                    "SageMaker.InstanceCount": {"NumberValue": 1.0},
                    "SageMaker.InstanceType": {"StringValue": "ml.t3.2xlarge"},
                    "SageMaker.VolumeSizeInGB": {"NumberValue": 30.0},
                },
                "InputArtifacts": {
                    "SageMaker.ImageUri": {
                        "Value": "311638508164.dkr.ecr.us-east-1.amazonaws.com/predict-flight-delay/components/preprocessing:9a3c5ffa"
                    },
                    "ground-truth": {
                        "Value": "s3://sagemaker-flight-delay-prediction-demo/preprocessing-2022-01-27-09-38-24-317/output/labels-test"
                    },
                    "predictions": {
                        "Value": "s3://sagemaker-flight-delay-prediction-demo/flight-delay-prediction-training-pipeline-31-bring-back-lightgbm-model-to-training/7s1cdjmwshwl/2022-01-27T09:46:40.248Z/predictions"
                    },
                },
                "OutputArtifacts": {
                    "metrics": {
                        "Value": "s3://sagemaker-flight-delay-prediction-demo/preprocessing-2022-01-27-09-38-24-331/output/metrics"
                    }
                },
                "Metrics": [],
            }
        )

        tc2 = TrialComponent.parse_obj(
            {
                "TrialComponentName": "pipelines-7s1cdjmwshwl-apply-best-model-7uo0cjtngi-aws-transform-job",
                "TrialComponentArn": "arn:aws:sagemaker:us-east-1:311638508164:experiment-trial-component/pipelines-7s1cdjmwshwl-apply-best-model-7uo0cjtngi-aws-transform-job",
                "DisplayName": "pipelines-7s1cdjmwshwl-apply-best-model-7uo0cjtngi-aws-transform-job",
                "Source": {
                    "SourceArn": "arn:aws:sagemaker:us-east-1:311638508164:transform-job/pipelines-7s1cdjmwshwl-apply-best-model-7uo0cjtngi",
                    "SourceType": "SageMakerTransformJob",
                },
                "Status": {
                    "PrimaryStatus": "Completed",
                    "Message": "Status: Completed, failure reason: null",
                },
                "StartTime": datetime.datetime.now(),
                "EndTime": datetime.datetime.now(),
                "CreationTime": datetime.datetime.now(),
                "CreatedBy": {},
                "LastModifiedTime": datetime.datetime.now(),
                "LastModifiedBy": {},
                "Parameters": {
                    "SageMaker.InstanceCount": {"NumberValue": 1.0},
                    "SageMaker.InstanceType": {"StringValue": "ml.c5.2xlarge"},
                    "SageMaker.ModelName": {
                        "StringValue": "pipelines-7s1cdjmwshwl-create-model-hvZh7eqxCd"
                    },
                    "SageMaker.ModelPrimary.DataUrl": {
                        "StringValue": "s3://sagemaker-flight-delay-prediction-demo/7s1cdjmwshwl-hp-tuni-gtlulL7vhj-001-f77268cf/output/model.tar.gz"
                    },
                    "SageMaker.ModelPrimary.Image": {
                        "StringValue": "311638508164.dkr.ecr.us-east-1.amazonaws.com/predict-flight-delay/components/lightgbm-serving:9a3c5ffa"
                    },
                },
                "InputArtifacts": {
                    "SageMaker.TransformInput": {
                        "MediaType": "text/csv",
                        "Value": "s3://sagemaker-flight-delay-prediction-demo/preprocessing-2022-01-27-09-38-24-317/output/featurized-test",
                    }
                },
                "OutputArtifacts": {
                    "SageMaker.TransformOutput": {
                        "Value": "s3://sagemaker-flight-delay-prediction-demo/flight-delay-prediction-training-pipeline-31-bring-back-lightgbm-model-to-training/7s1cdjmwshwl/2022-01-27T09:46:40.248Z/predictions"
                    }
                },
                "Metrics": [],
            }
        )

        trial = Trial(
            TrialArn="some_trial",
            TrialName="some_trial",
            TrialSource=Source(SourceArn="arn", SourceType="type"),
            Created=datetime.datetime.now(),
            Modified=datetime.datetime.now(),
            TrialComponents=[tc1, tc2],
        )

        experiment = Experiment.parse_obj(
            {
                "ExperimentArn": "arn:aws:sagemaker:us-east-1:311638508164:experiment/sd52eaxvt60k-hp-tuni-bsg5hjrhyx",
                "ExperimentName": "sd52eaxvt60k-hp-tuni-bsg5hjrhyx",
                "DisplayName": "sd52eaxvt60k-hp-tuni-bsg5hjrhyx",
                "ExperimentSource": {
                    "SourceArn": "arn:aws:sagemaker:us-east-1:311638508164:hyper-parameter-tuning-job/sd52eaxvt60k-hp-tuni-bsg5hjrhyx",
                    "SourceType": "SageMakerHyperParameterTuningJob",
                },
                "CreationTime": datetime.datetime.now(),
                "LastModifiedTime": datetime.datetime.now(),
                "Trials": [trial],
            }
        )

        yield experiment
