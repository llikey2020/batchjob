Helm chart for batch job service

helm install batch-job batch-job/

For configuring the spark jobs, override the default configuration values under sparkJob in the helm chart.

__Important Values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `sparkJob.namespace` | The K8s namespace where spark jobs are launched | `default` |
| `sparkJob.serviceAccount` | The K8s service account used by spark driver/executor pods | `spark` |
| `sparkJob.image` | The spark image used | `gitlab.planetrover.io:5050/sequoiadp/spark:latest` |

Currently, the only spark configurations that are supported are the ones shown in the default values.yaml file under `sparkJob.sparkConf`. Hence, additional spark configurations will not take effect.
