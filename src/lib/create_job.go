package batchjob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type batchJobMetadata struct {
	Name      string `yaml:"name" json:"name"`
	Namespace string `yaml:"namespace"`
}

type batchJobSpecRestartPolicy struct {
	Type                             string `yaml:"type"`
	OnSubmissionFailureRetries       int32  `yaml:"onSubmissionFailureRetries,omitempty"`
	OnFailureRetries                 int32  `yaml:"onFailureRetries,omitempty"`
	OnSubmissionFailureRetryInterval int64  `yaml:"onSubmissionFailureRetryInterval,omitempty"`
	OnFailureRetryInterval           int64  `yaml:"onFailureRetryInterval,omitempty"`
}

type batchJobSpecDynamicAllocation struct {
	Enabled                bool  `yaml:"enabled"`
	InitialExecutors       int32 `yaml:"initialExecutors,omitempty"`
	MinExecutors           int32 `yaml:"minExecutors,omitempty"`
	MaxExecutors           int32 `yaml:"maxExecutors,omitempty"`
	ShuffleTrackingTimeout int64 `yaml:"shuffleTrackingTimeout,omitempty"`
}

type batchJobSpecSparkConf struct {
	SparkJarsIvy                  string `yaml:"spark.jars.ivy,omitempty"`
	SparkSqlExtensions            string `yaml:"spark.sql.extensions,omitempty"`
	SparkSqlCatalogSparkCatalog   string `yaml:"spark.sql.catalog.spark_catalog,omitempty"`
	SparkDeltaLogStoreClass       string `yaml:"spark.delta.logStore.class,omitempty"`
	SparkDriverExtraJavaOptions   string `yaml:"spark.driver.extraJavaOptions,omitempty"`
	SparkExecutorExtraJavaOptions string `yaml:"spark.executor.extraJavaOptions,omitempty"`
	SparkSqlWarehouseDir          string `yaml:"spark.sql.warehouse.dir,omitempty"`
	SparkEventLogEnabled          string `yaml:"spark.eventLog.enabled,omitempty"`
	SparkEventLogDir              string `yaml:"spark.eventLog.dir,omitempty"`
}

type batchJobSpecSparkPodSpec struct {
	Cores          int32  `yaml:"cores" json:"cores"`
	CoreLimit      string `yaml:"coreLimit,omitempty"`
	Memory         string `yaml:"memory" json:"memory"`
	ServiceAccount string `yaml:"serviceAccount,omitempty"`
}

type batchJobSpecDriver struct {
	batchJobSpecSparkPodSpec `yaml:",inline"`
	JavaOptions              string `yaml:"javaOptions,omitempty"`
}

type batchJobSpecExecutor struct {
	batchJobSpecSparkPodSpec `yaml:",inline"`
	Instances                int32  `yaml:"instances,omitempty" json:"instances"`
	JavaOptions              string `yaml:"javaOptions,omitempty"`
}

type batchJobSpec struct {
	Type                string                        `yaml:"type" json:"type"`
	Mode                string                        `yaml:"mode"`
	Image               string                        `yaml:"image"`
	ImagePullSecrets    []string                      `yaml:"imagePullSecrets,omitempty"`
	ImagePullPolicy     string                        `yaml:"imagePullPolicy,omitempty"`
	MainClass           string                        `yaml:"mainClass" json:"mainClass"`
	MainApplicationFile string                        `yaml:"mainApplicationFile" json:"mainApplicationFile"`
	Arguments           []string                      `yaml:"arguments,omitempty" json:"arguments,omitempty"`
	SparkVersion        string                        `yaml:"sparkVersion"`
	RestartPolicy       batchJobSpecRestartPolicy     `yaml:"restartPolicy"`
	DynamicAllocation   batchJobSpecDynamicAllocation `yaml:"dynamicAllocation,omitempty"`
	SparkConf           batchJobSpecSparkConf         `yaml:"sparkConf"`
	Driver              batchJobSpecDriver            `yaml:"driver" json:"driver"`
	Executor            batchJobSpecExecutor          `yaml:"executor" json:"executor"`
}

type batchJobManifest struct {
	ApiVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	Metadata   batchJobMetadata `yaml:"metadata" json:"metadata"`
	Spec       batchJobSpec     `yaml:"spec" json:"spec"`
}

type serviceResponse struct {
	Status int    `json:"Status"`
	Output string `json:"Output"`
}

func createBatchJobMetadata(jobMetadataTemplate *batchJobMetadata) {
	jobMetadataTemplate.Namespace = SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]
}

func createBatchJobSpecSparkConf(batchJobSpecSparkConf *batchJobSpecSparkConf) {
	batchJobSpecSparkConf.SparkJarsIvy = SPARKJOB_SPARKCONFS["spark.jars.ivy"]
	batchJobSpecSparkConf.SparkSqlExtensions = SPARKJOB_SPARKCONFS["spark.sql.extensions"]
	batchJobSpecSparkConf.SparkSqlCatalogSparkCatalog = SPARKJOB_SPARKCONFS["spark.sql.catalog.spark_catalog"]
	batchJobSpecSparkConf.SparkDeltaLogStoreClass = SPARKJOB_SPARKCONFS["spark.delta.logStore.class"]
	batchJobSpecSparkConf.SparkSqlWarehouseDir = SPARKJOB_SPARKCONFS["spark.sql.warehouse.dir"]
	batchJobSpecSparkConf.SparkEventLogEnabled = SPARKJOB_SPARKCONFS["spark.eventLog.enabled"]
	batchJobSpecSparkConf.SparkEventLogDir = SPARKJOB_SPARKCONFS["spark.eventLog.dir"]
}

func createBatchJobSpecRestartPolicy(jobSpecRestartPolicy *batchJobSpecRestartPolicy) {
	jobSpecRestartPolicy.Type = SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_TYPE"]
}

func createBatchJobSpecDriver(jobSpecDriver *batchJobSpecDriver) {
	jobSpecDriver.ServiceAccount = SPARKJOB_CONFS["SPARKJOB_SERVICEACCOUNT"]
	jobSpecDriver.JavaOptions = SPARKJOB_CONFS["SPARKJOB_DRIVER_JAVAOPTIONS"]
}

func createBatchJobSpecExecutor(jobSpecExecutor *batchJobSpecExecutor) {
	jobSpecExecutor.JavaOptions = SPARKJOB_CONFS["SPARKJOB_EXECUTOR_JAVAOPTIONS"]
}

func createBatchJobSpec(jobSpecTemplate *batchJobSpec) {
	jobSpecTemplate.Mode = "cluster"
	jobSpecTemplate.Image = SPARKJOB_CONFS["SPARKJOB_IMAGE"]
	jobSpecTemplate.ImagePullSecrets = SPARKJOB_IMAGEPULLSECRETS
	jobSpecTemplate.ImagePullPolicy = SPARKJOB_CONFS["SPARKJOB_IMAGEPULLPOLICY"]
	jobSpecTemplate.SparkVersion = SPARKJOB_CONFS["SPARKJOB_SPARKVERSION"]
	createBatchJobSpecRestartPolicy(&jobSpecTemplate.RestartPolicy)
	createBatchJobSpecSparkConf(&jobSpecTemplate.SparkConf)
	createBatchJobSpecDriver(&jobSpecTemplate.Driver)
	createBatchJobSpecExecutor(&jobSpecTemplate.Executor)
}

func createBatchJobManifest(job *batchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	createBatchJobMetadata(&job.Metadata)
	createBatchJobSpec(&job.Spec)
}

func createJob(job batchJobManifest) (response serviceResponse) {
	createBatchJobManifest(&job)
	sparkJobManifest, err := yaml.Marshal(&job)
	if err != nil {
		log.Println("Unable to encode batch job into yaml. err: ", err)
	}
	fmt.Println(string(sparkJobManifest))
	curTime := strconv.FormatInt(time.Now().Unix(), 10)
	sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
	err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, 0644)
	if err != nil {
		log.Println("Unable to write batch job manifest to file. err: ", err)
	}
	cmd := exec.Command("sparkctl", "create", sparkJobManifestFile, "--namespace="+SPARKJOB_CONFS["SPARKJOB_NAMESPACE"])
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		response.Status = 1
		log.Println("ERROR:\n", err, out.String())
	}
	response.Status = 0
	response.Output = out.String()
	return
}

func createBatchJob(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit create jobs endpoint")
	decoder := json.NewDecoder(r.Body)
	var createReq batchJobManifest
	if err := decoder.Decode(&createReq); err != nil {
		log.Println("Cannot decode request body", err)
		return
	}
	createJobReponse := createJob(createReq)
	response, err := json.Marshal(createJobReponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
