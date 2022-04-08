package batchjob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/pytimer/k8sutil/apply"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serialYaml "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/robfig/cron"
)

// defaultRunHistoryLimit is the default value to set SuccessfulRunHistoryLimit and FailedRunHistoryLimit to for creating scheduled jobs.
const defaultRunHistoryLimit = 5
const nameRegex = `^[a-z]([-a-z0-9]*[a-z0-9])?$`

const manifestFileSuffix = ".yaml"
const scheduledManifestFileSuffix = "_scheduled.yaml"

// goRoutineCreated tracks whether the go routine which handles suspending one run scheduled jobs.
var goRoutineCreated bool
// nonRepeatJobsSync stores scheduled job names to batch job manifest. 
// Used to keep track of scheduled jobs that should run only once (one run scheduled jobs).
var nonRepeatJobsSync sync.Map

// batchJobMetadata holds metadata of a batch job. 
type batchJobMetadata struct {
	// Name is name of the batch job or scheduled batch job.
	Name      string `yaml:"name" json:"name"`
	// Namespace is the namespace the job will be created on.
	Namespace string `yaml:"namespace"`
}

// batchJobSpecRestartPolicy holds fields related to restart policy of a batch job.
type batchJobSpecRestartPolicy struct {
	Type                             string `yaml:"type"`
	OnSubmissionFailureRetries       int32  `yaml:"onSubmissionFailureRetries,omitempty"`
	OnFailureRetries                 int32  `yaml:"onFailureRetries,omitempty"`
	OnSubmissionFailureRetryInterval int64  `yaml:"onSubmissionFailureRetryInterval,omitempty"`
	OnFailureRetryInterval           int64  `yaml:"onFailureRetryInterval,omitempty"`
}

// batchJobSpecDynamicAllocation holds fields related to Dynamic Allocation of a batch job.
type batchJobSpecDynamicAllocation struct {
	Enabled                bool  `yaml:"enabled"`
	InitialExecutors       int32 `yaml:"initialExecutors,omitempty"`
	MinExecutors           int32 `yaml:"minExecutors,omitempty"`
	MaxExecutors           int32 `yaml:"maxExecutors,omitempty"`
	ShuffleTrackingTimeout int64 `yaml:"shuffleTrackingTimeout,omitempty"`
}

// batchJobSpecSparkConf holds fields realted to the Spark Configuration of a batch job.
type batchJobSpecSparkConf struct {
	SparkJars                               string `yaml:"spark.jars,omitempty"`
	SparkJarsIvy                            string `yaml:"spark.jars.ivy,omitempty"`
	SparkSqlExtensions                      string `yaml:"spark.sql.extensions,omitempty"`
	SparkSqlCatalogSparkCatalog             string `yaml:"spark.sql.catalog.spark_catalog,omitempty"`
	SparkDeltaLogStoreClass                 string `yaml:"spark.delta.logStore.class,omitempty"`
	SparkDriverExtraJavaOptions             string `yaml:"spark.driver.extraJavaOptions,omitempty"`
	SparkExecutorExtraJavaOptions           string `yaml:"spark.executor.extraJavaOptions,omitempty"`
	SparkSqlWarehouseDir                    string `yaml:"spark.sql.warehouse.dir,omitempty"`
	SparkEventLogEnabled                    string `yaml:"spark.eventLog.enabled,omitempty"`
	SparkEventLogDir                        string `yaml:"spark.eventLog.dir,omitempty"`
	SparkHadoopFsS3aEndpoint                string `yaml:"spark.hadoop.fs.s3a.endpoint,omitempty"`
	SparkHadoopFsS3aAccessKey               string `yaml:"spark.hadoop.fs.s3a.access.key,omitempty"`
	SparkHadoopFsS3aSecretKey               string `yaml:"spark.hadoop.fs.s3a.secret.key,omitempty"`
	SparkHadoopFsS3aImpl                    string `yaml:"spark.hadoop.fs.s3a.impl,omitempty"`
	SparkHadoopFsS3aConnectionSslEnabled    string `yaml:"spark.hadoop.fs.s3a.connection.ssl.enabled,omitempty"`
	SparkHadoopHiveMetastoreUris            string `yaml:"spark.hadoop.hive.metastore.uris,omitempty"`
	SparkSqlSequoiadpMetaserviceUri         string `yaml:"spark.sql.sequoiadp.metaservice.uri,omitempty"`
	SparkHistoryFsLogDirectory              string `yaml:"spark.history.fs.logDirectory,omitempty"`
	SparkHistoryProvider                    string `yaml:"spark.history.provider,omitempty"`
	SparkKubernetesContainerImagePullPolicy string `yaml:"spark.kubernetes.container.image.pullPolicy,omitempty"`
}

// batchJobSpecSparkPodSpec holds fields which define common things for a Spark driver or executor pod.
type batchJobSpecSparkPodSpec struct {
	Cores          int32  `yaml:"cores" json:"cores"`
	CoreLimit      string `yaml:"coreLimit,omitempty"`
	Memory         string `yaml:"memory" json:"memory"`
	ServiceAccount string `yaml:"serviceAccount,omitempty"`
}

// batchJobSpecDriver holds specification on the Spark Driver.
type batchJobSpecDriver struct {
	batchJobSpecSparkPodSpec `yaml:",inline"`
	JavaOptions              string `yaml:"javaOptions,omitempty"`
}

// batchJobSpecExecutor holds specification on the Spark Executor
type batchJobSpecExecutor struct {
	batchJobSpecSparkPodSpec `yaml:",inline"`
	Instances                int32  `yaml:"instances,omitempty" json:"instances"`
	JavaOptions              string `yaml:"javaOptions,omitempty"`
}

// batchJobSpec holds specification for a batch job and scheduled batch job.
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

// scheduledBatchJobSpec holds specification of a scheduled batch job
type scheduledBatchJobSpec struct {
	Schedule                    string              `yaml:"schedule"`
	Suspend                     bool                `yaml:"suspend"`
	ConcurrencyPolicy           string              `yaml:"concurrencyPolicy"`
	SuccessfulRunHistoryLimit   int32               `yaml:"successfulRunHistoryLimit,omitempty"`
	FailedRunHistoryLimit       int32               `yaml:"failedRunHistoryLimit,omitempty"`
	Template                    batchJobSpec        `yaml:"template"`
}

// batchJobManifest holds fields for creating a batch job using a yaml manifest.
type batchJobManifest struct {
	ApiVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	Metadata   batchJobMetadata `yaml:"metadata" json:"metadata"`
	Spec       batchJobSpec     `yaml:"spec" json:"spec"`
}

// scheduledBatchJobManifest holds fields for creating a scheduled batch job using a yaml manifest.
type scheduledBatchJobManifest struct {
	ApiVersion string                   `yaml:"apiVersion"`
	Kind       string                   `yaml:"kind"`
	Metadata   batchJobMetadata         `yaml:"metadata" json:"metadata"`
	Spec       scheduledBatchJobSpec    `yaml:"spec" json:"spec"`
}

// batchJobSchedule holds fields in a request to create a scheduled batch job.
type batchJobSchedule struct {
	CronSchedule                    string
	ConcurrencyPolicy               string
	Suspend                         bool
	SuccessfulRunHistoryLimit       int32
	FailedRunHistoryLimit           int32
	RunHistoryLimit                 int32
}

// batchJobRequest holds fields in a request to create a batch job or scheduled batch job.
// To create a batch job, Metadata and Spec are needed.
// To create a scheduled batch job, Metadata, Spec, and Schedule are needed. OneRunScheduledJob is optional.
type batchJobRequest struct {
	Metadata            batchJobMetadata `yaml:"metadata" json:"metadata"`
	Spec                batchJobSpec     `yaml:"spec" json:"spec"`
	OneRunScheduledJob  bool             `yaml:"oneRunScheduledJob" json:"oneRunScheduledJob"`
	Schedule            batchJobSchedule `yaml:"schedule,omitempty" json:"schedule,omitempty"`
}

// serviceResponse holds the response given to the user after a request is done.
type serviceResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status int    `json:"Status"`
	Output string `json:"Output"`
}

// createBatchJobMetadata populates batch job metadata with values from SPARKJOB_CONFS.
// Currently populates the metadata namespace.
func createBatchJobMetadata(jobMetadataTemplate *batchJobMetadata) {
	jobMetadataTemplate.Namespace = SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]
}

// createBatchJobSpecSparkConf populates batch job Spark conf with values from SPARKJOB_CONFS.
func createBatchJobSpecSparkConf(batchJobSpecSparkConf *batchJobSpecSparkConf) {
	batchJobSpecSparkConf.SparkJars = SPARKJOB_SPARKCONFS["spark.jars"]
	batchJobSpecSparkConf.SparkJarsIvy = SPARKJOB_SPARKCONFS["spark.jars.ivy"]
	batchJobSpecSparkConf.SparkSqlExtensions = SPARKJOB_SPARKCONFS["spark.sql.extensions"]
	batchJobSpecSparkConf.SparkSqlCatalogSparkCatalog = SPARKJOB_SPARKCONFS["spark.sql.catalog.spark_catalog"]
	batchJobSpecSparkConf.SparkDeltaLogStoreClass = SPARKJOB_SPARKCONFS["spark.delta.logStore.class"]
	batchJobSpecSparkConf.SparkSqlWarehouseDir = SPARKJOB_SPARKCONFS["spark.sql.warehouse.dir"]
	batchJobSpecSparkConf.SparkEventLogEnabled = SPARKJOB_SPARKCONFS["spark.eventLog.enabled"]
	batchJobSpecSparkConf.SparkEventLogDir = SPARKJOB_SPARKCONFS["spark.eventLog.dir"]
	batchJobSpecSparkConf.SparkHadoopFsS3aEndpoint = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.endpoint"]
	batchJobSpecSparkConf.SparkHadoopFsS3aAccessKey = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.access.key"]
	batchJobSpecSparkConf.SparkHadoopFsS3aSecretKey = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.secret.key"]
	batchJobSpecSparkConf.SparkHadoopFsS3aImpl = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.impl"]
	batchJobSpecSparkConf.SparkHadoopFsS3aConnectionSslEnabled = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.connection.ssl.enabled"]
	batchJobSpecSparkConf.SparkDriverExtraJavaOptions = SPARKJOB_SPARKCONFS["spark.driver.extraJavaOptions"]
	batchJobSpecSparkConf.SparkExecutorExtraJavaOptions = SPARKJOB_SPARKCONFS["spark.executor.extraJavaOptions"]
	batchJobSpecSparkConf.SparkKubernetesContainerImagePullPolicy = SPARKJOB_SPARKCONFS["spark.kubernetes.container.image.pullPolicy"]
	batchJobSpecSparkConf.SparkHadoopHiveMetastoreUris = SPARKJOB_SPARKCONFS["spark.hadoop.hive.metastore.uris"]
	batchJobSpecSparkConf.SparkSqlSequoiadpMetaserviceUri = SPARKJOB_SPARKCONFS["spark.sql.sequoiadp.metaservice.uri"]
	batchJobSpecSparkConf.SparkHistoryFsLogDirectory = SPARKJOB_SPARKCONFS["spark.history.fs.logDirectory"]
	batchJobSpecSparkConf.SparkHistoryProvider = SPARKJOB_SPARKCONFS["spark.history.provider"]
}

// createBatchJobSpecRestartPolicy populates batch job restart policy with values from SPARKJOB_CONFS.
func createBatchJobSpecRestartPolicy(jobSpecRestartPolicy *batchJobSpecRestartPolicy) {
	// TODO: The default value is currently used, and it will be open to users on RESTful API in the future.
	jobSpecRestartPolicy.Type = SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_TYPE"]

	onFailureRetries, _ := strconv.ParseInt(SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_ONFAILURE_RETRIES"], 10, 64)
	onFailureRetryInterval, _ := strconv.ParseInt(SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_ONFAILURE_RETRY_INTERVAL"], 10, 64)
	onSubmissionRetries, _ := strconv.ParseInt(SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_ONSUBMISSION_RETRIES"], 10, 64)
	onSubmissionRetryInterval, _ := strconv.ParseInt(SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_ONSUBMISSION_RETRY_INTERVAL"], 10, 64)

	jobSpecRestartPolicy.OnFailureRetries = int32(onFailureRetries)
	jobSpecRestartPolicy.OnFailureRetryInterval = onFailureRetryInterval
	jobSpecRestartPolicy.OnSubmissionFailureRetries = int32(onSubmissionRetries)
	jobSpecRestartPolicy.OnSubmissionFailureRetryInterval = onSubmissionRetryInterval
}

// createBatchJobSpecDriver populates batch job driver with values from SPARKJOB_CONFS.
func createBatchJobSpecDriver(jobSpecDriver *batchJobSpecDriver) {
	jobSpecDriver.ServiceAccount = SPARKJOB_CONFS["SPARKJOB_SERVICEACCOUNT"]
	jobSpecDriver.JavaOptions = SPARKJOB_CONFS["SPARKJOB_DRIVER_JAVAOPTIONS"]
}

// createBatchJobSpecExecutor populates batch job executor with values from SPARKJOB_CONFS.
func createBatchJobSpecExecutor(jobSpecExecutor *batchJobSpecExecutor) {
	jobSpecExecutor.JavaOptions = SPARKJOB_CONFS["SPARKJOB_EXECUTOR_JAVAOPTIONS"]
}

// createBatchJobSpec populates a batch job spec with default values and values from SPARKJOB_CONFS.
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

// createScheduledBatchJobSpec populates the scheduledBatchJobSpec struct with values in batchJobSpec and batchJobSchedule from a batchJobRequest.
func createScheduledBatchJobSpec(jobSpecTemplate *scheduledBatchJobSpec, spec batchJobSpec, schedule batchJobSchedule) {
	jobSpecTemplate.Schedule = schedule.CronSchedule
	jobSpecTemplate.Template = spec
	createBatchJobSpec(&jobSpecTemplate.Template)
	jobSpecTemplate.Suspend = schedule.Suspend
	jobSpecTemplate.ConcurrencyPolicy = schedule.ConcurrencyPolicy
	// check whether RunHistoryLimit is non-zero. Use it as value for both SuccessfulRunHistoryLimit and FailedRunHistoryLimit if it is.
	if schedule.RunHistoryLimit != 0 {
		jobSpecTemplate.SuccessfulRunHistoryLimit = schedule.RunHistoryLimit
		jobSpecTemplate.FailedRunHistoryLimit = schedule.RunHistoryLimit
	} else {
		// set SuccessfulRunHistoryLimit and/or FailedRunHistoryLimit given value in request. Set to default if not given.
		if jobSpecTemplate.SuccessfulRunHistoryLimit = schedule.SuccessfulRunHistoryLimit; schedule.SuccessfulRunHistoryLimit == 0 {
			jobSpecTemplate.SuccessfulRunHistoryLimit = defaultRunHistoryLimit
		}
		if jobSpecTemplate.FailedRunHistoryLimit = schedule.FailedRunHistoryLimit; schedule.FailedRunHistoryLimit == 0 {
			jobSpecTemplate.FailedRunHistoryLimit = defaultRunHistoryLimit
		}
	}
}

// createBatchJobManifest creates a batch job manifest used to create a SparkApplication.
func createBatchJobManifest(job *batchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	createBatchJobMetadata(&job.Metadata)
	createBatchJobSpec(&job.Spec)
}

// createScheduledJobManifest creates a scheduled batch job manifest used to create a ScheduledSparkApplication
func createScheduledJobManifest(job *scheduledBatchJobManifest, spec batchJobSpec, schedule batchJobSchedule) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "ScheduledSparkApplication"
	createBatchJobMetadata(&job.Metadata)
	createScheduledBatchJobSpec(&job.Spec, spec, schedule)
}

// createJob creates a yaml manifest file, and uses it to create a SparkApplication on the k8s cluster.
func createJob(job batchJobManifest) (response serviceResponse) {
	// create batch job maniest
	createBatchJobManifest(&job)
	sparkJobManifest, err := yaml.Marshal(&job)
	if err != nil {
		log.Println("Unable to encode batch job into yaml. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to encode batch job into yaml. err: " + err.Error()
		return
	}
	// print for logging purposes
	fmt.Println(string(sparkJobManifest))

	// save manifest to ss3
	jobName := job.Metadata.Name
	fileName := jobName + manifestFileSuffix
	uploadResponse := uploadFile(S3_BUCKET_NAME, MANIFEST, fileName, false,
		bytes.NewReader(sparkJobManifest))
	if uploadResponse.Status != http.StatusOK {
		log.Println("Unable to save batch job manifest to ss3. err: ", uploadResponse.Output)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to save batch job manifest to ss3. err: " + uploadResponse.Output
		return
	}

	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an dynamic client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an dynamic client. err: " + err.Error()
		return
	}

	// specify the schema for creating a SparkApplication k8s object
	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}

	// decode YAML contents into Unstructured struct which is used to create the deployment
	var decUnstructured = serialYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	deployment := &unstructured.Unstructured{}
    _, _, err = decUnstructured.Decode(sparkJobManifest, nil, deployment)
    if err != nil {
        response.Status = http.StatusInternalServerError
		response.Output = "Unable to Create SparkApplication. err: " + err.Error()
		return
    }

	// Create the SparkApplication using the yaml
	result, err := dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Create(context.TODO(), deployment, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		log.Println("Unable to Create SparkApplication. err: ", err.Error())
		response.Status = http.StatusConflict
		response.Output = "Unable to Create SparkApplication. err: " + err.Error()
		return
	} else if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to Create SparkApplication. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to Create SparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Created sparkapplication "+ result.GetName()
	return
}

// createScheduledJob creates a yaml manifest file, and uses it to create a SparkApplication on the k8s cluster.
func createScheduledJob(job scheduledBatchJobManifest, spec batchJobSpec, schedule batchJobSchedule, nonRepeat bool) (response serviceResponse) {
	createScheduledJobManifest(&job, spec, schedule)
	sparkJobManifest, err := yaml.Marshal(&job)
	if err != nil {
		log.Println("Unable to encode batch job into yaml. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to encode batch job into yaml. err: " + err.Error()
		return
	}
	fmt.Println(string(sparkJobManifest))

	// save manifest to ss3
	jobName := job.Metadata.Name
	fileName := jobName + scheduledManifestFileSuffix
	uploadResponse := uploadFile(S3_BUCKET_NAME, MANIFEST, fileName, false,
		bytes.NewReader(sparkJobManifest))
	if uploadResponse.Status != http.StatusOK {
		log.Println("Unable to save batch job manifest to ss3. err: ", uploadResponse.Output)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to save batch job manifest to ss3. err: " + uploadResponse.Output
		return
	}
	response = applyManifest(sparkJobManifest, jobName)
	// if it is a non-repeating scheuled job, put in map to be tracked 
	if nonRepeat {
		// create go routine if not created yet
		if !goRoutineCreated {
			go nonRepeatScheduledJobCleanup()
			goRoutineCreated = true
		}
		nonRepeatJobsSync.Store(job.Metadata.Name, &job)
	}

	return
}

// applyManifest does "kubectl apply" using the given manifest file.
func applyManifest(sparkJobManifest []byte, jobName string) (response serviceResponse) {
	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an dynamic client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an dynamic client. err: " + err.Error()
		return
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		log.Println("Unable to create an discovery client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an discovery client. err: " + err.Error()
		return
	}

	applyOptions := apply.NewApplyOptions(dynamicClient, discoveryClient)
	if err := applyOptions.Apply(context.TODO(), sparkJobManifest); err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to apply the batch job manifest. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to read apply the batch job manifest. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "ScheduledSparkApplication " + jobName + " created"
	return
}

// nonRepeatScheduledJobCleanup periodically checks nonRepeatJobsSync map of scheduled jobs that should only run once.
// Will 
func nonRepeatScheduledJobCleanup() {
	ticker := time.NewTicker(15 * time.Second)
	i := 1
	// create the in-cluster config for GET
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		goRoutineCreated = false
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		goRoutineCreated = false
		return
	}
	for _ = range ticker.C {
		log.Println("Tock", i)
		i = i + 1
		nonRepeatJobsSync.Range(func(k, v interface{}) bool {
			// GET ScheduledSparkApplication with job name
			jobName := k.(string)
			log.Println("Checking if JOBNAME:", jobName, "has run.")
			res := ScheduledSparkApplication{}
			err := clientset.RESTClient().Get().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("ScheduledSparkApplications").
				Name(jobName).
				Do(context.TODO()).
				Into(&res)
			if err != nil {
				log.Println("Unable to get ScheduledSparkApplication. err: ", err)
				nonRepeatJobsSync.Delete(jobName)
				return true
			}
			if res.Status.LastRunName != "" {
				// job has run, suspend it so no future runs happen
				jobManifest := v.(*scheduledBatchJobManifest)
				jobManifest.Spec.Suspend = true
				log.Println("Suspending Non-repeating scheduled job:", jobName)
				sparkJobManifest, err := yaml.Marshal(jobManifest)
				if err != nil {
					log.Println("ERROR: Unable to encode batch job into yaml. err: ", err)
					return true
				}
				curTime := strconv.FormatInt(time.Now().Unix(), 10)

				// save manifest to ss3
				fileName := jobName + "_" + curTime + scheduledManifestFileSuffix
				uploadResponse := uploadFile(S3_BUCKET_NAME, MANIFEST, fileName, false,
					bytes.NewReader(sparkJobManifest))
				if uploadResponse.Status != http.StatusOK {
					log.Println("ERROR: Unable to save batch job manifest to ss3. err: ", uploadResponse.Output)
					return true
				}
				
				response := applyManifest(sparkJobManifest, jobName)
				if response.Status == http.StatusInternalServerError {
					log.Println("ERROR: Something when wrong when suspending the job", response.Output)
					return true
				}
				log.Println("DONE suspending Non-repeating scheduled job:", jobName)
				nonRepeatJobsSync.Delete(jobName)
			}
			return true
		})
	}
}

// createBatchJob is the handler for POST: /job
// It creates a SparkApplication object with the spec given in the request body.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func createBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit create jobs endpoint")
	decoder := json.NewDecoder(r.Body)
	// Get request bodys
	var batchJobReq batchJobRequest
	if err := decoder.Decode(&batchJobReq); err != nil {
		log.Println("Cannot decode request body", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body:"+ err.Error()))
		return
	}
	// error 400 checking for mandatory fields and valid values
	if batchJobReq.Metadata.Name == "" {
		log.Println("Missing metadata: name")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing metadata: name"))
		return
	} else if match, _ := regexp.MatchString(nameRegex, batchJobReq.Metadata.Name); !match {
		log.Println("Invalid name: " + batchJobReq.Metadata.Name + ". must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - " + "Invalid name: " + batchJobReq.Metadata.Name + ". must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')"))
		return
	} else if len(batchJobReq.Metadata.Name) >= 64 {
		log.Println("Invalid name: " + batchJobReq.Metadata.Name + ": must be no more than 63 characters.")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid name: " + batchJobReq.Metadata.Name + ": must be no more than 63 characters."))
		return
	} else if batchJobReq.Spec.Type == "" || batchJobReq.Spec.MainClass == "" || batchJobReq.Spec.MainApplicationFile == "" {
		log.Println("Missing one of spec parameters: type, mainClass, or mainApplicationFile")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of spec parameters: type, mainClass, or mainApplicationFile"))
		return
	} else if batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Memory == "" {
		log.Println("Missing one of driver parameters: cores, or memory")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of driver parameters: cores, or memor"))
		return
	} else if batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Memory == "" {
		log.Println("Missing one of executor parameters: cores, or memory")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of executor parameters: cores, or memory"))
		return
	}

	var response []byte
	var err error
	// create SparkApplication
	log.Println("Creating SparkApplication")
	var createReq batchJobManifest
	createReq.Metadata = batchJobReq.Metadata
	createReq.Spec = batchJobReq.Spec
	// create batch job
	createJobResponse := createJob(createReq)
	if createJobResponse.Status != http.StatusOK {
		log.Println("Error creating job: ", createJobResponse.Output)
		w.WriteHeader(createJobResponse.Status)
		w.Write([]byte(strconv.Itoa(createJobResponse.Status) + " - Error creating job: " + createJobResponse.Output))
		return
	}
	// encode response
	response, err = json.Marshal(createJobResponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Unable to encode a response:" + err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// createScheduledBatchJob is the handler for POST: /scheduledjob
// It creates a ScheduledSparkApplication object with the spec given in the request body.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func createScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit create jobs endpoint")
	decoder := json.NewDecoder(r.Body)
	// Get request
	var batchJobReq batchJobRequest
	if err := decoder.Decode(&batchJobReq); err != nil {
		log.Println("Cannot decode request body", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body:"+ err.Error()))
		return
	}
	// error 400 checking for mandatory fields and valid values
	if  batchJobReq.Metadata.Name == "" {
		log.Println("Missing metadata: name")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing metadata: name"))
		return
	} else if match, _ := regexp.MatchString(nameRegex, batchJobReq.Metadata.Name); !match{
		log.Println("Invalid name: " + batchJobReq.Metadata.Name + ". must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - " + "Invalid name: " + batchJobReq.Metadata.Name + ". must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')"))
		return
	} else if len(batchJobReq.Metadata.Name) >= 64 {
		log.Println("Invalid name: " + batchJobReq.Metadata.Name + ": must be no more than 63 characters.")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid name: " + batchJobReq.Metadata.Name + ": must be no more than 63 characters."))
		return
	} else if batchJobReq.Spec.Type == "" || batchJobReq.Spec.MainClass == "" || batchJobReq.Spec.MainApplicationFile == "" {
		log.Println("Missing one of spec parameters: type, mainClass, or mainApplicationFile")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of spec parameters: type, mainClass, or mainApplicationFile"))
		return
	} else if batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Memory == "" {
		log.Println("Missing one of driver parameters: cores, or memory")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of driver parameters: cores, or memory"))
		return
	} else if batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Memory == "" {
		log.Println("Missing one of executor parameters: cores, or memory")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing one of executor parameters: cores, or memory"))
		return
	} else if batchJobReq.Schedule.ConcurrencyPolicy == "" {
		log.Println("Missing schedule parameters: concurrencyPolicy")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing schedule parameters: concurrencyPolicy"))
		return
	}

	// error 400 for invalid format and input
	_, err := cron.ParseStandard(batchJobReq.Schedule.CronSchedule)
	if err != nil {
		log.Println("Invalid cron schedule format: " + err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid cron schedule format: " + err.Error()))
		return
	}
	// check concurrencyPolicy is one of "Allow", "Forbid", "Replace". Check 
	if batchJobReq.Schedule.ConcurrencyPolicy != "Allow" && batchJobReq.Schedule.ConcurrencyPolicy != "Forbid" && batchJobReq.Schedule.ConcurrencyPolicy != "Replace" {
		log.Println("Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace"))
		return
	} else if batchJobReq.Schedule.RunHistoryLimit < 0 || batchJobReq.Schedule.SuccessfulRunHistoryLimit < 0 || batchJobReq.Schedule.FailedRunHistoryLimit < 0 {
		log.Println("Invalid HistoryLimit, RunHistoryLimit/SuccessfulRunHistoryLimit/FailedRunHistoryLimit should be greater than 0")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid HistoryLimit, RunHistoryLimit/SuccessfulRunHistoryLimit/FailedRunHistoryLimit should be greater than 0"))
		return
	} else if batchJobReq.Schedule.RunHistoryLimit != 0 && (batchJobReq.Schedule.SuccessfulRunHistoryLimit != 0 || batchJobReq.Schedule.FailedRunHistoryLimit != 0) {
		log.Println("Not allowed to set RunHistoryLimit and (SuccessfulRunHistoryLimit + FailedRunHistoryLimit). Must set either RunHistoryLimit or (SuccessfulRunHistoryLimit + FailedRunHistoryLimit)")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Not allowed to set RunHistoryLimit and (SuccessfulRunHistoryLimit + FailedRunHistoryLimit). Must set either RunHistoryLimit or (SuccessfulRunHistoryLimit + FailedRunHistoryLimit)"))
		return
	}

	var response []byte
	// create ScheduledSparkApplication
	log.Println("Creating ScheduledSparkApplication")
	var createReq scheduledBatchJobManifest
	createReq.Metadata = batchJobReq.Metadata
	createJobResponse := createScheduledJob(createReq, batchJobReq.Spec, batchJobReq.Schedule, batchJobReq.OneRunScheduledJob)
	// error handling createScheduledJob
	if createJobResponse.Status != http.StatusOK {
		log.Println("Error creating scheduled job: ", createJobResponse.Output)
		w.WriteHeader(createJobResponse.Status)
		w.Write([]byte(strconv.Itoa(createJobResponse.Status) + " - Error creating scheduled job: " + createJobResponse.Output))
		return
	}
	// encode response
	response, err = json.Marshal(createJobResponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Unable to encode a response:" + err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
