package batchjob

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"context"
	"sync"
	"regexp"
	"errors"

	"gopkg.in/yaml.v2"

	"github.com/pytimer/k8sutil/apply"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	serialYaml "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/robfig/cron"
)

// defaultRunHistoryLimit is the default value to set SuccessfulRunHistoryLimit and FailedRunHistoryLimit to for creating scheduled jobs.
const defaultRunHistoryLimit = 5
// nameRegex is the regex used by Kubernetes to check for valid object name
const nameRegex = `^[a-z]([-a-z0-9]*[a-z0-9])?$`
// nameMaxCharCount is the max number of characters in a Kuberenetes object's name
const nameMaxCharCount = 63
// formatIntBase10 is used to convert an integer of the current unix time to a string in base 10
const formatIntBase10 = 10
// represents read/write permissions in the file system (-rw-r--r--).
const fsPerms = 0644

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
	jobSpecRestartPolicy.Type = SPARKJOB_CONFS["SPARKJOB_RESTARTPOLICY_TYPE"]
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
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to encode batch job into yaml. err: " + err.Error()
		return
	}
	// print for logging purposes
	logInfo("Creating SparkApplication with manifest: " + string(sparkJobManifest))
	// write manifest into yaml file
	curTime := strconv.FormatInt(time.Now().Unix(), formatIntBase10)
	sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
	err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, fsPerms)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to write batch job manifest to file. err: " + err.Error()
		return
	}

	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an dynamic client. err: " + err.Error()
		return
	}

	// specify the schema for creating a SparkApplication k8s object
	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	// read spark job manifest yaml into a byte[]
	content, err := ioutil.ReadFile(sparkJobManifestFile)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to read batch job manifest to file. err: " + err.Error()
		return
	}
	// decode YAML contents into Unstructured struct which is used to create the deployment
	var decUnstructured = serialYaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	deployment := &unstructured.Unstructured{}
    _, _, err = decUnstructured.Decode(content, nil, deployment)
    if err != nil {
        response.Status = http.StatusInternalServerError
		response.Output = "Unable to Create SparkApplication. err: " + err.Error()
		return
    }

	// Create the SparkApplication using the yaml
	result, err := dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.Output = "Unable to get SparkApplications. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Created sparkapplication: "+ result.GetName()
	return
}

// createScheduledJob creates a yaml manifest file, and uses it to create a SparkApplication on the k8s cluster.
func createScheduledJob(job scheduledBatchJobManifest, spec batchJobSpec, schedule batchJobSchedule, nonRepeat bool) (response serviceResponse) {
	createScheduledJobManifest(&job, spec, schedule)
	sparkJobManifest, err := yaml.Marshal(&job)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to encode batch job into yaml. err: " + err.Error()
		return
	}
	logInfo("Creating ScheduledSparkApplication with manifest: " + string(sparkJobManifest))
	curTime := strconv.FormatInt(time.Now().Unix(), formatIntBase10)
	sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
	err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, fsPerms)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to write batch job manifest to file. err: " + err.Error()
		return
	}
	response = applyManifest(sparkJobManifestFile, job.Metadata.Name)
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
func applyManifest(sparkJobManifestFile string, jobName string) (response serviceResponse) {
	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an dynamic client. err: " + err.Error()
		return
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an discovery client. err: " + err.Error()
		return
	}
	// read spark job manifest yaml into a byte[] and apply
	content, err := ioutil.ReadFile(sparkJobManifestFile)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to read batch job manifest to file. err: " + err.Error()
		return
	}
	applyOptions := apply.NewApplyOptions(dynamicClient, discoveryClient)
	if err := applyOptions.Apply(context.TODO(), content); err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.Output = "Unable to read apply the batch job manifest to file. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Created ScheduledSparkApplication: " + jobName
	return
}

// nonRepeatScheduledJobCleanup periodically checks nonRepeatJobsSync map of scheduled jobs that should only run once.
// Will update 
func nonRepeatScheduledJobCleanup() {
	ticker := time.NewTicker(15 * time.Second)
	i := 1
	// create the in-cluster config for GET
	config, err := rest.InClusterConfig()
	if err != nil {
		logError("Unable to create an in-cluster config. err: " + err.Error())
		goRoutineCreated = false
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logError("Unable to create an kubernetes client. err: " + err.Error())
		goRoutineCreated = false
		return
	}
	for _ = range ticker.C {
		i = i + 1
		nonRepeatJobsSync.Range(func(k, v interface{}) bool {
			// GET ScheduledSparkApplication with job name
			jobName := k.(string)
			logInfo("Checking if JOBNAME: " + jobName + " has run.")
			res := ScheduledSparkApplication{}
			err := clientset.RESTClient().Get().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("ScheduledSparkApplications").
				Name(jobName).
				Do(context.TODO()).
				Into(&res)
			if err != nil {
				logError("Unable to get ScheduledSparkApplication from sync map. err: " + err.Error())
				nonRepeatJobsSync.Delete(jobName)
				return true
			}
			if res.Status.LastRunName != "" {
				// job has run, suspend it so no future runs happen
				jobManifest := v.(*scheduledBatchJobManifest)
				jobManifest.Spec.Suspend = true
				logInfo("Suspending Non-repeating scheduled job: " + jobName)
				sparkJobManifest, err := yaml.Marshal(jobManifest)
				if err != nil {
					logError("Unable to encode batch job into yaml. err: " + err.Error())
					return true
				}
				curTime := strconv.FormatInt(time.Now().Unix(), formatIntBase10)
				sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
				err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, fsPerms)
				if err != nil {
					logError("Unable to write batch job manifest to file. err: " + err.Error())
					return true
				}
				
				response := applyManifest(sparkJobManifestFile, jobName)
				if response.Status == http.StatusInternalServerError {
					logError("Something when wrong when suspending the job: " + response.Output)
					return true
				}
				logInfo("DONE suspending Non-repeating scheduled job: " + jobName)
				nonRepeatJobsSync.Delete(jobName)
			}
			return true
		})
	}
}

// verifyCreateJobRequestBody is used to verify the contents of a batchJobRequest.
// If isScheduledJob is true, will check for fields needed to make scheduled jobs.
func verifyCreateJobRequestBody(isScheduledJob bool, batchJobReq batchJobRequest) (response serviceResponse) {
	// error 400 checking for mandatory fields and valid values
	response.Status = http.StatusBadRequest
	if  batchJobReq.Metadata.Name == "" {
		response.Output = "Missing metadata: name"
		return
	} else if match, _ := regexp.MatchString(nameRegex, batchJobReq.Metadata.Name); !match{
		response.Output = "Invalid name: " + batchJobReq.Metadata.Name + ". must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')"
		return
	} else if len(batchJobReq.Metadata.Name) > nameMaxCharCount {
		response.Output = "Invalid name: " + batchJobReq.Metadata.Name + ": must be no more than 63 characters."
		return
	} else if batchJobReq.Spec.Type == "" || batchJobReq.Spec.MainClass == "" || batchJobReq.Spec.MainApplicationFile == "" {
		response.Output = "Missing one of spec parameters: type, mainClass, or mainApplicationFile"
		return
	} else if batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Driver.batchJobSpecSparkPodSpec.Memory == "" {
		response.Output = "Missing one of driver parameters: cores, or memory"
		return
	} else if batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Cores == 0 || batchJobReq.Spec.Executor.batchJobSpecSparkPodSpec.Memory == "" {
		response.Output = "Missing one of executor parameters: cores, or memory"
		return
	}
	if isScheduledJob {
		// error 400 for invalid format and input
		_, err := cron.ParseStandard(batchJobReq.Schedule.CronSchedule)
		if err != nil {
			response.Output = "Invalid cron schedule format: " + err.Error()
			return
		}
		 if batchJobReq.Schedule.ConcurrencyPolicy == "" {
			response.Output = "Missing schedule parameters: concurrencyPolicy"
			return
		} else if batchJobReq.Schedule.ConcurrencyPolicy != string(ConcurrencyAllow) && batchJobReq.Schedule.ConcurrencyPolicy != string(ConcurrencyForbid) && batchJobReq.Schedule.ConcurrencyPolicy != string(ConcurrencyReplace) {
			response.Output = "Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace"
			return
		} else if batchJobReq.Schedule.RunHistoryLimit < 0 || batchJobReq.Schedule.SuccessfulRunHistoryLimit < 0 || batchJobReq.Schedule.FailedRunHistoryLimit < 0 {
			response.Output = "Invalid HistoryLimit, RunHistoryLimit/SuccessfulRunHistoryLimit/FailedRunHistoryLimit should be greater than 0"
			return
		} else if batchJobReq.Schedule.RunHistoryLimit != 0 && (batchJobReq.Schedule.SuccessfulRunHistoryLimit != 0 || batchJobReq.Schedule.FailedRunHistoryLimit != 0) {
			response.Output = "Not allowed to set RunHistoryLimit and (SuccessfulRunHistoryLimit + FailedRunHistoryLimit). Must set either RunHistoryLimit or (SuccessfulRunHistoryLimit + FailedRunHistoryLimit)"
			return
		}
	}
	response.Status = http.StatusOK
	response.Output = "Batch job request ok"
	return
}

// createBatchJob is the handler for POST: /jobs/create
// It creates a SparkApplication object with the spec given in the request body.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func createBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit create batch job endpoint")
	decoder := json.NewDecoder(r.Body)
	// Get request bodys
	var batchJobReq batchJobRequest
	if err := decoder.Decode(&batchJobReq); err != nil {
		logError("Cannot decode request body: "+ err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body: "+ err.Error()))
		return
	}
	verifyRequestResponse := verifyCreateJobRequestBody(false, batchJobReq)
	if verifyRequestResponse.Status != http.StatusOK {
		logError("Invalid request: " + verifyRequestResponse.Output)
		w.WriteHeader(verifyRequestResponse.Status)
		w.Write([]byte(strconv.Itoa(verifyRequestResponse.Status) + " - Invalid request: " + verifyRequestResponse.Output))
		return
	}
	// create SparkApplication
	var response []byte
	var createReq batchJobManifest
	createReq.Metadata = batchJobReq.Metadata
	createReq.Spec = batchJobReq.Spec
	// create batch job
	logInfo("Creating SparkApplication: " + batchJobReq.Metadata.Name)
	createJobResponse := createJob(createReq)
	if createJobResponse.Status != http.StatusOK {
		logError("Error creating job: " + createJobResponse.Output)
		w.WriteHeader(createJobResponse.Status)
		w.Write([]byte(strconv.Itoa(createJobResponse.Status) + " - Error creating job: " + createJobResponse.Output))
		return
	}
	// encode response
	response, err := json.Marshal(createJobResponse)
	if err != nil {
		logError("Failed to encode a response: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode a response: " + err.Error()))
		return
	}
	logInfo(createJobResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// createScheduledBatchJob is the handler for POST: /scheduledjobs/create
// It creates a ScheduledSparkApplication object with the spec given in the request body.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func createScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit create scheduled batch job endpoint")
	decoder := json.NewDecoder(r.Body)
	// Get request
	var batchJobReq batchJobRequest
	if err := decoder.Decode(&batchJobReq); err != nil {
		logError("Cannot decode request body: "+ err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body: "+ err.Error()))
		return
	}
	verifyRequestResponse := verifyCreateJobRequestBody(true, batchJobReq)
	if verifyRequestResponse.Status != http.StatusOK {
		logError("Invalid request: " + verifyRequestResponse.Output)
		w.WriteHeader(verifyRequestResponse.Status)
		w.Write([]byte(strconv.Itoa(verifyRequestResponse.Status) + " - Invalid request: " + verifyRequestResponse.Output))
		return
	}
	// create ScheduledSparkApplication
	var response []byte
	var createReq scheduledBatchJobManifest
	createReq.Metadata = batchJobReq.Metadata
	logInfo("Creating ScheduledSparkApplication: " + batchJobReq.Metadata.Name)
	createJobResponse := createScheduledJob(createReq, batchJobReq.Spec, batchJobReq.Schedule, batchJobReq.OneRunScheduledJob)
	if createJobResponse.Status != http.StatusOK {
		logError("Error creating scheduled job: " + createJobResponse.Output)
		w.WriteHeader(createJobResponse.Status)
		w.Write([]byte(strconv.Itoa(createJobResponse.Status) + " - Error creating scheduled job: " + createJobResponse.Output))
		return
	}
	// encode response
	response, err := json.Marshal(createJobResponse)
	if err != nil {
		logError("Failed to encode a response: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode a response: " + err.Error()))
		return
	}
	logInfo(createJobResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
