package batchjob

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"context"
	"sync"

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
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/robfig/cron"
)

const bufferSize = 1024

var goRoutineCreated bool
var nonRepeatJobsSync sync.Map

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

type scheduledBatchJobSpec struct {
	Schedule                    string              `yaml:"schedule"`
	Suspend                     bool                `yaml:"suspend"`
	ConcurrencyPolicy           string              `yaml:"concurrencyPolicy"`
	SuccessfulRunHistoryLimit   int32               `yaml:"successfulRunHistoryLimit,omitempty"`
	FailedRunHistoryLimit       int32               `yaml:"failedRunHistoryLimit,omitempty"`
	Template                    batchJobSpec        `yaml:"template"`
}

type batchJobManifest struct {
	ApiVersion string           `yaml:"apiVersion"`
	Kind       string           `yaml:"kind"`
	Metadata   batchJobMetadata `yaml:"metadata" json:"metadata"`
	Spec       batchJobSpec     `yaml:"spec" json:"spec"`
}

type scheduledBatchJobManifest struct {
	ApiVersion string                   `yaml:"apiVersion"`
	Kind       string                   `yaml:"kind"`
	Metadata   batchJobMetadata         `yaml:"metadata" json:"metadata"`
	Spec       scheduledBatchJobSpec    `yaml:"spec" json:"spec"`
}

type batchJobSchedule struct {
	CronSchedule                    string
	ConcurrencyPolicy               string
	Suspend                         bool
	SuccessfulRunHistoryLimit       int32
	FailedRunHistoryLimit           int32
}

type batchJobRequest struct {
	Metadata            batchJobMetadata `yaml:"metadata" json:"metadata"`
	Spec                batchJobSpec     `yaml:"spec" json:"spec"`
	OneRunScheduledJob  bool             `yaml:"oneRunScheduledJob" json:"oneRunScheduledJob"`
	Schedule            batchJobSchedule `yaml:"schedule,omitempty" json:"schedule,omitempty"`
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

func createScheduledBatchJobSpec(jobSpecTemplate *scheduledBatchJobSpec, spec batchJobSpec, schedule batchJobSchedule) {
	jobSpecTemplate.Schedule = schedule.CronSchedule
	jobSpecTemplate.Template = spec
	createBatchJobSpec(&jobSpecTemplate.Template)
	jobSpecTemplate.Suspend = schedule.Suspend
	jobSpecTemplate.ConcurrencyPolicy = schedule.ConcurrencyPolicy
	jobSpecTemplate.SuccessfulRunHistoryLimit = schedule.SuccessfulRunHistoryLimit
	jobSpecTemplate.FailedRunHistoryLimit = schedule.FailedRunHistoryLimit
}

func createBatchJobManifest(job *batchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	createBatchJobMetadata(&job.Metadata)
	createBatchJobSpec(&job.Spec)
}

func createScheduledJobManifest(job *scheduledBatchJobManifest, spec batchJobSpec, schedule batchJobSchedule) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "ScheduledSparkApplication"
	createBatchJobMetadata(&job.Metadata)
	createScheduledBatchJobSpec(&job.Spec, spec, schedule)
}

func createJob(job batchJobManifest) (response serviceResponse) {
	createBatchJobManifest(&job)
	sparkJobManifest, err := yaml.Marshal(&job)
	if err != nil {
		log.Println("Unable to encode batch job into yaml. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to encode batch job into yaml. err: " + err.Error()
		return
	}
	fmt.Println(string(sparkJobManifest))
	curTime := strconv.FormatInt(time.Now().Unix(), 10)
	sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
	err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, 0644)
	if err != nil {
		log.Println("Unable to write batch job manifest to file. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to write batch job manifest to file. err: " + err.Error()
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

	// creating sparkapplication from yaml
	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	// read spark job manifest yaml into a byte[]
	content, err := ioutil.ReadFile(sparkJobManifestFile)
	if err != nil {
		log.Println("Unable to read batch job manifest to file. err: ", err)
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
	curTime := strconv.FormatInt(time.Now().Unix(), 10)
	sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
	err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, 0644)
	if err != nil {
		log.Println("Unable to write batch job manifest to file. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to write batch job manifest to file. err: " + err.Error()
		return
	}
	response = applyManifest(sparkJobManifestFile, job.Metadata.Name)
	// non-repeating scheuled job, put in map to be tracked 
	if nonRepeat {
		if !goRoutineCreated {
			go nonRepeatScheduledJobCleanup()
			goRoutineCreated = true
		}
		nonRepeatJobsSync.Store(job.Metadata.Name, &job)
	}

	return
}

func applyManifest(sparkJobManifestFile string, jobName string) (response serviceResponse) {
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
	// read spark job manifest yaml into a byte[] and apply
	content, err := ioutil.ReadFile(sparkJobManifestFile)
	if err != nil {
		log.Println("Unable to read batch job manifest to file. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to read batch job manifest to file. err: " + err.Error()
		return
	}
	applyOptions := apply.NewApplyOptions(dynamicClient, discoveryClient)
	if err := applyOptions.Apply(context.TODO(), content); err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to apply the batch job manifest to file. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to read apply the batch job manifest to file. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "ScheduledSparkApplication " + jobName + " created"
	return
}

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
				sparkJobManifestFile := "/opt/batch-job/manifests/" + curTime
				err = ioutil.WriteFile(sparkJobManifestFile, sparkJobManifest, 0644)
				if err != nil {
					log.Println("ERROR: Unable to write batch job manifest to file. err: ", err)
					return true
				}
				
				response := applyManifest(sparkJobManifestFile, jobName)
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

/**
* handler for POST: /job
* Creates a job
**/
func createBatchJob(w http.ResponseWriter, r *http.Request) {
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
	// error 400 checking for mandatory fields
	if batchJobReq.Metadata.Name == "" {
		log.Println("Missing metadata: name")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing metadata: name"))
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
	// SparkApplication
	log.Println("Creating SparkApplication")
	var createReq batchJobManifest
	createReq.Metadata = batchJobReq.Metadata
	createReq.Spec = batchJobReq.Spec
	createJobResponse := createJob(createReq)
	if createJobResponse.Status != http.StatusOK {
		log.Println("Error creating job: ", createJobResponse.Output)
		w.WriteHeader(createJobResponse.Status)
		w.Write([]byte(strconv.Itoa(createJobResponse.Status) + " - Error creating job: " + createJobResponse.Output))
		return
	}
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

/**
* handler for POST: /scheduledjob
* Creates a scheduled job
**/
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
	// error 400 checking for mandatory fields
	if batchJobReq.Metadata.Name == "" {
		log.Println("Missing metadata: name")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Missing metadata: name"))
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
	// check concurrencyPolicy is one of "Allow", "Forbid", "Replace"
	if batchJobReq.Schedule.ConcurrencyPolicy != "Allow" && batchJobReq.Schedule.ConcurrencyPolicy != "Forbid" && batchJobReq.Schedule.ConcurrencyPolicy != "Replace" {
		log.Println("Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace"))
		return
	}

	var response []byte
	// ScheduledSparkApplication
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
