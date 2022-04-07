package batchjob

import (
	"encoding/json"
	"net/http"
	"strconv"
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// RunSparkAppType is the value for the "sparkAppType" label of SparkApplications that represent runs of a batch job
const RunSparkAppType = "jobRun"

// runJobRequest holds the name of the job to create a run for, and optional change of arguments
type runJobRequest struct {
	// JobName is name of job to create run for.
	JobName   string   `yaml:"name" json:"name"`
	// Arguments are optional different arguments for a job run.
	Arguments []string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// runBatchJobMetadata holds the Namespace to create the run on, and labels to add to distinguish the created SparkApplication as a run.
// generateName is used to generate unique names for job runs.
type runBatchJobMetadata struct {
	// GenerateName is the <UID>-<JobName> where UID and JobName are the original job's name and UID.
	// Uses the generateName field in k8s to create a unique random name for job run. 
	GenerateName string            `json:"generateName"`
	// Namespace is the namespace the job will be created on.
	Namespace    string            `json:"namespace"`
	// Labels will contain job run specific labels to distinguish it from regular jobs.
	Labels       map[string]string `json:"labels"`
}

// runBatchJobManifest holds the fields needed for creating a SparkApplication which represents a run of an existing job.
type runBatchJobManifest struct {
	ApiVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   runBatchJobMetadata  `json:"metadata"`
	Spec       SparkApplicationSpec `json:"spec"`
}

// createRunBatchJobMetadata creates metadata for the SparkApplication created to represent a run of a batch job.
// Two labels are added for originalJobName with value of the job name created, and sparkAppType to show it's a job run.
func createRunBatchJobMetadata(jobMetadata *runBatchJobMetadata, jobName string, jobRunName string) {
	jobMetadata.GenerateName = jobRunName
	jobMetadata.Namespace = SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]
	// label SparkApplication as a "jobRun", add label for original SparkApplication name
	jobMetadata.Labels = map[string]string{"originalJobName": jobName, "sparkAppType": RunSparkAppType}
}

// createRunBatchJobManifest creates the manifest with spec of the job to create a run of.
// Will set arguments if any arguments are provided.
// Returns a manifest that can be used to create a SparkApplication.
func createRunBatchJobManifest(job *runBatchJobManifest, jobName string, jobRunName string, arguments []string, spec SparkApplicationSpec) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	createRunBatchJobMetadata(&job.Metadata, jobName, jobRunName)
	job.Spec = spec
	if len(arguments) != 0 {
		job.Spec.Arguments = arguments
	}
} 

// runJob creates a job run from an existing job.
// It will create a SparkApplication with the name <UID>-<jobName>-<aaaaa> 
// Where UID is the UID of the existing job, jobName is name of the existing job, and "aaaaa" will be a randomly generated string.
// The SparkApplication created will have same spec as the existing job.
// The SparkApplication will have two labels: originalJobName with value of the job name created, and sparkAppType to show it's a job run.
// Can not make a run off a SparkApplication that is a "job run" (labels originalJobName and sparkAppType=RunSparkAppType).
// Returns a serviceResponse containing the status code and string output.
func runJob(jobName string, arguments []string) (response serviceResponse) {
	getSparkAppResponse := getSparkApplication(jobName)
	if getSparkAppResponse.Status != http.StatusOK {
		response.Status = http.StatusNotFound
		response.Output = "Unable to get SparkApplication:" + jobName + " err: " + getSparkAppResponse.ErrMessage
		return
	}
	sparkApp := getSparkAppResponse.SparkApp
	uid := sparkApp.ObjectMeta.UID
	spec := sparkApp.Spec
	// Check SparkApplication labels if it's a job run
	labels := sparkApp.GetLabels()
	if val, prs := labels["sparkAppType"]; prs && val == RunSparkAppType {
		response.Status = http.StatusBadRequest
		response.Output = "Cannot create run off run type SparkApplication: " + jobName
		return
	}
	// create rerun SparkApplication with name <UID>-<jobName>-<RunId>, generateName is used to randomly generate <RunId>
	jobRunName := string(uid) + "-" + jobName + "-"
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// Read Struct into JSON, into unstructured to create SparkApplication for dynamic client
	var obj runBatchJobManifest
	createRunBatchJobManifest(&obj, jobName, jobRunName, arguments, spec)
	data, err := json.Marshal(obj)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create JSON. err: " + err.Error()
		return
	}
	deployment := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &deployment); err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create unstructured: " + err.Error()
		return
	}

	// Create a new SparkApplication for this run
	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	result, err := dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create SparkApplication: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Created job run: " + result.GetName()
	return
}

// runBatchJob is the handler for POST: /job/run
// It creates a run of an existing batch job.
// This creates a run by creating a new SparkApplication based off the existing job.
// Writes a response containing a success or failure message.
func runBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit run job endpoint")
	decoder := json.NewDecoder(r.Body)
	var runJobReq runJobRequest
	if err := decoder.Decode(&runJobReq); err != nil {
		logError("Cannot decode request body: "+ err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body: "+ err.Error()))
		return
	}
	runJobResponse:= runJob(runJobReq.JobName, runJobReq.Arguments)
	if runJobResponse.Status != http.StatusOK {
		logError("Failed to create batch job run: " + runJobResponse.Output)
		w.WriteHeader(runJobResponse.Status)
		w.Write([]byte(strconv.Itoa(runJobResponse.Status) + " - Failed to create batch job run: " + runJobResponse.Output))
		return
	}

	response, err := json.Marshal(runJobResponse)
	if err != nil {
		logError("Failed to encode response. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	logInfo(runJobResponse.Output + ", for job: "+ runJobReq.JobName)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
