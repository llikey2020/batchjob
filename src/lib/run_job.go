package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const RunSparkAppType = "jobRun"

type runJobRequest struct {
	JobName   string   `yaml:"name" json:"name"`
	Arguments []string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

type runBatchJobMetadata struct {
	GenerateName string            `json:"generateName"`
	Namespace    string            `json:"namespace"`
	Labels       map[string]string `json:"labels"`
}

type runBatchJobManifest struct {
	ApiVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   runBatchJobMetadata  `json:"metadata"`
	Spec       SparkApplicationSpec `json:"spec"`
}

func createRunBatchJobMetadata(jobMetadata *runBatchJobMetadata, jobName string, jobRunName string) {
	jobMetadata.GenerateName = jobRunName
	jobMetadata.Namespace = SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]
	// label SparkApplication as a "jobRun", add label for original SparkApplication name
	jobMetadata.Labels = map[string]string{"originalJobName": jobName, "sparkAppType": RunSparkAppType}
}

func createRunBatchJobManifest(job *runBatchJobManifest, jobName string, jobRunName string, arguments []string, spec SparkApplicationSpec) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	createRunBatchJobMetadata(&job.Metadata, jobName, jobRunName)
	job.Spec = spec
	if len(arguments) != 0 {
		job.Spec.Arguments = arguments
	}
} 

func runJob(jobName string, arguments []string) (response serviceResponse) {
	getSparkAppResponse := getSparkApplication(jobName)
	if getSparkAppResponse.Status != http.StatusOK {
		log.Println("Unable to get SparkApplication:", jobName, " err: ", getSparkAppResponse.ErrMessage)
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
		log.Println("Cannot create run off run type SparkApplication: ", jobName)
		response.Status = http.StatusBadRequest
		response.Output = "Cannot create run off run type SparkApplication: " + jobName
		return
	}
	// create rerun SparkApplication with name <UID>-<jobName>-<RunId>, generateName is used to randomly generate <RunId>
	jobRunName := string(uid) + "-" + jobName + "-"
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create a dynamic client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// Read Struct into JSON, into unstructured to create SparkApplication for dynamic client
	var obj runBatchJobManifest
	createRunBatchJobManifest(&obj, jobName, jobRunName, arguments, spec)
	data, err := json.Marshal(obj)
	if err != nil {
		log.Println("Unable to create JSON. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create JSON. err: " + err.Error()
		return
	}
	deployment := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, &deployment); err != nil {
		log.Println("Unable to create unstructured: ", err)
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
		log.Println("Unable to create SparkApplication: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create SparkApplication: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Created job run: " + result.GetName()
	return
}

/**
* handler for POST: /job/run
* Runs a batch job by creating a new SparkApplication resource
**/
func runBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit rerun job endpoint")
	decoder := json.NewDecoder(r.Body)
	var runJobReq runJobRequest
	if err := decoder.Decode(&runJobReq); err != nil {
		log.Println("Cannot decode request body", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body:"+ err.Error()))
		return
	}
	runJobResponse:= runJob(runJobReq.JobName, runJobReq.Arguments)
	if runJobResponse.Status != http.StatusOK {
		log.Println("Error creating job run:", runJobResponse.Output)
		w.WriteHeader(runJobResponse.Status)
		w.Write([]byte(strconv.Itoa(runJobResponse.Status) + " - Failed to create batch job run:" + runJobResponse.Output))
		return
	}

	response, err := json.Marshal(runJobResponse)
	if err != nil {
		log.Println("Failed to encode jobs list", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode jobs list: " + err.Error()))
		return
	}
	
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
