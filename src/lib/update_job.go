package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"context"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/robfig/cron"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type updateBatchJobSpecRestartPolicy struct {
	Type                             string `json:"type,omitempty"`
	OnSubmissionFailureRetries       int32  `json:"onSubmissionFailureRetries,omitempty"`
	OnFailureRetries                 int32  `json:"onFailureRetries,omitempty"`
	OnSubmissionFailureRetryInterval int64  `json:"onSubmissionFailureRetryInterval,omitempty"`
	OnFailureRetryInterval           int64  `json:"onFailureRetryInterval,omitempty"`
}

type updateBatchJobSpecDynamicAllocation struct {
	Enabled                bool  `json:"enabled,omitempty"`
	InitialExecutors       int32 `json:"initialExecutors,omitempty"`
	MinExecutors           int32 `json:"minExecutors,omitempty"`
	MaxExecutors           int32 `json:"maxExecutors,omitempty"`
	ShuffleTrackingTimeout int64 `json:"shuffleTrackingTimeout,omitempty"`
}

type updateBatchJobSpecSparkPodSpec struct {
	Cores          int32  `json:"cores,omitempty"`
	CoreLimit      string `json:"coreLimit,omitempty"`
	Memory         string `json:"memory,omitempty"`
}

type updateBatchJobSpecDriver struct {
	batchJobSpecSparkPodSpec `json:",inline,omitempty"`
}

type updateBatchJobSpecExecutor struct {
	batchJobSpecSparkPodSpec        `json:",inline,omitempty"`
	Instances                int32  `json:"instances,omitempty"`
}

type updateBatchJobManifest struct {
	ApiVersion string             `json:"apiVersion"`
	Kind       string             `json:"kind"`
	Spec       updateBatchJobSpec `json:"spec"`
}

type updateScheduledBatchJobManifest struct {
	ApiVersion string                      `json:"apiVersion"`
	Kind       string                      `json:"kind"`
	Spec       updateScheduledBatchJobSpec `json:"spec"`
}

type updateBatchJobSpec struct {
	Type                string                               `json:"type,omitempty"`
	MainClass           string                               `json:"mainClass,omitempty"`
	MainApplicationFile string                               `json:"mainApplicationFile,omitempty"`
	Arguments           []string                             `json:"arguments,omitempty"`
	RestartPolicy       *updateBatchJobSpecRestartPolicy     `json:"restartPolicy,omitempty"`
	DynamicAllocation   *updateBatchJobSpecDynamicAllocation `json:"dynamicAllocation,omitempty"`
	Driver              *updateBatchJobSpecDriver            `json:"driver,omitempty"`
	Executor            *updateBatchJobSpecExecutor          `json:"executor,omitempty"`
}

type updateScheduledBatchJobSpec struct {
	Schedule                  string              `json:"schedule,omitempty"`
	Suspend                   *bool               `json:"suspend,omitempty"`
	ConcurrencyPolicy         string              `json:"concurrencyPolicy,omitempty"`
	SuccessfulRunHistoryLimit int32               `json:"successfulRunHistoryLimit,omitempty"`
	FailedRunHistoryLimit     int32               `json:"failedRunHistoryLimit,omitempty"`
	RunHistoryLimit           int32               `json:"runHistoryLimit,omitempty"`
	Template                  *updateBatchJobSpec `json:"template,omitempty"`
}

func createUpdateBatchJobManifest(spec updateBatchJobSpec) (job updateBatchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "SparkApplication"
	job.Spec = spec
	return
}

func createUpdateScheduledBatchJobManifest(spec updateScheduledBatchJobSpec) (job updateScheduledBatchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "ScheduledSparkApplication"
	if spec.RunHistoryLimit != 0 {
		spec.SuccessfulRunHistoryLimit = spec.RunHistoryLimit
		spec.FailedRunHistoryLimit = spec.RunHistoryLimit
		spec.RunHistoryLimit = 0
	}
	job.Spec = spec
	return
}

func updateJob(jobName string, spec updateBatchJobSpec) (response serviceResponse){
	getSparkAppResponse := getSparkApplication(jobName)
	if getSparkAppResponse.Status != http.StatusOK {
		log.Println("Unable to get SparkApplication:", jobName, " err: ", getSparkAppResponse.ErrMessage)
		response.Status = http.StatusNotFound
		response.Output = "Unable to get SparkApplication: " + jobName + ", err: " + getSparkAppResponse.ErrMessage
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
		log.Println("Unable to create a dynamic client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// Marshal into JSON
	obj := createUpdateBatchJobManifest(spec)
	data, err := json.Marshal(obj)
	if err != nil {
		log.Println("Unable to create JSON. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create JSON. err: " + err.Error()
		return
	}

	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	// Update the job
	_, err = dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Patch(context.TODO(), jobName, types.MergePatchType, data, metav1.PatchOptions{
			FieldManager: "batch-service",
		})
	if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to Patch SparkApplication. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to Patch SparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Successfully updated job: " + jobName
	return
}

func updateScheduledJob(jobName string, spec updateScheduledBatchJobSpec) (response serviceResponse){
	getSparkAppResponse := getScheduledSparkApplication(jobName)
	if getSparkAppResponse.Status != http.StatusOK {
		log.Println("Unable to get ScheduledSparkApplication:", jobName, " err: ", getSparkAppResponse.ErrMessage)
		response.Status = http.StatusNotFound
		response.Output = "Unable to get ScheduledSparkApplication: " + jobName + ", err: " + getSparkAppResponse.ErrMessage
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
		log.Println("Unable to create a dynamic client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// Marshal into JSON
	obj := createUpdateScheduledBatchJobManifest(spec)
	data, err := json.Marshal(obj)
	if err != nil {
		log.Println("Unable to create JSON. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create JSON. err: " + err.Error()
		return
	}

	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "scheduledsparkapplications"}
	// Update the scheduled job
	_, err = dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Patch(context.TODO(), jobName, types.MergePatchType, data, metav1.PatchOptions{
			FieldManager: "batch-service",
		})
	if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to Patch ScheduledSparkApplication. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to Patch ScheduledSparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Successfully updated job: " + jobName
	return
}

/**
* handler for PATCH: /job/{name}
* update a batch job
**/
func updateBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit update job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	decoder := json.NewDecoder(r.Body)
	var updateReq updateBatchJobSpec
	if err := decoder.Decode(&updateReq); err != nil {
		log.Println("Cannot decode request body", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body:"+ err.Error()))
		return
	}
	
	updateResponse := updateJob(jobName, updateReq)
	if updateResponse.Status != http.StatusOK {
		log.Println("Error updating job", updateResponse.Output)
		w.WriteHeader(updateResponse.Status)
		w.Write([]byte(strconv.Itoa(updateResponse.Status) + " - Failed to update batch job: " + updateResponse.Output))
		return
	}

	response, err := json.Marshal(updateResponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

/**
* handler for PATCH: /scheduledjob/{name}
* update a scheduled batch job
**/
func updateScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit update scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	decoder := json.NewDecoder(r.Body)
	var updateReq updateScheduledBatchJobSpec
	if err := decoder.Decode(&updateReq); err != nil {
		log.Println("Cannot decode request body", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Cannot decode request body:"+ err.Error()))
		return
	}
	// check if schedule is updated and is valid format
	if _, err := cron.ParseStandard(updateReq.Schedule); updateReq.Schedule != "" && err != nil {
		log.Println("Invalid cron schedule format: " + err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid cron schedule format: " + err.Error()))
		return
	}
	// check if concurrencyPolicy is updated and is one of "Allow", "Forbid", "Replace"
	if updateReq.ConcurrencyPolicy != "" && (updateReq.ConcurrencyPolicy != "Allow" && updateReq.ConcurrencyPolicy != "Forbid" && updateReq.ConcurrencyPolicy != "Replace") {
		log.Println("Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid ConcurrencyPolicy, must be one of: Allow, Forbid, Replace"))
		return
	}
	// check Run History Limit values. restrict to choose RunHistoryLimit or (SuccessfulRunHistoryLimit and FailedRunHistoryLimit) for update
	if updateReq.RunHistoryLimit < 0 || updateReq.SuccessfulRunHistoryLimit < 0 || updateReq.FailedRunHistoryLimit < 0 {
		log.Println("Invalid HistoryLimit, RunHistoryLimit/SuccessfulRunHistoryLimit/FailedRunHistoryLimit should be greater than 0")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Invalid HistoryLimit, RunHistoryLimit/SuccessfulRunHistoryLimit/FailedRunHistoryLimit should be greater than 0"))
		return
	} else if updateReq.RunHistoryLimit != 0 && (updateReq.SuccessfulRunHistoryLimit != 0 || updateReq.FailedRunHistoryLimit != 0) {
		log.Println("Not allowed to set RunHistoryLimit and one of SuccessfulRunHistoryLimit or FailedRunHistoryLimit. Must set either RunHistoryLimit or (SuccessfulRunHistoryLimit and/or FailedRunHistoryLimit)")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("400 - Not allowed to set RunHistoryLimit and one of SuccessfulRunHistoryLimit or FailedRunHistoryLimit. Must set either RunHistoryLimit or (SuccessfulRunHistoryLimit and/or FailedRunHistoryLimit)"))
		return
	}

	
	updateResponse := updateScheduledJob(jobName, updateReq)
	if updateResponse.Status != http.StatusOK {
		log.Println("Error updating job", updateResponse.Output)
		w.WriteHeader(updateResponse.Status)
		w.Write([]byte(strconv.Itoa(updateResponse.Status) + " - Failed to update scheduled batch job: " + updateResponse.Output))
		return
	}

	response, err := json.Marshal(updateResponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
