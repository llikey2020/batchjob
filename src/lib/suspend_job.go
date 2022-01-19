package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"context"
	"strconv"

	"github.com/gorilla/mux"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type suspendScheduledBatchJobSpec struct {
	Suspend bool `json:"suspend"`
}

type suspendScheduledBatchJobManifest struct {
	ApiVersion string                       `json:"apiVersion"`
	Kind       string                       `json:"kind"`
	Spec       suspendScheduledBatchJobSpec `json:"spec"`
}

func createSuspendScheduledBatchJobManifest(suspend bool) (job suspendScheduledBatchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "ScheduledSparkApplication"
	job.Spec.Suspend = suspend
	return
}

func suspendScheduledJob(jobName string, suspend bool) (response serviceResponse){
	getJobResponse := getScheduledJob(jobName)
	if getJobResponse.Status != 0 {
		if getJobResponse.Status == 404 {
			response.Status = http.StatusNotFound
		} else {
			response.Status = http.StatusInternalServerError
		}
		log.Println("Unable to find ScheduledSparkApplication:" + getJobResponse.ErrMessage)
		response.Output = getJobResponse.ErrMessage
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
	obj := createSuspendScheduledBatchJobManifest(suspend)
	data, err := json.Marshal(obj)
	if err != nil {
		log.Println("Unable to create JSON. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create JSON. err: " + err.Error()
		return
	}

	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "scheduledsparkapplications"}
	force := true
	// Update the scheduled job
	_, err = dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Patch(context.TODO(), jobName, types.ApplyPatchType, data, metav1.PatchOptions{
			FieldManager: "batch-service",
			Force: &force,
		})
	if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to Patch ScheduledSparkApplication. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to Patch ScheduledSparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	if suspend {
		response.Output = "Suspended ScheduledSparkApplication: " + jobName
	} else {
		response.Output = "Resumed ScheduledSparkApplication: " + jobName
	}
	return
}

/**
* handler for PATCH: /scheduledjob/{name}/suspend
* Suspend a scheduled batch job
**/
func suspendScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit suspend scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	
	suspendResponse := suspendScheduledJob(jobName, true)
	if suspendResponse.Status != http.StatusOK {
		log.Println("Error suspending job", suspendResponse.Output)
		w.WriteHeader(suspendResponse.Status)
		w.Write([]byte(strconv.Itoa(suspendResponse.Status) + " - Failed to suspend scheduled batch job:" + suspendResponse.Output))
		return
	}

	response, err := json.Marshal(suspendResponse)
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
* handler for PATCH: /scheduledjob/{name}/resume
* Resume a scheduled batch job
**/
func resumeScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit resume scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	
	suspendResponse := suspendScheduledJob(jobName, false)
	if suspendResponse.Status != http.StatusOK {
		log.Println("Error resuming job", suspendResponse.Output)
		w.WriteHeader(suspendResponse.Status)
		w.Write([]byte(strconv.Itoa(suspendResponse.Status) + " - Failed to resume scheduled batch job:" + suspendResponse.Output))
		return
	}

	response, err := json.Marshal(suspendResponse)
	if err != nil {
		log.Println("Failed to encode response", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
