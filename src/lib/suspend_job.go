package batchjob

import (
	"encoding/json"
	"net/http"
	"context"
	"strconv"
	"errors"

	"github.com/gorilla/mux"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// suspendScheduledBatchJobSpec holds the suspend field which value is changed when a scheduled job is suspended and resumed.
type suspendScheduledBatchJobSpec struct {
	Suspend bool `json:"suspend"`
}

// suspendScheduledBatchJobManifest holds the fields needed to patch a ScheduledSparkApplication object spec suspend field.
type suspendScheduledBatchJobManifest struct {
	ApiVersion string                       `json:"apiVersion"`
	Kind       string                       `json:"kind"`
	Spec       suspendScheduledBatchJobSpec `json:"spec"`
}

// createSuspendScheduledBatchJobManifest creates a manifest with the suspend field value as the given value.
// Returns a manifest that can be used to patch a ScheduledSparkApplication with a new suspend value in it's spec.
func createSuspendScheduledBatchJobManifest(suspend bool) (job suspendScheduledBatchJobManifest) {
	job.ApiVersion = "sparkoperator.k8s.io/v1beta2"
	job.Kind = "ScheduledSparkApplication"
	job.Spec.Suspend = suspend
	return
}

// suspendScheduledJob suspends or unsuspends (resumes) a ScheduledSparkApplication with given jobName.
// Setting suspend = true will suspend the ScheduledSparkApplication, setting suspend = false will resume the ScheduledSparkApplication.
// Returns a serviceResponse containing the status code and string output.
func suspendScheduledJob(jobName string, suspend bool) (response serviceResponse){
	getJobResponse := getScheduledJob(jobName)
	if getJobResponse.Status != http.StatusOK {
		response.Status = getJobResponse.Status
		response.Output = getJobResponse.ErrMessage
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
		response.Output = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// Marshal into JSON
	obj := createSuspendScheduledBatchJobManifest(suspend)
	data, err := json.Marshal(obj)
	if err != nil {
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
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.Output = "Unable to patch ScheduledSparkApplication. err: " + err.Error()
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

// suspendScheduledBatchJob is the handler for PATCH: /scheduledjob/suspend/{name}
// Will suspend a scheduled batch job (ScheduledSparkApplication) with the given name by setting the suspend field to true.
// Writes a response containing a success or failure message.
func suspendScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit suspend scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	
	suspendResponse := suspendScheduledJob(jobName, true)
	if suspendResponse.Status != http.StatusOK {
		logError("Failed to suspend scheduled batch job: " + suspendResponse.Output)
		w.WriteHeader(suspendResponse.Status)
		w.Write([]byte(strconv.Itoa(suspendResponse.Status) + " - Failed to suspend scheduled batch job:" + suspendResponse.Output))
		return
	}

	response, err := json.Marshal(suspendResponse)
	if err != nil {
		logError("Failed to encode response. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	logInfo(suspendResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// suspendScheduledBatchJob is the handler for PATCH: /scheduledjobs/resume/{name}
// Will suspend a scheduled batch job (ScheduledSparkApplication) with the given name by setting the suspend field to false.
// Writes a response containing a success or failure message.
func resumeScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit resume scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	
	suspendResponse := suspendScheduledJob(jobName, false)
	if suspendResponse.Status != http.StatusOK {
		logError("Failed to resume scheduled batch job: " + suspendResponse.Output)
		w.WriteHeader(suspendResponse.Status)
		w.Write([]byte(strconv.Itoa(suspendResponse.Status) + " - Failed to resume scheduled batch job: " + suspendResponse.Output))
		return
	}

	response, err := json.Marshal(suspendResponse)
	if err != nil {
		logError("Failed to encode response. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	logInfo(suspendResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
