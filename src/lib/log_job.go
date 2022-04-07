package batchjob

import (
	"encoding/json"
	"net/http"
	"strconv"
	"context"
	"errors"

	"github.com/gorilla/mux"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	apiv1 "k8s.io/api/core/v1"
)

// batchJobRunOutput holds the fields of a batch job outputted to the user.
type batchJobRunOutput struct {
	// Id is Spark Application ID of a batch job.
	Id         string               `json:"Id"`
	// Name of a batch job.
	Name       string               `json:"Name"`
	// SparkUISvc is the spark UI name of the job.
	SparkUISvc string               `json:"SparkUISvc"`
	// State is current state of the job.
	State      ApplicationStateType `json:"State"`
	// Output is the output of the job run.
	Output     string               `json:"Output"`
}

// batchJobRunOutputResponse holds status and fields of a batch job output to the user.
type batchJobRunOutputResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status     int               `json:"Status"`
	// Run is information on the batch job and it's output
	Run        batchJobRunOutput `json:"Run"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage string            `json:"ErrorMessage,omitempty"`
}

// logJob gets a SparkApplication by SparkApplicationID and gets the logs of the driver for the SparkApplication.
// Returns job driver log output, state, and sparkUISvc.
func logJob(jobId string) (response batchJobRunOutputResponse) {
	job := getJobFromId(jobId)
	jobName := job.Name
	if jobName == "" {
		response.Status = http.StatusNotFound
		response.ErrMessage = "Unable to get job with jobId: " + jobId
		return
	}
	// get the SparkApplication 
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to create an kubernetes client. err: " + err.Error()
		return
	}
	getSparkAppResponse := getSparkApplication(jobName)
	if getSparkAppResponse.Status != http.StatusOK {
		response.Status = getSparkAppResponse.Status
		response.ErrMessage = getSparkAppResponse.ErrMessage
		return
	}
	res := getSparkAppResponse.SparkApp

	// get spark job driver pod logs
	podName := res.Status.DriverInfo.PodName
	rawLogs, err := clientset.CoreV1().
		Pods(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		GetLogs(podName, &apiv1.PodLogOptions{}).
		Do(context.TODO()).
		Raw()
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.ErrMessage = "Unable to get SparkApplication driver logs. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Run.Id = job.Id
	response.Run.Name = job.Name
	response.Run.SparkUISvc = job.SparkUISvc
	response.Run.State = job.State
	response.Run.Output = string(rawLogs)
	return
}

// getJobOutput is the handler for GET: /job/{name}/{id}
// Will take the name of a job/SparkApplication and Spark Application ID in URL.
// Returns the SparkUISvc, State, and log output of the job's driver.
func getJobOutput(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit log job endpoint")
	vars := mux.Vars(r)
	jobId := vars["id"]
	logJobResponse := logJob(jobId)
	if logJobResponse.Status != http.StatusOK {
		logError("Error getting logs: " + logJobResponse.ErrMessage)
		w.WriteHeader(logJobResponse.Status)
		w.Write([]byte(strconv.Itoa(logJobResponse.Status) + " - Error getting logs: " + logJobResponse.ErrMessage))
		return
	}

	response, err := json.Marshal(logJobResponse)
	if err != nil {
		logError("Failed to encode response. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	logInfo("Successfully get logs for job with ID: " + jobId)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
