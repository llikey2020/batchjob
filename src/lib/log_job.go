package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"context"

	"github.com/gorilla/mux"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	"k8s.io/apimachinery/pkg/api/errors"
	apiv1 "k8s.io/api/core/v1"
)

type batchJobRunOutput struct {
	Id         string               `json:"Id"`
	Name       string               `json:"Name"`
	SparkUISvc string               `json:"SparkUISvc"`
	State      ApplicationStateType `json:"State"`
	Output     string               `json:"Output"`
}

type batchJobRunOutputResponse struct {
	Status     int               `json:"Status"`
	Run        batchJobRunOutput `json:"Run"`
	ErrMessage string            `json:"ErrorMessage,omitempty"`
}

func logJob(jobId string) (response batchJobRunOutputResponse) {
	job := getJobFromId(jobId)
	jobName := job.Name
	if jobName == "" {
		log.Println("Unable to get job with jobId: " + jobId)
		response.Status = 404
		response.ErrMessage = "Unable to get job with jobId: " + jobId
		return
	}
	// get the SparkApplication 
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to create an kubernetes client. err: " + err.Error()
		return
	}
	res := SparkApplication{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("SparkApplications").
		Name(jobName).
		Do(context.TODO()).
		Into(&res)
	if errors.IsNotFound(err) {
		log.Println("Unable to get SparkApplication. err: ", err)
		response.Status = 404
		response.ErrMessage = "Unable to get SparkApplication. err: " + err.Error()
		return
	} else if err != nil {
		log.Println("StatusCode:", errors.ReasonForError(err))
		log.Println("Unable to get SparkApplication. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get SparkApplication. err: " + err.Error()
		return
	}

	// get spark job driver pod logs
	podName := res.Status.DriverInfo.PodName
	rawLogs, err := clientset.CoreV1().
		Pods(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		GetLogs(podName, &apiv1.PodLogOptions{}).
		Do(context.TODO()).
		Raw()
	if errors.IsNotFound(err) {
		log.Println("Unable to get SparkApplication driver logs. err: ", err)
		response.Status = 404
		response.ErrMessage = "Unable to get SparkApplication driver logs. err: " + err.Error()
		return
	} else if err != nil {
		log.Println("StatusCode:", errors.ReasonForError(err))
		log.Println("Unable to get SparkApplication driver logs. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get SparkApplication driver logs. err: " + err.Error()
		return
	}

	response.Status = 0
	response.Run.Id = job.Id
	response.Run.Name = job.Name
	response.Run.SparkUISvc = job.SparkUISvc
	response.Run.State = job.State
	response.Run.Output = string(rawLogs)
	return
}

/**
* handler for GET: /job/{name}/{id}
* get all jobs
**/
func getJobOutput(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit log job endpoint")
	vars := mux.Vars(r)
	jobId := vars["id"]
	logJobResponse := logJob(jobId)
	if logJobResponse.Status == 404 {
		log.Println("Error getting logs: " + logJobResponse.ErrMessage)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - " + logJobResponse.ErrMessage))
		return
	} else if logJobResponse.Status == 1 {
		log.Println("Error getting logs: " + logJobResponse.ErrMessage)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - " + logJobResponse.ErrMessage))
		return
	}
	response, err := json.Marshal(logJobResponse)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
