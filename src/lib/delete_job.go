package batchjob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"context"

	"github.com/gorilla/mux"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
)

func deleteJob(jobName string) (response serviceResponse) {
	cmd := exec.Command("sparkctl", "delete", jobName, "--namespace="+SPARKJOB_CONFS["SPARKJOB_NAMESPACE"])
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil{
		log.Println("ERROR:\n", err)
		response.Status = 1
		response.Output = "ERROR:" + err.Error()
		return
	} else if errBuf.String() != "" {
		log.Println("ERROR:\n", errBuf.String())
		response.Status = 404
		response.Output = "ERROR:" + errBuf.String()
		return
	}

	response.Status = 0
	response.Output = outBuf.String()
	return
}

func deleteScheduledJob(jobName string) (response serviceResponse) {
	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = 1
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = 1
		response.Output = "Unable to create an kubernetes client. err: " + err.Error()
		return
	}
	var res rest.Result
	// check if scheduled job exists
	res = clientset.RESTClient().Get().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("ScheduledSparkApplications").
				Name(jobName).
				Do(context.TODO())
	if res.Error() != nil {
		log.Println("Scheduled job " + jobName + " does not exist")
		response.Status = 404
		response.Output = "Scheduled job " + jobName + " does not exist"
		return
	}
	// delete the job
	res = clientset.RESTClient().Delete().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("ScheduledSparkApplications").
				Name(jobName).
				Do(context.TODO())
	if res.Error() != nil {
		log.Println("Unable to DELETE ScheduledSparkApplication. err: ", res.Error())
		response.Status = 1
		response.Output = "Unable to DELETE ScheduledSparkApplication. err: " + res.Error().Error()
		return
	}

	response.Status = 0
	response.Output = "Deleted scheduled job: " + jobName
	return
}

func deleteBatchJob(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit delete job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	deleteJobResponse := deleteJob(jobName)
	if deleteJobResponse.Status == 1 {
		log.Println("Error deleting job: " + deleteJobResponse.Output)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Error deleting scheduled job: " + jobName + ", err: " + deleteJobResponse.Output))
		return
	} else if deleteJobResponse.Status == 404 {
		log.Println("Error deleting job: " + jobName + ", err: " + deleteJobResponse.Output)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Error deleting job: " + jobName + ", err: " + deleteJobResponse.Output))
		return
	}
	response, err := json.Marshal(deleteJobResponse)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func deleteScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit delete scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	deleteJobResponse := deleteScheduledJob(jobName)
	if deleteJobResponse.Status == 1 {
		log.Println("Error deleting scheduled job: " + jobName + ", err: " + deleteJobResponse.Output)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Error deleting scheduled job: " + jobName + ", err: " + deleteJobResponse.Output))
		return
	} else if deleteJobResponse.Status == 404 {
		log.Println("Error deleting scheduled job: " + jobName + ", err: " + deleteJobResponse.Output)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Error deleting scheduled job: " + jobName + ", err: " + deleteJobResponse.Output))
		return
	}
	response, err := json.Marshal(deleteJobResponse)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
