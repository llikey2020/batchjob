package batchjob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"

	"github.com/gorilla/mux"
)

type batchJobRunOutput struct {
	Id         string               `json:"Id"`
	Name       string               `json:"Name"`
	SparkUISvc string               `json:"SparkUISvc"`
	State      ApplicationStateType `json:"State"`
	Output     string               `json:"Output"`
}

type batchJobRunOutputResponse struct {
	Status int               `json:"Status"`
	Run    batchJobRunOutput `json:"Run"`
}

func logJob(jobId string) (response batchJobRunOutputResponse) {
	job := getJobFromId(jobId)
	jobName := job.Name
	cmd := exec.Command("sparkctl", "log", jobName, "--namespace="+SPARKJOB_CONFS["SPARKJOB_NAMESPACE"])
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		response.Status = 1
		log.Println("ERROR:\n", err, out.String())
		return
	}

	response.Status = 0
	response.Run.Id = job.Id
	response.Run.Name = job.Name
	response.Run.SparkUISvc = job.SparkUISvc
	response.Run.State = job.State
	response.Run.Output = out.String()
	return
}

func getJobOutput(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit log job endpoint")
	vars := mux.Vars(r)
	jobId := vars["id"]
	logJobResponse := logJob(jobId)
	response, err := json.Marshal(logJobResponse)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
