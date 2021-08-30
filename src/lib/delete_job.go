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

func deleteJob(jobName string) (response serviceResponse) {
	cmd := exec.Command("sparkctl", "delete", jobName, "--namespace="+SPARKJOB_CONFS["SPARKJOB_NAMESPACE"])
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		response.Status = 1
		log.Println("ERROR:\n", err, out.String())
		return
	}
	response.Status = 0
	response.Output = out.String()
	return
}

func deleteBatchJob(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit delete job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	deleteJobResponse := deleteJob(jobName)
	response, err := json.Marshal(deleteJobResponse)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
