package batchjob

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
)

var SPARKJOB_IMAGEPULLSECRETS []string
var SPARKJOB_CONFS map[string]string = make(map[string]string)
var SPARKJOB_SPARKCONFS map[string]string = make(map[string]string)

func init() {
	sparkJobConfKeys := []string{
		"SPARKJOB_NAMESPACE",
		"SPARKJOB_SPARKVERSION",
		"SPARKJOB_IMAGE",
		"SPARKJOB_IMAGEPULLSECRETS",
		"SPARKJOB_IMAGEPULLPOLICY",
		"SPARKJOB_SERVICEACCOUNT",
		"SPARKJOB_RESTARTPOLICY_TYPE",
		"SPARKJOB_DRIVER_JAVAOPTIONS",
		"SPARKJOB_EXECUTOR_JAVAOPTIONS"}
	for _, conf := range sparkJobConfKeys {
		SPARKJOB_CONFS[conf] = os.Getenv(conf)
	}

	SPARKJOB_IMAGEPULLSECRETS = strings.Split(strings.TrimSpace(SPARKJOB_CONFS["SPARKJOB_IMAGEPULLSECRETS"]), " ")

	sparkConfs := os.Getenv("SPARKJOB_SPARKCONF")
	for _, conf := range strings.Split(strings.TrimSpace(sparkConfs), " ") {
		sparkConf := strings.Split(conf, "=")
		SPARKJOB_SPARKCONFS[sparkConf[0]] = sparkConf[1]
	}
}

func HandleRequests() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs", getBatchJobs).Methods("GET")
	router.HandleFunc("/job", createBatchJob).Methods("POST")
	// for getting all runs of a batch job, however currently re-runs not supported
	router.HandleFunc("/job/{name}", getBatchJobRuns).Methods("GET")
	router.HandleFunc("/job/{name}", deleteBatchJob).Methods("DELETE")
	router.HandleFunc("/job/{name}/{id}", getJobOutput).Methods("GET")
	router.HandleFunc("/scheduledjob", createScheduledBatchJob).Methods("POST")
	log.Fatal(http.ListenAndServe(":8888", router))
}
