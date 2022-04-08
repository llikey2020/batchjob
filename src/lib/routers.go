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
var S3_BUCKET_NAME string

func init() {
	sparkJobConfKeys := []string{
		"SPARKJOB_NAMESPACE",
		"SPARKJOB_SPARKVERSION",
		"SPARKJOB_IMAGE",
		"SPARKJOB_IMAGEPULLSECRETS",
		"SPARKJOB_IMAGEPULLPOLICY",
		"SPARKJOB_SERVICEACCOUNT",
		"SPARKJOB_RESTARTPOLICY_TYPE",
		"SPARKJOB_RESTARTPOLICY_ONFAILURE_RETRIES",
		"SPARKJOB_RESTARTPOLICY_ONFAILURE_RETRY_INTERVAL",
		"SPARKJOB_RESTARTPOLICY_ONSUBMISSION_RETRIES",
		"SPARKJOB_RESTARTPOLICY_ONSUBMISSION_RETRY_INTERVAL",
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

	S3_BUCKET_NAME = os.Getenv("S3A_BUCKET_NAME")
}

func HandleRequests() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs/create", createBatchJob).Methods("POST")
	router.HandleFunc("/jobs/list", getBatchJobs).Methods("GET")
	router.HandleFunc("/jobs/get/{name}", getBatchJobRuns).Methods("GET")
	router.HandleFunc("/jobs/delete/{name}", deleteBatchJob).Methods("DELETE")
	router.HandleFunc("/jobs/get/{name}/{id}", getJobOutput).Methods("GET")
	router.HandleFunc("/jobs/run", runBatchJob).Methods("POST")
	router.HandleFunc("/jobs/update/{name}", updateBatchJob).Methods("PATCH")
	router.HandleFunc("/scheduledjobs/create", createScheduledBatchJob).Methods("POST")
	router.HandleFunc("/scheduledjobs/list", getScheduledBatchJobs).Methods("GET")
	router.HandleFunc("/scheduledjobs/get/{name}", getScheduledBatchJob).Methods("GET")
	router.HandleFunc("/scheduledjobs/delete/{name}", deleteScheduledBatchJob).Methods("DELETE")
	router.HandleFunc("/scheduledjobs/suspend/{name}", suspendScheduledBatchJob).Methods("PATCH")
	router.HandleFunc("/scheduledjobs/resume/{name}", resumeScheduledBatchJob).Methods("PATCH")
	router.HandleFunc("/scheduledjobs/update/{name}", updateScheduledBatchJob).Methods("PATCH")

	router.HandleFunc("/ss3/upload/{bucketName}/{fileName}", SS3UploadFile).
		Methods("PUT").
		Queries("fileType", "{fileType}").
		Queries("isShared", "{isShared}")

	router.HandleFunc("/ss3/delete/{bucketName}/{fileName}", SS3DeleteObject).
		Methods("DELETE").
		Queries("fileType", "{fileType}").
		Queries("isShared", "{isShared}")

	log.Fatal(http.ListenAndServe(":8888", router))
}
