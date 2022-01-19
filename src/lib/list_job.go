package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"context"

	"github.com/gorilla/mux"
	"github.com/olekukonko/tablewriter"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	"k8s.io/apimachinery/pkg/api/errors"
)

type sparkApplicationResponse struct {
	Status     int              `json:"Status"`
	SparkApp   SparkApplication `json:"SparkApp"`
	ErrMessage string           `json:"ErrorMessage,omitempty"`
}

type batchJob struct {
	Name              string               `json:"Name"`
	Id                string               `json:"Id"`
	SparkUISvc        string               `json:"SparkUISvc"`
	State             ApplicationStateType `json:"State"`
	CreationTimestamp string               `json:"CreationTimestamp"`
	Spec              SparkApplicationSpec `json:"Spec"`
}

type batchJobsResponse struct {
	Status     int        `json:"Status"`
	TotalJobs  int        `json:"TotalJobs"`
	Jobs       []batchJob `json:"Jobs"`
	ErrMessage string     `json:"ErrorMessage,omitempty"`
}

type scheduledBatchJob struct {
	Name              string                          `json:"Name"`
	CreationTimestamp string                          `json:"CreationTimestamp"`
	Spec              ScheduledSparkApplicationSpec   `json:"Spec"`
	Status            ScheduledSparkApplicationStatus `json:"Status,omitempty"`
}

type scheduledSparkApplicationResponse struct {
	Status     int                       `json:"Status"`
	SparkApp   ScheduledSparkApplication `json:"ScheduledSparkApp"`
	ErrMessage string                    `json:"ErrorMessage,omitempty"`
}

type scheduledBatchJobsResponse struct {
	Status        int                 `json:"Status"`
	TotalJobs     int                 `json:"TotalJobs"`
	ScheduledJobs []scheduledBatchJob `json:"ScheduledJobs"`
	ErrMessage    string              `json:"ErrorMessage,omitempty"`
}

type scheduledBatchJobResponse struct {
	Status       int               `json:"Status"`
	ScheduledJob scheduledBatchJob `json:"ScheduledJob"`
	ErrMessage   string            `json:"ErrorMessage,omitempty"`
}

func getSparkApplication(jobName string) (response sparkApplicationResponse) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = 1
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = 1
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
		log.Println("Reason for error:", errors.ReasonForError(err))
		log.Println("Unable to get SparkApplication. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get SparkApplication. err: " + err.Error()
		return
	}

	response.Status = 0
	response.SparkApp = res
	return
}

/**
* get list for runs for job with name: jobName
* To-Do: When re-runs for single job is supported,
* 		 this function needs to correctly support getting list of all runs for a job
**/
func getJobFromId(id string) batchJob {
	log.Println("getting job with id: ", id)
	allJobRes := listJobs(false)
	for _, job := range allJobRes.Jobs {
		if job.Id == id {
			return job
		}
	}
	log.Println("No job with id: ", id)
	return batchJob{}
}

func printJobs(jobs []SparkApplication) {
	log.Println("list jobs:\n")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Application Id", "State", "Submission Attempt Time", "Termination Time"})
	for _, job := range jobs {
		table.Append([]string{
			string(job.ObjectMeta.Name),
			string(job.Status.SparkApplicationID),
			string(job.Status.AppState.State),
			string(job.Status.LastSubmissionAttemptTime.String()),
			string(job.Status.TerminationTime.String()),
		})
	}
	table.Render()
}

func listJobs(onlyRunning bool) (response batchJobsResponse) {
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
	res := SparkApplicationList{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("SparkApplications").
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to get SparkApplications. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get SparkApplications. err: " + err.Error()
		return
	}
	// print jobs for logging purposes
	printJobs(res.Items)

	response.Jobs = []batchJob{}
	for _, item := range res.Items {
		var job batchJob
		job.Name = item.ObjectMeta.Name
		job.Id = item.Status.SparkApplicationID
		job.SparkUISvc = item.Status.DriverInfo.WebUIServiceName
		job.State = item.Status.AppState.State
		job.CreationTimestamp = item.ObjectMeta.CreationTimestamp.String()
		job.Spec = item.Spec
		if onlyRunning && item.Status.AppState.State != "RUNNING" {
			continue
		}
		response.Jobs = append(response.Jobs, job)
	}

	response.Status = 0
	response.TotalJobs = len(response.Jobs)
	return
}

func listScheduledJobs() (response scheduledBatchJobsResponse) {
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
	res := ScheduledSparkApplicationList{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("ScheduledSparkApplications").
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		log.Println("Reason for error", errors.ReasonForError(err))
		log.Println("Unable to get ScheduledSparkApplications. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get ScheduledSparkApplications. err: " + err.Error()
		return
	}
	// get only relevant data and metadata into result
	response.ScheduledJobs = []scheduledBatchJob{}
	for _, item := range res.Items {
		var scheduledJob scheduledBatchJob
		scheduledJob.Name = item.ObjectMeta.Name
		scheduledJob.CreationTimestamp = item.ObjectMeta.CreationTimestamp.String()
		scheduledJob.Spec = item.Spec
		scheduledJob.Status = item.Status
		response.ScheduledJobs = append(response.ScheduledJobs, scheduledJob)
	}

	response.Status = 0
	response.TotalJobs = len(response.ScheduledJobs)
	return
}

func getScheduledSparkApplication(jobName string) (response scheduledSparkApplicationResponse) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = 1
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = 1
		return
	}
	res := ScheduledSparkApplication{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("ScheduledSparkApplications").
		Name(jobName).
		Do(context.TODO()).
		Into(&res)
	if errors.IsNotFound(err) {
		log.Println("Unable to get ScheduledSparkApplication. err: ", err)
		response.Status = 404
		response.ErrMessage = "Unable to get ScheduledSparkApplication. err: " + err.Error()
		return
	} else if err != nil {
		log.Println("Reason for error:", errors.ReasonForError(err))
		log.Println("Unable to get ScheduledSparkApplication. err: ", err)
		response.Status = 1
		response.ErrMessage = "Unable to get ScheduledSparkApplication. err: " + err.Error()
		return
	}

	response.Status = 0
	response.SparkApp = res
	return
}

func getScheduledJob(jobName string) (response scheduledBatchJobResponse) {
	getSchedSparkAppResponse := getScheduledSparkApplication(jobName)
	if getSchedSparkAppResponse.Status != 0 {
		response.Status = getSchedSparkAppResponse.Status
		response.ErrMessage = getSchedSparkAppResponse.ErrMessage
		return
	}
	res := getSchedSparkAppResponse.SparkApp

	var scheduledJob scheduledBatchJob
	scheduledJob.Name = res.ObjectMeta.Name
	scheduledJob.CreationTimestamp = res.ObjectMeta.GetCreationTimestamp().String()
	scheduledJob.Spec = res.Spec
	scheduledJob.Status = res.Status

	response.Status = 0
	response.ScheduledJob = scheduledJob
	return
}

/**
* handler for GET: /jobs
* get all jobs
**/
func getBatchJobs(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit list jobs endpoint")
	var onlyRunning bool = false
	if r.URL.Query().Get("running") != "" {
		var err error
		onlyRunning, err = strconv.ParseBool(r.URL.Query().Get("running"))
		if err != nil {
			log.Println("Invalid value for query running. Accepts true|false. err: ", err)
			return
		}
	}
	listJobsResponse := listJobs(onlyRunning)
	if listJobsResponse.Status == 1 {
		log.Println("Unable to get SparkApplications: ", listJobsResponse.ErrMessage)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Unable to get SparkApplications: " + listJobsResponse.ErrMessage))
		return
	}
	response, err := json.Marshal(listJobsResponse)
	if err != nil {
		log.Println("Failed to encode jobs list", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode jobs list: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

/**
* handler for GET: /scheduledjobs
* get all scheduledjobs
**/
func getScheduledBatchJobs(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit list scheduled jobs endpoint")
	listJobsResponse := listScheduledJobs()
	if listJobsResponse.Status == 1 {
		log.Println("Unable to get ScheduledSparkApplications: ", listJobsResponse.ErrMessage)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Unable to get ScheduledSparkApplications: " + listJobsResponse.ErrMessage))
		return
	}
	response, err := json.Marshal(listJobsResponse)
	if err != nil {
		log.Println("Failed to encode scheduled jobs list", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode scheduled jobs list: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

/**
* handler for GET: /scheduledjob/{name}
* get all scheduledjobs
**/
func getScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit get scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	getJobResponse := getScheduledJob(jobName)
	if getJobResponse.Status == 1 {
		log.Println("Unable to get ScheduledSparkApplication: ", getJobResponse.ErrMessage)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Unable to get ScheduledSparkApplication: " + getJobResponse.ErrMessage))
		return
	} else if getJobResponse.Status == 404 {
		log.Println("No scheduled job with name: ", jobName + ", " + getJobResponse.ErrMessage)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - No scheduled job with name:" + jobName + ", " + getJobResponse.ErrMessage))
		return
	}
	response, err := json.Marshal(getJobResponse)
	if err != nil {
		log.Println("Failed to encode scheduled jobs list", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode scheduled jobs list: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

type batchJobRunsResponse struct {
	Status  int        `json:"Status"`
	JobName string     `json:"JobName"`
	Runs    []batchJob `json:"Runs"`
}

/**
* get list for runs for job with name: jobName
* To-Do: When re-runs for single job is supported,
* 		 this function needs to correctly support getting list of all runs for a job
**/
func getRunsFromJobName(jobName string) []batchJob {
	log.Println("getting job with name: ", jobName)
	allJobRes := listJobs(false)
	for _, job := range allJobRes.Jobs {
		if job.Name == jobName {
			return []batchJob{job}
		}
	}
	log.Println("No job with name: ", jobName)
	return []batchJob{}
}

/**
* handler for GET: /job/{name}
* get all runs for a job with a given name
**/
func getBatchJobRuns(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit get job runs endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	runs := getRunsFromJobName(jobName)
	if len(runs) == 0 {
		log.Println("Error: No job with name: ", jobName)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 - Error: No job with name:" + jobName))
		return
	}
	var response batchJobRunsResponse
	response.Status = 0
	response.JobName = jobName
	response.Runs = runs
	reponse, err := json.Marshal(response)
	if err != nil {
		log.Println("Failed to encode response. error:", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(reponse)
}
