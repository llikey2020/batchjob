package batchjob

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"context"
	"sort"
	"errors"

	"github.com/gorilla/mux"
	"github.com/olekukonko/tablewriter"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// sparkApplicationResponse holds status and SparkApplication object outputted as response to user.
// ErrMessage contains an error message if getting a SparkApplication fails.
type sparkApplicationResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status     int              `json:"Status"`
	// SparkApp is a SparkApplication object the user requests.
	SparkApp   SparkApplication `json:"SparkApp"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage string           `json:"ErrorMessage,omitempty"`
}

// batchJob holds fields of a batch job outputted in response to user getting batch job(s).
type batchJob struct {
	// Name of a batch job
	Name              string                `json:"Name"`
	// Id is Spark Application ID of a batch job.
	Id                string                `json:"Id"`
	// SparkUISvc is the spark UI name of the job.
	SparkUISvc        string                `json:"SparkUISvc"`
	// State is current state of the job.
	State             ApplicationStateType  `json:"State"`
	// CreationTimestamp is timestamp of when the job was created.
	CreationTimestamp string                `json:"CreationTimestamp"`
	// Spec is the spec of the SparkApplication object representing the batch job.
	Spec              *SparkApplicationSpec `json:"Spec,omitempty"`
}

// batchJobsResponse holds status, number of jobs, and batch jobs output as response to getting batch jobs.
// ErrMessage contains an error message if getting batch jobs fails.
type batchJobsResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status     int        `json:"Status"`
	TotalJobs  int        `json:"TotalJobs"`
	// Jobs is a list of batch jobs on the cluster namespace.
	Jobs       []batchJob `json:"Jobs"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage string     `json:"ErrorMessage,omitempty"`
}

// scheduledBatchJob holds fields of a scheduled batch job outputted in response to user getting scheduled batch job(s).
type scheduledBatchJob struct {
	// Name of a scheduled batch job.
	Name              string                          `json:"Name"`
	// CreationTimestamp is timestamp of when the job was created.
	CreationTimestamp string                          `json:"CreationTimestamp"`
	// Spec is the spec of the ScheduledSparkApplication object representing the scheduled batch job.
	Spec              ScheduledSparkApplicationSpec   `json:"Spec"`
	// Status is the status of the scheduled batch job.
	Status            ScheduledSparkApplicationStatus `json:"Status,omitempty"`
	// PastRuns is a list of the past job runs of a scheduled batch jobs.
	PastRuns          []batchJob                      `json:"PastRuns,omitempty"`
}

// scheduledSparkApplicationResponse holds status and ScheduledSparkApplication object outputted as response to user.
// ErrMessage contains an error message if getting a ScheduledSparkApplication fails.
type scheduledSparkApplicationResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status     int                       `json:"Status"`
	// SparkApp is a ScheduledSparkApplication object the user requests.
	SparkApp   ScheduledSparkApplication `json:"ScheduledSparkApp"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage string                    `json:"ErrorMessage,omitempty"`
}

// scheduledBatchJobsResponse holds status, number of jobs, and scheduled batch jobs outputted as response to getting scheduled batch jobs.
// ErrMessage contains an error message if getting scheduled batch jobs fails. 
type scheduledBatchJobsResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status        int                 `json:"Status"`
	TotalJobs     int                 `json:"TotalJobs"`
	// ScheduledJobs is a list of scheduled batch jobs on the cluster namespace.
	ScheduledJobs []scheduledBatchJob `json:"ScheduledJobs"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage    string              `json:"ErrorMessage,omitempty"`
}

// scheduledBatchJobResponse status, number of jobs, and scheduled batch job outputted as response to getting scheduled batch job.
// ErrMessage contains an error message if getting a scheduled batch job fails.
type scheduledBatchJobResponse struct {
	// Status is an HTTP status code returned to the user for their request.
	Status       int               `json:"Status"`
	// ScheduledJobs is a scheduled batch job on the cluster namespace.
	ScheduledJob scheduledBatchJob `json:"ScheduledJob"`
	// ErrMessage is an error message if the user request fails.
	ErrMessage   string            `json:"ErrorMessage,omitempty"`
}

// getSparkApplication gets the SparkApplication object on the current cluster and namespace.
// Returns a response with a SparkApplication object with name jobName, or an error message if unable to get the SparkApplication.
func getSparkApplication(jobName string) (response sparkApplicationResponse) {
	// get config and clientset
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
	// get SparkApplication using the rest client
	res := SparkApplication{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("SparkApplications").
		Name(jobName).
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.ErrMessage = "Unable to get SparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.SparkApp = res
	return
}

// getJobFromId gets a batch job by Spark Application Id.
// Returns the batch job with the given Spark Application Id.
// If SparkApplication with given Id can't be found, returns an empty batchJob
func getJobFromId(id string) batchJob {
	allJobRes := listJobs(false, false)
	for _, job := range allJobRes.Jobs {
		if job.Id == id {
			return job
		}
	}
	return batchJob{}
}

// printJobs prints a table of the jobs/SparkApplications given.
// The table headers are "Name", "Application Id", "State", "Submission Attempt Time", and "Termination Time".
// Output of this table can be seen in the batch job service pod logs.
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

// readSparkApplicationsIntoBatchJob takes SparkApplications and puts them into into a list of batchJob items.
// Essentially acts to get certain fields in SparkApplication for the user into batchJob.
// onlyRunning set to true will make this return batch jobs in the RUNNING state only.
// noJobRuns set to true will make this exclude returning batch job runs (SparkApplication created by run job endpoint)
func readSparkApplicationsIntoBatchJob(items []SparkApplication, onlyRunning bool, noJobRuns bool) []batchJob {
	jobs := []batchJob{}
	for _, item := range items {
		labels := item.GetLabels()
		if val, prs := labels["sparkAppType"]; noJobRuns && prs && val == RunSparkAppType {
			continue
		}
		if onlyRunning && item.Status.AppState.State != "RUNNING" {
			continue
		}
		var job batchJob
		job.Name = item.ObjectMeta.Name
		job.Id = item.Status.SparkApplicationID
		job.SparkUISvc = item.Status.DriverInfo.WebUIServiceName
		job.State = item.Status.AppState.State
		job.CreationTimestamp = item.ObjectMeta.GetCreationTimestamp().String()
		sparkAppSpec := item.Spec
		job.Spec = &sparkAppSpec
		jobs = append(jobs, job)
	}
	return jobs
}

// readScheduledSparkApplicationsIntoScheduledBatchJob takes ScheduledSparkApplications and puts them into into a list of scheduledBatchJob items.
// Essentially acts to get certain fields in ScheduledSparkApplication for the user into scheduledBatchJob.
func readScheduledSparkApplicationsIntoScheduledBatchJob(items []ScheduledSparkApplication) []scheduledBatchJob {
	scheduledJobs := []scheduledBatchJob{}
	for _, item := range items {
		var scheduledJob scheduledBatchJob
		scheduledJob.Name = item.ObjectMeta.Name
		scheduledJob.CreationTimestamp = item.ObjectMeta.GetCreationTimestamp().String()
		scheduledJob.Spec = item.Spec
		scheduledJob.Status = item.Status
		// get and order past runs
		runHistoryLimit := *item.Spec.SuccessfulRunHistoryLimit
		if *item.Spec.FailedRunHistoryLimit < *item.Spec.SuccessfulRunHistoryLimit{
			runHistoryLimit = *item.Spec.FailedRunHistoryLimit
		}
		pastRunNames := append(scheduledJob.Status.PastSuccessfulRunNames, scheduledJob.Status.PastFailedRunNames...)
		pastRunSparkApps := getPastScheduledJobRuns(pastRunNames)
		// sort jobs by creation time
		sort.Slice(pastRunSparkApps, func(i, j int) bool {
			t1 := pastRunSparkApps[i].ObjectMeta.GetCreationTimestamp()
			t2 := pastRunSparkApps[j].ObjectMeta.GetCreationTimestamp()
			return t1.Before(&t2)
		})
		// limit by the runHistoryLimit, the amount of jobs returned
		if int32(len(pastRunSparkApps)) > runHistoryLimit {
			pastRunSparkApps = pastRunSparkApps[int32(len(pastRunSparkApps)) - runHistoryLimit:]
		}
		for _, runItem := range pastRunSparkApps {
			var job batchJob
			job.Name = runItem.ObjectMeta.Name
			job.Id = runItem.Status.SparkApplicationID
			job.SparkUISvc = runItem.Status.DriverInfo.WebUIServiceName
			job.State = runItem.Status.AppState.State
			job.CreationTimestamp = runItem.ObjectMeta.GetCreationTimestamp().String()
			scheduledJob.PastRuns = append(scheduledJob.PastRuns, job)
		}
		scheduledJobs = append(scheduledJobs, scheduledJob)
	}
	return scheduledJobs
}

// listJobs gets SparkApplications, gets the relevant information and puts them into batchJob structs.
// onlyRunning set to true will make this return batch jobs in the RUNNING state only.
// noJobRuns set to true will make this exclude returning batch job runs (SparkApplication created by run job endpoint)
func listJobs(onlyRunning bool, noRuns bool) (response batchJobsResponse) {
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
	res := SparkApplicationList{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("SparkApplications").
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.ErrMessage = "Unable to get SparkApplications. err: " + err.Error()
		return
	}
	response.Jobs = readSparkApplicationsIntoBatchJob(res.Items, onlyRunning, noRuns)

	response.Status = http.StatusOK
	response.TotalJobs = len(response.Jobs)
	return
}

// getPastScheduledJobRuns given a list of run names (i.e SparkApplication names), and gets a list of SparkApplication.
func getPastScheduledJobRuns(pastRunNames []string) (sparkAppList []SparkApplication) {
	for _, name := range pastRunNames {
		sparkAppResponse := getSparkApplication(name)
		if sparkAppResponse.Status != http.StatusOK {
			continue
		}
		sparkAppList = append(sparkAppList, sparkAppResponse.SparkApp)
	}
	return
}

// listScheduledJobs gets ScheduledSparkApplication, and puts them into scheduledBatchJob structs.
func listScheduledJobs() (response scheduledBatchJobsResponse) {
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
	res := ScheduledSparkApplicationList{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("ScheduledSparkApplications").
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.ErrMessage = "Unable to get ScheduledSparkApplications. err: " + err.Error()
		return
	}
	// get only relevant data and metadata into result
	response.ScheduledJobs = readScheduledSparkApplicationsIntoScheduledBatchJob(res.Items)

	response.Status = http.StatusOK
	response.TotalJobs = len(response.ScheduledJobs)
	return
}

// getScheduledSparkApplication gets a ScheduledSparkApplication with the name jobName on the cluster and namespace.
func getScheduledSparkApplication(jobName string) (response scheduledSparkApplicationResponse) {
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
	res := ScheduledSparkApplication{}
	err = clientset.RESTClient().Get().
		AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		Resource("ScheduledSparkApplications").
		Name(jobName).
		Do(context.TODO()).
		Into(&res)
	if err != nil {
		response.Status = http.StatusInternalServerError
		if status := k8serrors.APIStatus(nil); errors.As(err, &status) {
			response.Status = int(status.Status().Code)
		}
		response.ErrMessage = "Unable to get ScheduledSparkApplication. err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.SparkApp = res
	return
}

// getScheduledJob gets a ScheduledSparkApplication with name jobName.
// Returns a scheduledBatchJob which contains data from a ScheduledSparkApplication with name as jobName.
func getScheduledJob(jobName string) (response scheduledBatchJobResponse) {
	getSchedSparkAppResponse := getScheduledSparkApplication(jobName)
	if getSchedSparkAppResponse.Status != http.StatusOK {
		response.Status = getSchedSparkAppResponse.Status
		response.ErrMessage = getSchedSparkAppResponse.ErrMessage
		return
	}
	res := getSchedSparkAppResponse.SparkApp
	scheduledJob := readScheduledSparkApplicationsIntoScheduledBatchJob([]ScheduledSparkApplication{res})[0]

	response.Status = http.StatusOK
	response.ScheduledJob = scheduledJob
	return
}

// getBatchJobs is the handler for GET: /jobs/list
// It gets SparkApplications (that don't have the sparkAppType=RunSparkAppType label) and reads each SparkApplication into batchJob type.
// Writes a response with a list of batch jobs.
// On failure, writes an error message in response.
func getBatchJobs(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit list jobs endpoint")
	var onlyRunning bool = false
	if r.URL.Query().Get("running") != "" {
		var err error
		onlyRunning, err = strconv.ParseBool(r.URL.Query().Get("running"))
		if err != nil {
			logError("Invalid value for query running. Accepts true|false. err: " + err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(strconv.Itoa(http.StatusBadRequest) + "- Invalid value for query running. Accepts true|false. err: " + err.Error()))
			return
		}
	}
	listJobsResponse := listJobs(onlyRunning, true)
	if listJobsResponse.Status != http.StatusOK {
		logError("Unable to get SparkApplications: " + listJobsResponse.ErrMessage)
		w.WriteHeader(listJobsResponse.Status)
		w.Write([]byte(strconv.Itoa(listJobsResponse.Status) + " - Unable to get SparkApplications: " + listJobsResponse.ErrMessage))
		return
	}

	response, err := json.Marshal(listJobsResponse)
	if err != nil {
		logError("Failed to encode jobs list: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode jobs list: " + err.Error()))
		return
	}
	logInfo("Successfully get batch jobs")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

/**
* handler for GET: /scheduledjobs
* get all scheduledjobs
**/
// getScheduledBatchJobs is the handler for GET: /scheduledjobs/list
// It gets ScheduledSparkApplications and reads each ScheduledSparkApplication into scheduledBatchJob type.
// Writes a response with a list of scheduled batch jobs.
// On failure, writes an error message in response.
func getScheduledBatchJobs(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit list scheduled jobs endpoint")
	listJobsResponse := listScheduledJobs()
	if listJobsResponse.Status != http.StatusOK {
		logError("Unable to get ScheduledSparkApplications: " + listJobsResponse.ErrMessage)
		w.WriteHeader(listJobsResponse.Status)
		w.Write([]byte(strconv.Itoa(listJobsResponse.Status) + " - Unable to get ScheduledSparkApplications: " + listJobsResponse.ErrMessage))
		return
	}

	response, err := json.Marshal(listJobsResponse)
	if err != nil {
		logError("Failed to encode scheduled jobs list: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode scheduled jobs list: " + err.Error()))
		return
	}
	logInfo("Successfully get scheduled batch jobs")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// getScheduledBatchJobs is the handler for GET: /scheduledjobs/get/{name}
// It gets a ScheduledSparkApplication with the given name and reads it into a scheduledBatchJob type.
// Writes a response with a scheduled batch job.
// On failure, writes an error message in response.
func getScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit get scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	getJobResponse := getScheduledJob(jobName)
	if getJobResponse.Status != http.StatusOK {
		logError("Unable to get ScheduledSparkApplication: " + getJobResponse.ErrMessage)
		w.WriteHeader(getJobResponse.Status)
		w.Write([]byte(strconv.Itoa(getJobResponse.Status) + " - Unable to get ScheduledSparkApplication: " + getJobResponse.ErrMessage))
		return
	}

	response, err := json.Marshal(getJobResponse)
	if err != nil {
		logError("Failed to encode scheduled jobs list: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode scheduled jobs list: " + err.Error()))
		return
	}
	logInfo("Successfully get scheduled batch job: " + jobName)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// batchJobRunsResponse holds fields for response to getting runs of a job.
type batchJobRunsResponse struct {
	Status  int        `json:"Status"`
	JobName string     `json:"JobName"`
	Runs    []batchJob `json:"Runs"`
}

// getRunsFromJobName will get runs of a job with jobName.
// Runs are represented as SparkApplication objects in the k8s cluster with two labels: originalJobName and sparkAppType.
// Get runs by getting SparkApplication objects labels originalJobName=jobName and sparkAppType=RunSparkAppType.
// If includeOriginalJob = true, will include the job with name = jobName in the list of runs returned.
// Returns a list of batchJob representing the runs in chronological order of creation.
func getRunsFromJobName(jobName string, includeOriginalJob bool) (response batchJobsResponse) {
	// use dynamic client and LabelSelector in ListOptions
	config, err := rest.InClusterConfig()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to create a dynamic client. err: " + err.Error()
		return
	}

	// get list of SparkApplications with appropriate labels
	runLabels := "originalJobName=" + jobName + ",sparkAppType=" + RunSparkAppType 
	deploymentRes := schema.GroupVersionResource{Group: "sparkoperator.k8s.io", Version: "v1beta2", Resource: "sparkapplications"}
	result, err := dynamicClient.Resource(deploymentRes).
		Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
		List(context.TODO(), metav1.ListOptions{
			LabelSelector: runLabels,
		})
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to get SparkApplications: " + err.Error()
		return
	}

	// Marshal Items(type: []Unstructured) then Unmarshal into []SparkApplication type
	jsonData, err := json.Marshal(result.Items)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to Marshal SparkApplications: " + err.Error()
		return
	}
	var sparkAppList []SparkApplication
	err = json.Unmarshal(jsonData, &sparkAppList)
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.ErrMessage = "Unable to Unmarshal SparkApplications: " + err.Error()
		return
	}
	// sort SparkApplications by ascending creation time
	sort.Slice(sparkAppList, func(i, j int) bool {
		t1 := sparkAppList[i].ObjectMeta.GetCreationTimestamp()
		t2 := sparkAppList[j].ObjectMeta.GetCreationTimestamp()
		return t1.Before(&t2)
	})
	response.Jobs = readSparkApplicationsIntoBatchJob(sparkAppList, false, false)
	if includeOriginalJob {
		// get original job run and prepend it to jobs
		getSparkAppResponse := getSparkApplication(jobName)
		if getSparkAppResponse.Status != http.StatusOK {
			response.Status = getSparkAppResponse.Status
			response.ErrMessage = getSparkAppResponse.ErrMessage
			return
		}
		sparkApp := getSparkAppResponse.SparkApp
		var job batchJob
		job.Name = sparkApp.ObjectMeta.Name
		job.Id = sparkApp.Status.SparkApplicationID
		job.SparkUISvc = sparkApp.Status.DriverInfo.WebUIServiceName
		job.State = sparkApp.Status.AppState.State
		job.CreationTimestamp = sparkApp.ObjectMeta.GetCreationTimestamp().String()
		job.Spec = &sparkApp.Spec
		response.Jobs = append([]batchJob{job}, response.Jobs...)
	}

	response.Status = http.StatusOK
	response.TotalJobs = len(response.Jobs)
	return 
}

// getBatchJobRuns is the handler for GET: /jobs/get/{name}
// It gets all created runs of the batch job with the name in the request URL.
// Writes a response with a list of runs.
// On failure, writes an error message in response.
func getBatchJobRuns(w http.ResponseWriter, r *http.Request) {
	logInfo("Hit get job runs endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	runsResponse := getRunsFromJobName(jobName, true)
	if runsResponse.Status != http.StatusOK {
		logError("Unable to get job runs for job:" + jobName + ", err:" + runsResponse.ErrMessage)
		w.WriteHeader(runsResponse.Status)
		w.Write([]byte(strconv.Itoa(runsResponse.Status) + " - Unable to get job runs for job:" + jobName + ", err:" + runsResponse.ErrMessage))
		return
	}

	var batchJobRunsResponse batchJobRunsResponse
	batchJobRunsResponse.Status = http.StatusOK
	batchJobRunsResponse.JobName = jobName
	batchJobRunsResponse.Runs = runsResponse.Jobs
	response, err := json.Marshal(batchJobRunsResponse)
	if err != nil {
		logError("Failed to encode response. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode response. error: " + err.Error()))
		return
	}
	logInfo("Successfully got runs for batch job: " + jobName)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
