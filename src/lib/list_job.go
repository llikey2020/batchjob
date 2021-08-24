package batchjob

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type batchJob struct {
	Name       string `json:"Name"`
	Id         string `json:"Id"`
	SparkUISvc string `json:"SparkUISvc"`
	Completed  bool   `json:"Completed"`
}

type batchJobsResponse struct {
	Status    int        `json:"Status"`
	TotalJobs int        `json:"TotalJobs"`
	Jobs      []batchJob `json:"Jobs"`
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

/**
* parse `sparkctl list` output.
* If onlyRunning is true: only return running jobs
* Otherwise: return all jobs
**/
func parseJobs(jobs string, onlyRunning bool) (totalRunningJobs int, runningJobs []batchJob) {
	fmt.Println("list jobs:\n", jobs)
	totalRunningJobs = 0
	runningJobs = make([]batchJob, 0)
	for _, line := range strings.Split(strings.TrimSuffix(jobs, "\n"), "\n") {
		if strings.HasPrefix(line, "|") {
			job := strings.Split(strings.Trim(line, "|"), "|")
			if strings.TrimSpace(job[0]) != "NAME" {
				if strings.TrimSpace(job[2]) == "RUNNING" {
					totalRunningJobs++
					runningJobs = append(runningJobs, batchJob{Name: strings.TrimSpace(job[0]),
						Id: strings.TrimSpace(job[1]), SparkUISvc: strings.TrimSpace(job[0]) + "-ui-svc", Completed: false})
				} else if !onlyRunning {
					totalRunningJobs++
					runningJobs = append(runningJobs, batchJob{Name: strings.TrimSpace(job[0]),
						Id: strings.TrimSpace(job[1]), SparkUISvc: strings.TrimSpace(job[0]) + "-ui-svc", Completed: true})
				}
			}
		}
	}
	return
}

func listJobs(onlyRunning bool) (response batchJobsResponse) {
	cmd := exec.Command("sparkctl", "list", "--namespace="+SPARKJOB_CONFS["SPARKJOB_NAMESPACE"])
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		response.Status = 1
		log.Println("ERROR:\n", err, out.String())
		return
	}
	response.Status = 0
	response.TotalJobs, response.Jobs = parseJobs(out.String(), onlyRunning)
	return
}

/**
* handler for GET: /jobs
* get all jobs
**/
func getBatchJobs(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hit list jobs endpoint")
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
	response, err := json.Marshal(listJobsResponse)
	if err != nil {
		log.Println("Failed to encode jobs list", err)
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
	fmt.Println("Hit get job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	runs := getRunsFromJobName(jobName)
	var response batchJobRunsResponse
	response.Status = 0
	response.JobName = jobName
	response.Runs = runs
	reponse, err := json.Marshal(response)
	if err != nil {
		log.Println("Failed to encode jobs list", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(reponse)
}
