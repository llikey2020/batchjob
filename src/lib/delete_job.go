package batchjob

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// deleteJob deletes a job with the given jobName. A job is represented by the SparkApplication CRD.
// It checks for a SparkApplication with name jobName, then deletes the object from the cluster.
// Returns a response with a status code and message depending on the status code.
func deleteJob(jobName string) (response serviceResponse) {
	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an kubernetes client. err: " + err.Error()
		return
	}

	// delete batch job manifest yaml
	fileName := jobName + manifestFileSuffix
	deleteResponse := deleteObject(S3_BUCKET_NAME, MANIFEST, fileName, false)
	if deleteResponse.Status != http.StatusOK {
		log.Println("Unable to delete scheduled batch job manifest file in S3. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to delete scheduled batch job manifest file in S3. err: " + err.Error()
		return
	}

	// delete any runs of this job
	getRunsResponse := getRunsFromJobName(jobName, false)
	if getRunsResponse.Status != http.StatusOK {
		log.Println("Unable to delete job runs. err: ", getRunsResponse.ErrMessage)
		response.Status = getRunsResponse.Status
		response.Output = "Unable to delete job runs. err: " + getRunsResponse.ErrMessage
		return
	}
	for _, item := range getRunsResponse.Jobs {
		deleteRunResponse := deleteJob(item.Name)
		if deleteRunResponse.Status != http.StatusOK {
			log.Println("Unable to delete job runs. err: ", deleteRunResponse.Output)
			response.Status = http.StatusInternalServerError
			response.Output = "Unable to delete job runs. err: " + deleteRunResponse.Output
			return
		}
	}
	// delete the job
	var res rest.Result
	res = clientset.RESTClient().Delete().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("SparkApplications").
				Name(jobName).
				Do(context.TODO())

	statusCode := http.StatusOK
	res = res.StatusCode(&statusCode)
	if statusCode == http.StatusNotFound {
		log.Println("SparkApplication with name "+ jobName +" does not exist. err: ", res.Error())
		response.Status = http.StatusNotFound
		response.Output = "SparkApplication with name "+ jobName +" does not exist. err: " + res.Error().Error()
		return
	} else if res.Error() != nil {
		log.Println("StatusCode:", statusCode)
		log.Println("Unable to DELETE SparkApplication. err: ", res.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to DELETE SparkApplication. err: " + res.Error().Error()
		return
	}
	
	response.Status = http.StatusOK
	response.Output = "Deleted job: " + jobName
	return
}

// deleteScheduledJob deletes a scheduled job with the given jobName. A scheduled job is represented by the ScheduledSparkApplication CRD.
// It checks for a ScheduledSparkApplication with name jobName, then deletes the object from the cluster.
// Returns a response with a status code and message depending on the status code.
func deleteScheduledJob(jobName string) (response serviceResponse) {
	// create the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Unable to create an in-cluster config. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an in-cluster config. err: " + err.Error()
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Println("Unable to create an kubernetes client. err: ", err)
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create an kubernetes client. err: " + err.Error()
		return
	}

	// delete batch job manifest yaml
	fileName := jobName + scheduledManifestFileSuffix
	deleteResponse := deleteObject(S3_BUCKET_NAME, MANIFEST, fileName, false)
	if deleteResponse.Status != http.StatusOK {
		log.Println("Unable to delete scheduled batch job manifest file in S3. err: ", err.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to delete scheduled batch job manifest file in S3. err: " + err.Error()
		return
	}

	var res rest.Result
	// delete the job
	res = clientset.RESTClient().Delete().
				AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
				Namespace(SPARKJOB_CONFS["SPARKJOB_NAMESPACE"]).
				Resource("ScheduledSparkApplications").
				Name(jobName).
				Do(context.TODO())
	statusCode := http.StatusOK
	res = res.StatusCode(&statusCode)
	if statusCode == http.StatusNotFound {
		log.Println("ScheduledSparkApplication with name "+ jobName +" does not exist. err: ", res.Error())
		response.Status = http.StatusNotFound
		response.Output = "ScheduledSparkApplication with name "+ jobName +" does not exist. err: " + res.Error().Error()
		return
	} else if res.Error() != nil {
		log.Println("StatusCode:", statusCode)
		log.Println("Unable to DELETE ScheduledSparkApplication. err: ", res.Error())
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to DELETE ScheduledSparkApplication. err: " + res.Error().Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Deleted scheduled job: " + jobName
	return
}

// deleteBatchJob is the handler for DELETE: /job/{name}
// It deletes a batch job with the given name in the URL by deleting a SparkApplication with the same name.
// Writes a response with status and message on success.
// On failure, writes an error message in response.
func deleteBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit delete job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	deleteJobResponse := deleteJob(jobName)
	if deleteJobResponse.Status != http.StatusOK {
		log.Println("Error deleting job: ", deleteJobResponse.Output)
		w.WriteHeader(deleteJobResponse.Status)
		w.Write([]byte(strconv.Itoa(deleteJobResponse.Status) + " - Error deleting job: " + deleteJobResponse.Output))
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

// deleteScheduledBatchJob is the handler for DELETE: /scheduledjob/{name}
// It deletes a scheduled batch job with the given name in the URL by deleting a ScheduledSparkApplication with the same name.
// Writes a response with status and message on success.
// On failure, writes an error message in response.
func deleteScheduledBatchJob(w http.ResponseWriter, r *http.Request) {
	log.Println("Hit delete scheduled job endpoint")
	vars := mux.Vars(r)
	jobName := vars["name"]
	deleteJobResponse := deleteScheduledJob(jobName)
	if deleteJobResponse.Status != http.StatusOK {
		log.Println("Error deleting scheduled job: ", deleteJobResponse.Output)
		w.WriteHeader(deleteJobResponse.Status)
		w.Write([]byte(strconv.Itoa(deleteJobResponse.Status) + " - Error deleting scheduled job: " + deleteJobResponse.Output))
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
