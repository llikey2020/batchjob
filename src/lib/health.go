package batchjob

import (
	"strconv"
	"net/http"
)

// healthCheck is the handler for GET: /healthcheck
// It will return the operational status of the batch-job service by 
// checking if CRDs sparkapplications.sparkoperator.k8s.io and scheduledsparkapplications.sparkoperator.k8s.io
// from Spark Operator exist.
// Writes a response with status and message on success.
// On failure, writes an error message in response.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	// Check if able to get the CRDs sparkapplications.sparkoperator.k8s.io, and scheduledsparkapplications.sparkoperator.k8s.io
	listJobsResponse := listJobs(false, true)
	if listJobsResponse.Status != http.StatusOK {
		logError(listJobsResponse.ErrMessage)
		w.WriteHeader(listJobsResponse.Status)
		w.Write([]byte(strconv.Itoa(listJobsResponse.Status) + " - Failed Health check: " + listJobsResponse.ErrMessage))
	}
	listScheduledJobsResponse := listScheduledJobs()
	if listScheduledJobsResponse.Status != http.StatusOK {
		logError(listScheduledJobsResponse.ErrMessage)
		w.WriteHeader(listScheduledJobsResponse.Status)
		w.Write([]byte(strconv.Itoa(listScheduledJobsResponse.Status) + " - Failed Health check: " + listScheduledJobsResponse.ErrMessage))
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Batch-job service passed health check"))
}
