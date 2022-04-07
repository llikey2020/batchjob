package batchjob

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
)
// Region is a placeholder region for aws.Config. 
// The config requires the region field to be filled even if it is not being used for the S3 endpoint.
const Region = "temp"

const DefaultUser = "anonymous"

type FileType string

const (
	APPLICATION FileType = "applications"
	LIB			FileType = "libs"
	UPLOAD		FileType = "uploads"
	MANIFEST	FileType = "manifests"
)

var SupportedFileTypes = map[FileType]bool {
	APPLICATION: true,
	LIB:		 true,
	UPLOAD:		 true,
	MANIFEST:	 true,
}

// createS3Client will create an S3 client to the endpoint using the credentials and values in the values.yaml file of batch-job.
// Returns an S3 client on success, returns an error on failure.
func createS3Client() (*s3.S3, error) {
	var AccessKey = SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.access.key"]
	var SecretKey =  SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.secret.key"]
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(AccessKey, SecretKey,""),
		Endpoint: aws.String(SPARKJOB_SPARKCONFS["spark.hadoop.fs.s3a.endpoint"]),
		Region: aws.String(Region),
		DisableSSL:       aws.Bool(true),
        S3ForcePathStyle: aws.Bool(true),
	}
	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, err
	}
	s3Client := s3.New(sess)

	return s3Client, nil
}

// uploadFile uses the S3 client to upload a file to given bucket name and with given object key.
func uploadFile(bucketName string, fileType FileType, fileName string, isShared bool,
				file io.ReadSeeker) (response serviceResponse) {
	s3Client, err := createS3Client()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create S3 client: " + err.Error()
		return
	}

	rootFolderPath := generateRootFolder(fileType, DefaultUser, isShared)
	objectKey := fmt.Sprintf("%s/%s", rootFolderPath, fileName)

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to put object into S3 Bucket: " + bucketName + ", err: " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Uploaded file to S3 bucket: " + bucketName + " with object key: " + objectKey
	return
}

func generateRootFolder(fileType FileType, userName string, isShared bool) string {
	var rootFolderPath string
	if isShared {
		rootFolderPath = fmt.Sprintf("shared/%s", fileType)
	} else {
		rootFolderPath = fmt.Sprintf("%s/%s", userName, fileType)
	}

	return rootFolderPath
}

// deleteObject uses the S3 client to delete an object on S3 with the given object key on the given bucket.
func deleteObject(bucketName string, fileType FileType, fileName string, isShared bool) (response serviceResponse) {
	s3Client, err := createS3Client()
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to create S3 client: " + err.Error()
		return
	}

	rootFolderPath := generateRootFolder(fileType, DefaultUser, isShared)
	objectKey := fmt.Sprintf("%s/%s", rootFolderPath, fileName)

	_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		response.Status = http.StatusInternalServerError
		response.Output = "Unable to delete object from S3 Bucket " + bucketName + ": " + err.Error()
		return
	}

	response.Status = http.StatusOK
	response.Output = "Deleted file from S3 bucket: " + bucketName + " with object key: " + objectKey
	return
}

func getObject(bucketName string, fileType FileType, fileName string, isShared bool) (output *s3.GetObjectOutput, err error) {
	s3Client, err := createS3Client()
	if err != nil {
		log.Println("Unable to create S3 client: ", err)
		return nil, err
	}

	rootFolderPath := generateRootFolder(fileType, DefaultUser, isShared)
	objectKey := fmt.Sprintf("%s/%s", rootFolderPath, fileName)

	objectOutput, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		log.Println("Unable to get object from S3 Bucket " + bucketName + ":", err)
		return nil, err
	}

	return objectOutput, nil
}

// SS3UploadFile is the handler for PUT: /ss3/upload/{bucketName}/{fileName}
// It uploads the file in the multipart/form-data of the request to SS3 specified in values.yaml file for batch-job.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func SS3UploadFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	fileType := FileType(vars["fileType"])
	if _, ok := SupportedFileTypes[fileType]; !ok {
		log.Println("Unsupported file type:", fileType)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported file type:" + fileType))
		return
	}

	fileName := vars["fileName"]
	isShared, _ := strconv.ParseBool(vars["isShared"])

	f, _, err := r.FormFile("file")
	if err != nil {
		logError("Unable to read file from multipart/form-data: " + err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unable to read file from multipart/form-data: " + err.Error()))
		return
	}
	defer f.Close()

	uploadResponse := uploadFile(bucketName, fileType, fileName, isShared, f)
	if uploadResponse.Status != http.StatusOK {
		logError("Error uploading to SS3: " + uploadResponse.Output)
		w.WriteHeader(uploadResponse.Status)
		w.Write([]byte(strconv.Itoa(uploadResponse.Status) + " - Error uploading to SS3: " + uploadResponse.Output))
		return
	}
	// encode response
	response, err := json.Marshal(uploadResponse)
	if err != nil {
		logError("Failed to encode a response: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode a response: " + err.Error()))
		return
	}
	logInfo(uploadResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// deleteSS3Object is the handler for DELETE: /ss3/delete/{bucketName}/{fileName}
// It deletes an object in the SS3 specified in values.yaml with the given fileName.
// Writes a response with a status code and message.
// On failure, writes an error message in response.
func SS3DeleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucketName"]
	fileType := FileType(vars["fileType"])
	if _, ok := SupportedFileTypes[fileType]; !ok {
		log.Println("Unsupported file type:", fileType)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported file type:" + fileType))
		return
	}

	fileName := vars["fileName"]
	isShared, _ := strconv.ParseBool(vars["isShared"])

	deleteObjectResponse := deleteObject(bucketName, fileType, fileName, isShared)
	if deleteObjectResponse.Status != http.StatusOK {
		logError("Error deleting from SS3: " + deleteObjectResponse.Output)
		w.WriteHeader(deleteObjectResponse.Status)
		w.Write([]byte(strconv.Itoa(deleteObjectResponse.Status) + " - Error deleting from SS3: " + deleteObjectResponse.Output))
		return
	}
	// encode response
	response, err := json.Marshal(deleteObjectResponse)
	if err != nil {
		logError("Failed to encode a response: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("500 - Failed to encode a response: " + err.Error()))
		return
	}
	logInfo(deleteObjectResponse.Output)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}
