package batchjob

import (
	"log"
	"time"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// serviceName is the name of batch-job service to be added to log messages
const serviceName = "batch-job"
// callerSkip is used to skip this logging wrapper so log messages will contain the file and line where the logger was called at correctly.
const callerSkip = 1
var logger *zap.SugaredLogger

// initializeLogging will create a sugared logger used to create structured logs
func initializeLogging() {
	// configure logger to use timestamp instead of the default unix time
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.EncoderConfig.TimeKey = "timestamp"
    loggerConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)
	zapLogger, err := loggerConfig.Build(
		zap.AddCallerSkip(callerSkip),
	)
	if err != nil {
		log.Fatalf("Can't initialize zap logger: %v", err)
	}
	zapLogger = zapLogger.With(
		zap.String("service", serviceName),
	)
	logger = zapLogger.Sugar()
	logger.Info("Logger initialized")
}

func logDebug(msg string) {
	logger.Debug(msg)
}

func logInfo(msg string) {
	logger.Info(msg)
}

func logInfow(msg string, keysAndValues ...interface{}) {
	logger.Infow(msg, keysAndValues)
}

func logInfof(template string, args ...interface{}) {
	logger.Infof(template, args)
}

func logWarning(msg string) {
	logger.Warn(msg)
}

func logWarningw(msg string, keysAndValues ...interface{}) {
	logger.Warnw(msg, keysAndValues)
}

func logError(msg string) {
	logger.Error(msg)
}

func logErrorw(msg string, keysAndValues ...interface{}) {
	logger.Errorw(msg, keysAndValues)
}

func logDPanic(msg string) {
	logger.DPanic(msg)
}

func logPanic(msg string) {
	logger.Panic(msg)
}

func logFatal(msg string) {
	logger.Fatal(msg)
}
