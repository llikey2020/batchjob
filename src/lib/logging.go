package batchjob

import (
	"log"
	"go.uber.org/zap"
)

const ServiceName = "batch-job"
const callerSkip = 1
var logger *zap.SugaredLogger

func initializeLogging() {
	zapLogger, err := zap.NewProduction(
		zap.AddCallerSkip(callerSkip),
	)
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	zapLogger = zapLogger.With(
		zap.String("service", ServiceName),
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
