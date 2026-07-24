package delivery

import (
	"net/http"
	"sync/atomic"
)

type submissionMetrics struct {
	accepted atomic.Uint64
	rejected atomic.Uint64
}

func (metrics *submissionMetrics) observe(status int) {
	if status == http.StatusAccepted {
		metrics.accepted.Add(1)
		return
	}
	metrics.rejected.Add(1)
}

func (metrics *submissionMetrics) snapshot() map[string]uint64 {
	return map[string]uint64{
		"accepted": metrics.accepted.Load(),
		"rejected": metrics.rejected.Load(),
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (writer *statusResponseWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *statusResponseWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}
