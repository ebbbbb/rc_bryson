package delivery

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	StatusPending         = "pending"
	StatusDelivering      = "delivering"
	StatusSucceeded       = "succeeded"
	StatusFailedPermanent = "failed_permanent"
)

var (
	ErrUnauthorized        = errors.New("caller authentication failed")
	ErrDestinationDenied   = errors.New("destination is not authorized")
	ErrIdempotencyConflict = errors.New("idempotency key was reused with different content")
	ErrBacklogCapacity     = errors.New("active delivery backlog capacity exceeded")
	ErrNotFound            = errors.New("delivery not found")
	ErrReplayConflict      = errors.New("only a permanently failed delivery can be replayed")
)

type Principal struct {
	CallerID   string
	IsOperator bool
}

type DestinationVersion struct {
	DestinationID     string
	Version           int64
	URL               string
	NetworkPolicy     string
	AllowedMethods    []string
	AllowedHeaders    []string
	SecretRef         string
	CredentialHeader  string
	IdempotencyHeader string
	SuccessStatuses   []int32
	RetryStatuses     []int32
	ConnectTimeout    time.Duration
	RequestTimeout    time.Duration
	RatePerSecond     float64
	MaxConcurrency    int
}

type Delivery struct {
	ID                       string
	CallerID                 string
	DestinationID            string
	DestinationVersion       int64
	Method                   string
	CallerHeaders            map[string]string
	Body                     []byte
	SupplierIdempotencyValue string
	Status                   string
	Generation               int64
	AcceptedAt               time.Time
	RetryDeadline            time.Time
	NextAttemptAt            time.Time
	LeaseOwner               string
	LeaseToken               string
	LeaseUntil               time.Time
}

type AttemptSummary struct {
	Generation     int64
	ResultClass    string
	ResponseStatus *int
	ErrorCategory  *string
	StartedAt      time.Time
	FinishedAt     time.Time
}

type Submission struct {
	CallerID                 string
	IdempotencyKey           string
	RequestHash              [32]byte
	DestinationID            string
	DestinationVersion       int64
	Method                   string
	CallerHeaders            map[string]string
	Body                     []byte
	SupplierIdempotencyValue string
	AcceptedAt               time.Time
	RetryDeadline            time.Time
}

type BootstrapConfig struct {
	CallerID     string
	CallerAPIKey string
	Destination  DestinationVersion
}

func IsSupportedOutboundMethod(method string) bool {
	return strings.EqualFold(method, http.MethodPost) ||
		strings.EqualFold(method, http.MethodPut) ||
		strings.EqualFold(method, http.MethodPatch) ||
		strings.EqualFold(method, http.MethodDelete)
}
