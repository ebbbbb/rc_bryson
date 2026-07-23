package delivery

import (
	"errors"
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
)

type DestinationVersion struct {
	DestinationID     string
	Version           int64
	URL               string
	NetworkPolicy     string
	AllowedMethods    []string
	AllowedHeaders    []string
	SecretRef         string
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
