package delivery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	maxDeliveryPayloadBytes = 256 * 1024
	maxSubmissionJSONBytes  = 512 * 1024
	statusAttemptLimit      = 20
)

type API struct {
	store             *Store
	logger            *slog.Logger
	now               func() time.Time
	submissionMetrics submissionMetrics
}

func NewAPI(store *Store, logger *slog.Logger) (*API, error) {
	if store == nil {
		return nil, errors.New("delivery API requires a store")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &API{
		store:  store,
		logger: logger,
		now:    time.Now,
	}, nil
}

func (api *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /deliveries", func(response http.ResponseWriter, request *http.Request) {
		observed := &statusResponseWriter{ResponseWriter: response}
		api.submit(observed, request)
		api.submissionMetrics.observe(observed.status)
	})
	mux.HandleFunc("GET /deliveries/{id}", api.get)
	mux.HandleFunc("POST /deliveries/{id}/replay", api.replay)
}

func (api *API) SubmissionCounts() map[string]uint64 {
	return api.submissionMetrics.snapshot()
}

type submissionRequest struct {
	DestinationID string          `json:"destination_id"`
	Method        string          `json:"method"`
	Headers       json.RawMessage `json:"headers"`
	BodyBase64    string          `json:"body_base64"`
}

type deliveryResponse struct {
	ID                 string                    `json:"id"`
	Status             string                    `json:"status"`
	DestinationID      string                    `json:"destination_id"`
	DestinationVersion int64                     `json:"destination_version"`
	Generation         int64                     `json:"generation"`
	AcceptedAt         time.Time                 `json:"accepted_at"`
	NextAttemptAt      time.Time                 `json:"next_attempt_at"`
	RetryDeadline      time.Time                 `json:"retry_deadline"`
	Attempts           *[]attemptSummaryResponse `json:"attempts,omitempty"`
}

type attemptSummaryResponse struct {
	Generation     int64     `json:"generation"`
	ResultClass    string    `json:"result_class"`
	ResponseStatus *int      `json:"response_status"`
	ErrorCategory  *string   `json:"error_category"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
}

func (api *API) submit(response http.ResponseWriter, request *http.Request) {
	callerID, ok := api.authenticate(response, request)
	if !ok {
		return
	}

	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" || len(idempotencyKey) > 200 {
		writeError(response, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required")
		return
	}

	request.Body = http.MaxBytesReader(response, request.Body, maxSubmissionJSONBytes)
	var input submissionRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		if isRequestTooLarge(err) {
			writeError(response, http.StatusRequestEntityTooLarge, "payload_too_large", "request exceeds size limit")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid_request", "request body is invalid")
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "request body must contain one JSON value")
		return
	}

	callerHeaders, err := decodeCallerHeaders(input.Headers)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_headers", "caller Header names must be unique")
		return
	}
	body, err := base64.StdEncoding.DecodeString(input.BodyBase64)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_body_encoding", "body_base64 is invalid")
		return
	}
	canonicalHeaders, err := CanonicalCallerHeaders(callerHeaders)
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_headers", "caller Header names must be unique")
		return
	}
	if payloadSize(canonicalHeaders, body) > maxDeliveryPayloadBytes {
		writeError(response, http.StatusRequestEntityTooLarge, "payload_too_large", "Header and Body payload exceeds 256 KiB")
		return
	}

	destination, err := api.store.AuthorizedDestination(request.Context(), callerID, input.DestinationID)
	if err != nil {
		if errors.Is(err, ErrDestinationDenied) {
			writeError(response, http.StatusForbidden, "destination_forbidden", "destination is not authorized")
			return
		}
		api.internalError(response, "resolve_destination", err)
		return
	}

	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if !containsFold(destination.AllowedMethods, method) {
		writeError(response, http.StatusBadRequest, "method_not_allowed", "method is not allowed for destination")
		return
	}
	if err := validateCallerHeaders(canonicalHeaders, destination); err != nil {
		writeError(response, http.StatusBadRequest, "header_not_allowed", err.Error())
		return
	}

	requestHash, err := RequestHash(HashInput{
		DestinationID:      destination.DestinationID,
		DestinationVersion: destination.Version,
		Method:             method,
		CallerHeaders:      canonicalHeaders,
		Body:               body,
	})
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", "request cannot be canonicalized")
		return
	}

	acceptedAt := api.now().UTC()
	delivery, err := api.store.Submit(request.Context(), Submission{
		CallerID:           callerID,
		IdempotencyKey:     idempotencyKey,
		RequestHash:        requestHash,
		DestinationID:      destination.DestinationID,
		DestinationVersion: destination.Version,
		Method:             method,
		CallerHeaders:      canonicalHeaders,
		Body:               body,
		AcceptedAt:         acceptedAt,
		RetryDeadline:      acceptedAt.Add(24 * time.Hour),
	})
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		writeError(response, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was reused with different content")
		return
	case errors.Is(err, ErrBacklogCapacity):
		response.Header().Set("Retry-After", "60")
		writeError(response, http.StatusServiceUnavailable, "backlog_capacity_exceeded", "active delivery backlog is full")
		return
	case err != nil:
		api.internalError(response, "submit_delivery", err)
		return
	}

	writeJSON(response, http.StatusAccepted, responseFromDelivery(delivery))
}

func decodeCallerHeaders(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return map[string]string{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	open, err := decoder.Token()
	if err != nil || open != json.Delim('{') {
		return nil, errors.New("caller Headers must be a JSON object")
	}
	headers := make(map[string]string)
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("caller Header name is invalid")
		}
		canonical := strings.ToLower(strings.TrimSpace(name))
		if _, duplicate := seen[canonical]; duplicate {
			return nil, errors.New("duplicate caller Header name")
		}
		seen[canonical] = struct{}{}
		var value string
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("caller Header value must be a string")
		}
		headers[name] = value
	}
	closeToken, err := decoder.Token()
	if err != nil || closeToken != json.Delim('}') {
		return nil, errors.New("caller Headers object is invalid")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return headers, nil
}

func (api *API) get(response http.ResponseWriter, request *http.Request) {
	callerID, ok := api.authenticate(response, request)
	if !ok {
		return
	}
	delivery, err := api.store.Get(request.Context(), callerID, request.PathValue("id"))
	if errors.Is(err, ErrNotFound) {
		writeError(response, http.StatusNotFound, "delivery_not_found", "delivery was not found")
		return
	}
	if err != nil {
		api.internalError(response, "get_delivery", err)
		return
	}
	attempts, err := api.store.ListAttemptSummaries(
		request.Context(),
		callerID,
		delivery.ID,
		statusAttemptLimit,
	)
	if err != nil {
		api.internalError(response, "list_delivery_attempts", err)
		return
	}
	payload := responseFromDelivery(delivery)
	summaries := make([]attemptSummaryResponse, 0, len(attempts))
	for _, attempt := range attempts {
		summaries = append(summaries, attemptSummaryResponse{
			Generation:     attempt.Generation,
			ResultClass:    attempt.ResultClass,
			ResponseStatus: attempt.ResponseStatus,
			ErrorCategory:  attempt.ErrorCategory,
			StartedAt:      attempt.StartedAt,
			FinishedAt:     attempt.FinishedAt,
		})
	}
	payload.Attempts = &summaries
	writeJSON(response, http.StatusOK, payload)
}

type replayRequest struct {
	Reason string `json:"reason"`
}

func (api *API) replay(response http.ResponseWriter, request *http.Request) {
	principal, ok := api.authenticatePrincipal(response, request)
	if !ok {
		return
	}
	if !principal.IsOperator {
		writeError(response, http.StatusForbidden, "operator_required", "operator authorization is required")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 4096)
	var input replayRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || ensureJSONEOF(decoder) != nil {
		writeError(response, http.StatusBadRequest, "invalid_replay", "replay request is invalid")
		return
	}
	reason := strings.TrimSpace(input.Reason)
	if reason == "" || len(reason) > 1000 {
		writeError(response, http.StatusBadRequest, "invalid_replay_reason", "reason must contain 1 to 1000 bytes")
		return
	}
	replayed, err := api.store.Replay(
		request.Context(),
		request.PathValue("id"),
		principal.CallerID,
		reason,
	)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(response, http.StatusNotFound, "delivery_not_found", "delivery was not found")
		return
	case errors.Is(err, ErrReplayConflict):
		writeError(response, http.StatusConflict, "delivery_not_replayable", "delivery is not permanently failed")
		return
	case err != nil:
		api.internalError(response, "replay_delivery", err)
		return
	}
	writeJSON(response, http.StatusAccepted, responseFromDelivery(replayed))
}

func (api *API) authenticate(response http.ResponseWriter, request *http.Request) (string, bool) {
	principal, ok := api.authenticatePrincipal(response, request)
	if !ok {
		return "", false
	}
	return principal.CallerID, true
}

func (api *API) authenticatePrincipal(
	response http.ResponseWriter,
	request *http.Request,
) (Principal, bool) {
	authorization := request.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) || len(authorization) == len(prefix) {
		writeError(response, http.StatusUnauthorized, "unauthorized", "valid caller authentication is required")
		return Principal{}, false
	}
	principal, err := api.store.AuthenticatePrincipal(request.Context(), authorization[len(prefix):])
	if errors.Is(err, ErrUnauthorized) {
		writeError(response, http.StatusUnauthorized, "unauthorized", "valid caller authentication is required")
		return Principal{}, false
	}
	if err != nil {
		api.internalError(response, "authenticate_caller", err)
		return Principal{}, false
	}
	return principal, true
}

func (api *API) internalError(response http.ResponseWriter, operation string, err error) {
	api.logger.Error("request failed", "operation", operation, "error", errorCategory(err))
	writeError(response, http.StatusServiceUnavailable, "service_unavailable", "service is temporarily unavailable")
}

func responseFromDelivery(delivery Delivery) deliveryResponse {
	return deliveryResponse{
		ID:                 delivery.ID,
		Status:             delivery.Status,
		DestinationID:      delivery.DestinationID,
		DestinationVersion: delivery.DestinationVersion,
		Generation:         delivery.Generation,
		AcceptedAt:         delivery.AcceptedAt,
		NextAttemptAt:      delivery.NextAttemptAt,
		RetryDeadline:      delivery.RetryDeadline,
	}
}

func validateCallerHeaders(headers map[string]string, destination DestinationVersion) error {
	allowed := make(map[string]struct{}, len(destination.AllowedHeaders))
	for _, name := range destination.AllowedHeaders {
		allowed[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	protected := map[string]struct{}{
		"authorization":       {},
		"proxy-authorization": {},
		"host":                {},
		"connection":          {},
		"keep-alive":          {},
		"proxy-authenticate":  {},
		"te":                  {},
		"trailer":             {},
		"transfer-encoding":   {},
		"upgrade":             {},
		"cookie":              {},
		"set-cookie":          {},
		"content-length":      {},
		strings.ToLower(destination.IdempotencyHeader): {},
		strings.ToLower(destination.CredentialHeader):  {},
	}
	for name := range headers {
		if _, denied := protected[name]; denied {
			return errors.New("caller attempted to set a protected Header")
		}
		if _, ok := allowed[name]; !ok {
			return errors.New("caller Header is not allowlisted")
		}
	}
	return nil
}

func containsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), candidate) {
			return true
		}
	}
	return false
}

func payloadSize(headers map[string]string, body []byte) int {
	size := len(body)
	for name, value := range headers {
		size += len(name) + len(value)
	}
	return size
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func isRequestTooLarge(err error) bool {
	var maxBytesError *http.MaxBytesError
	return errors.As(err, &maxBytesError)
}

func errorCategory(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "request_canceled"
	default:
		return "internal"
	}
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
