package outbound

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"reliable-notifier/internal/delivery"
)

const testNetworkPolicy = "test-only-supplier"

type Resolver interface {
	LookupIP(context.Context, string, string) ([]net.IP, error)
}

type SecretProvider interface {
	Resolve(context.Context, string) (string, error)
}

type permanentError struct {
	cause error
}

func (err permanentError) Error() string {
	return err.cause.Error()
}

func (err permanentError) Unwrap() error {
	return err.cause
}

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

func permanent(err error) error {
	return permanentError{cause: err}
}

type EnvironmentSecrets struct{}

func (EnvironmentSecrets) Resolve(_ context.Context, reference string) (string, error) {
	const prefix = "env:"
	if !strings.HasPrefix(reference, prefix) {
		return "", errors.New("unsupported secret reference")
	}
	name := strings.TrimPrefix(reference, prefix)
	if name == "" {
		return "", errors.New("empty secret environment reference")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", errors.New("referenced secret is unavailable")
	}
	return value, nil
}

type Sender struct {
	resolver     Resolver
	secrets      SecretProvider
	dial         func(context.Context, string, string) (net.Conn, error)
	testPolicy   bool
	testCAFile   string
	testHostname string
}

type Result struct {
	StatusCode int
	RetryAfter string
}

func NewSender(
	resolver Resolver,
	secrets SecretProvider,
	testPolicy bool,
	testCAFile string,
) (*Sender, error) {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if secrets == nil {
		return nil, errors.New("outbound sender requires a secret provider")
	}
	return &Sender{
		resolver:     resolver,
		secrets:      secrets,
		testPolicy:   testPolicy,
		testCAFile:   testCAFile,
		testHostname: "fake-supplier.test",
	}, nil
}

func (sender *Sender) Send(
	ctx context.Context,
	task delivery.Delivery,
	destination delivery.DestinationVersion,
) (Result, error) {
	target, err := validateURL(destination.URL)
	if err != nil {
		return Result{}, permanent(err)
	}
	if !strings.EqualFold(task.Method, http.MethodPost) &&
		!strings.EqualFold(task.Method, http.MethodPut) &&
		!strings.EqualFold(task.Method, http.MethodPatch) &&
		!strings.EqualFold(task.Method, http.MethodDelete) {
		return Result{}, permanent(errors.New("delivery method is not safe for configured outbound use"))
	}
	if !containsFold(destination.AllowedMethods, task.Method) {
		return Result{}, permanent(errors.New("delivery method is not allowed by bound destination version"))
	}
	if err := validateStoredHeaders(task.CallerHeaders, destination); err != nil {
		return Result{}, permanent(err)
	}

	addresses, err := sender.resolver.LookupIP(ctx, "ip", target.Hostname())
	if err != nil {
		return Result{}, fmt.Errorf("resolve registered destination: %w", err)
	}
	validatedIP, err := sender.validateAddresses(target.Hostname(), destination.NetworkPolicy, addresses)
	if err != nil {
		return Result{}, permanent(err)
	}
	roots, err := sender.roots(destination.NetworkPolicy)
	if err != nil {
		return Result{}, permanent(err)
	}
	secret, err := sender.secrets.Resolve(ctx, destination.SecretRef)
	if err != nil {
		return Result{}, fmt.Errorf("resolve destination credential: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, task.Method, target.String(), bytes.NewReader(task.Body))
	if err != nil {
		return Result{}, permanent(fmt.Errorf("build outbound request: %w", err))
	}
	for name, value := range task.CallerHeaders {
		request.Header.Set(name, value)
	}
	request.Header.Set(destination.IdempotencyHeader, task.SupplierIdempotencyValue)
	request.Header.Set(destination.CredentialHeader, secret)

	port := target.Port()
	if port == "" {
		port = "443"
	}
	transport := &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: target.Hostname(),
		},
		DialContext: func(dialContext context.Context, network, address string) (net.Conn, error) {
			host, dialPort, splitErr := net.SplitHostPort(address)
			if splitErr != nil {
				return nil, splitErr
			}
			if !strings.EqualFold(host, target.Hostname()) || dialPort != port {
				return nil, errors.New("outbound transport refused an unvalidated endpoint")
			}
			dial := sender.dial
			if dial == nil {
				dial = (&net.Dialer{Timeout: destination.ConnectTimeout}).DialContext
			}
			return dial(
				dialContext,
				network,
				net.JoinHostPort(validatedIP.String(), port),
			)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   destination.RequestTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return Result{}, fmt.Errorf("perform outbound HTTPS request: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	return Result{
		StatusCode: response.StatusCode,
		RetryAfter: response.Header.Get("Retry-After"),
	}, nil
}

func validateURL(rawURL string) (*url.URL, error) {
	target, err := url.ParseRequestURI(rawURL)
	if err != nil ||
		target.Scheme != "https" ||
		target.Host == "" ||
		target.Hostname() == "" ||
		target.User != nil ||
		target.Fragment != "" {
		return nil, errors.New("destination must be an absolute HTTPS URL without userinfo or fragment")
	}
	if target.Port() != "" {
		port, err := strconv.Atoi(target.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, errors.New("destination port is invalid")
		}
	}
	return target, nil
}

func (sender *Sender) validateAddresses(hostname, policy string, addresses []net.IP) (net.IP, error) {
	if len(addresses) == 0 {
		return nil, errors.New("destination DNS returned no addresses")
	}
	if policy == testNetworkPolicy {
		if !sender.testPolicy || hostname != sender.testHostname {
			return nil, errors.New("test-only destination policy is unavailable")
		}
		return addresses[0], nil
	}
	if policy != "public-internet" {
		return nil, errors.New("destination network policy is not supported")
	}
	for _, address := range addresses {
		if forbidden(address) {
			return nil, errors.New("destination resolved to a forbidden address")
		}
	}
	return addresses[0], nil
}

func (sender *Sender) roots(policy string) (*x509.CertPool, error) {
	if policy != testNetworkPolicy {
		return nil, nil
	}
	if !sender.testPolicy || sender.testCAFile == "" {
		return nil, errors.New("test-only TLS trust is unavailable")
	}
	certificate, err := os.ReadFile(sender.testCAFile)
	if err != nil {
		return nil, fmt.Errorf("read test-only CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return nil, errors.New("parse test-only CA")
	}
	return roots, nil
}

func forbidden(address net.IP) bool {
	return address == nil ||
		address.IsUnspecified() ||
		address.IsLoopback() ||
		address.IsPrivate() ||
		address.IsLinkLocalUnicast() ||
		address.IsLinkLocalMulticast() ||
		address.IsMulticast()
}

func validateStoredHeaders(headers map[string]string, destination delivery.DestinationVersion) error {
	if !validInjectedHeader(destination.CredentialHeader) ||
		!validInjectedHeader(destination.IdempotencyHeader) ||
		strings.EqualFold(destination.CredentialHeader, destination.IdempotencyHeader) {
		return errors.New("destination injection Header configuration is invalid")
	}
	allowed := make(map[string]struct{}, len(destination.AllowedHeaders))
	for _, name := range destination.AllowedHeaders {
		allowed[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	seen := make(map[string]struct{}, len(headers))
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
		canonical := strings.ToLower(strings.TrimSpace(name))
		if canonical == "" || strings.ContainsAny(canonical, " \t\r\n:") ||
			strings.ContainsAny(headers[name], "\r\n") {
			return errors.New("stored delivery contains an invalid Header")
		}
		if _, duplicate := seen[canonical]; duplicate {
			return errors.New("stored delivery contains a case-insensitive duplicate Header")
		}
		seen[canonical] = struct{}{}
		if _, denied := protected[canonical]; denied {
			return errors.New("stored delivery contains a protected Header")
		}
		if _, ok := allowed[canonical]; !ok {
			return errors.New("stored delivery contains a non-allowlisted Header")
		}
	}
	return nil
}

func validInjectedHeader(name string) bool {
	canonical := strings.ToLower(strings.TrimSpace(name))
	if canonical == "" || strings.ContainsAny(canonical, " \t\r\n:") {
		return false
	}
	switch canonical {
	case "host",
		"connection",
		"keep-alive",
		"proxy-authenticate",
		"te",
		"trailer",
		"transfer-encoding",
		"upgrade",
		"cookie",
		"set-cookie",
		"content-length":
		return false
	default:
		return true
	}
}

func containsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(candidate)) {
			return true
		}
	}
	return false
}
