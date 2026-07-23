# ADR 0003: Registered Destinations Define the SSRF Boundary

- **Status:** Accepted
- **Date:** 2026-07-23

## Context

The service can make authenticated outbound requests. If callers control the URL,
redirect target, sensitive headers, or resolved address, the service becomes an
SSRF proxy capable of reaching internal services, cloud metadata endpoints, or
other callers' suppliers.

## Decision

Only privileged configuration may define a destination version. Every version is
immutable and includes its URL, network policy, allowed method and headers,
response rules, timeout, and secret reference. A delivery binds the version current
at acceptance. The secret value is not versioned with the configuration and is
resolved dynamically for each attempt. Callers submit a destination ID and only
fields explicitly allowed by the bound version.

Before every network attempt:

- require an absolute HTTPS URL without userinfo or fragment;
- reject literal and resolved loopback, private, link-local, multicast, unspecified,
  and known metadata addresses;
- disable redirects rather than trusting the redirected host;
- prevent caller overrides of Host, authentication, idempotency, hop-by-hop,
  cookie, and framing headers;
- bind the HTTP transport to the validated resolved address while preserving the
  registered hostname for TLS verification;
- apply destination-specific method, header, timeout, rate, and concurrency rules.

Credentials are resolved from a secret reference at send time. They are absent
from database task payloads, queue messages, logs, metrics, and API responses.
Production deployments must add an egress proxy or firewall policy as an
independent enforcement layer.

The fake HTTPS supplier uses an explicit test-only network policy that permits only
the named test endpoint on the Docker test network. This policy exists solely in
test configuration, cannot be selected by production configuration, and is not a
general switch for allowing private networks.

## Consequences

- Arbitrary caller-provided URLs and redirects are not supported.
- DNS validation must be coupled to the actual socket dial to resist DNS rebinding.
- Destinations that resolve to both public and forbidden addresses are rejected
  conservatively.
- Internal/private suppliers require a separate reviewed network policy rather
  than an exception hidden in application input.
- The Docker test-network allowance cannot authorize any production connection.
- Network policy failure is permanent until the destination configuration changes;
  no forbidden connection is attempted.

## Rejected alternatives

- **Trust internal callers:** a compromised caller would inherit the service's
  network reach and credentials.
- **Validate only at registration:** DNS answers can change between registration
  and delivery.
- **Validate then use the default HTTP dialer:** a second DNS lookup can return a
  different, forbidden address.
- **Follow redirects and revalidate:** it expands the state space and is not needed
  for the MVP.
