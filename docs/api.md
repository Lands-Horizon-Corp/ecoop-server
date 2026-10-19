# Horizon API Security and Hardening Guide

## 1. Purpose

This document defines the security controls the API must have, and the risks to avoid, for an API that handles cooperative/member information, account balances, credit data, and financial ledgers.

**Status: the transport layer is implemented and tested; authentication and authorization are not.** The API framework is **Hertz** (`github.com/cloudwego/hertz`), and [internal/pkg/api](../internal/pkg/api/api.go) builds the server, applies the security middleware to every route in a fixed order, and runs under fx ([api.module.go](../internal/pkg/api/api.module.go), wired in [main.go](../main.go)). [Section 3.1](#31-implementation-status) says which control is built and where it is tested. Nothing here authenticates a caller or checks a tenant: those belong to route middleware (section 5), and no route is registered yet. An earlier version of this document described an Echo-based bootstrap that is not part of this repository; the findings in section 4 are the mistakes that bootstrap made, and each one is covered by a regression test.

The API must include request tracing, host filtering, HTTPS redirection in secured mode, CORS, request-size controls, rate limiting, suspicious-path blocking, security headers, cookie hardening, structured request logging, and health endpoints. Metrics, traces and logs go through the shared observer described in [logs.md](logs.md).

**Important:** this README is not a security certification. A control being listed does not prove that it is effective in production. Complete the priority fixes and verification checklist below before relying on these controls for sensitive financial operations.

## 2. Security goals

The security program should protect:

- **Confidentiality:** member information, identity documents, account details, credit information, credentials, and transaction records must only be accessible to authorized users and services.
- **Integrity:** balances and posted ledger entries must not be changed, duplicated, reordered, or deleted without a controlled and auditable business process.
- **Availability:** the API, database, cache, and storage must remain usable during malformed requests, scans, traffic spikes, and dependency failures.
- **Tenant isolation:** users, staff, and service accounts must only access cooperatives, branches, members, accounts, and records within their assigned permissions.
- **Accountability:** sensitive reads and every financial state transition must be attributable to an authenticated actor or service, with sufficient records to investigate disputes and incidents.
- **Recoverability:** encrypted, tested backups and documented recovery procedures must allow restoration after accidental deletion, corruption, ransomware, or compromise.

## 3. Controls the API must implement

The table lists each control, the behavior to build into the Hertz API, and the limitation to design around. The behavior column was taken from an earlier Echo bootstrap and is a requirement, not a description of code in this repository. Each item must be covered by tests in the deployed configuration.

| Control | Required behavior | Important limitation |
|---|---|---|
| HTTPS redirect | Secured mode redirects requests to HTTPS, except the live and readiness health paths. | Correct behavior depends on TLS termination and trusted proxy configuration. Verify the redirect at the public edge and confirm health checks are intentionally reachable without redirects. |
| Host allowlist | Requests are checked against host values derived from the configured origins; disallowed hosts are logged and rejected. | Host allowlisting is not authentication. The origin list is also reused to construct host values and includes duplicate entries, trailing-slash variants, development/staging domains, and some HTTP origins. |
| HTTP method allowlist | Only GET, POST, PUT, PATCH, DELETE, HEAD, and OPTIONS are accepted by the general middleware. | Validate that method restrictions also match individual route intent. Unneeded methods should not be enabled on a route. |
| Request body limit | A middleware checks a declared content length and wraps the request body with a limited reader. | The current limited-reader approach can truncate an oversized or streaming request rather than consistently returning a clear 413 response. See Priority 0 fixes. |
| Rate limiting | A per-client-IP limiter at a configured rate (the earlier bootstrap used 10 requests per second, a burst of 30, and a three-minute store expiry). Hertz has no built-in limiter, so use a middleware or the shared cache. | An in-memory limiter is per process and is not shared across replicas. IP-based limiting alone is insufficient for authenticated financial operations. |
| Suspicious-path blocking | Suspicious paths are checked using a helper and cached in Redis for five minutes. Attempts are recorded in a sorted set with old entries periodically removed. | Background goroutines are started per suspicious request. Add bounded work queues, limits, and monitoring to prevent abuse of this tracking path. |
| CORS | A configured list of origins, methods, headers, exposed headers, credential support, and a one-hour preflight cache is supplied. | CORS is a browser policy, not an access-control boundary. Production, staging, development, and plain-HTTP origins must not share one broad allowlist. |
| Security headers | The intended secured profile includes content-type sniffing protection, frame restrictions, a referrer policy, CSP, HSTS, Permissions Policy, and cross-origin policies. | Headers are currently assigned after the next handler has run. Once a response is committed, changes may not reach the client. Fix this before relying on the headers. |
| Cookie hardening | In secured mode, the middleware attempts to add HttpOnly, Secure, and SameSite=None; development mode attempts to add HttpOnly and SameSite=Lax. | String replacement on a single Set-Cookie value is fragile and may miss cookies, alter existing attributes, or fail when Path is absent or differently formatted. Set flags when each cookie is created. |
| Panic recovery | Recovery middleware and an outer tracing/recovery wrapper attempt to prevent panics from crashing request handling. | Panic details and request body content can leak through logs or streaming responses. Never return raw panic/error details to clients. |
| Request tracing | Every request is one `Operation` of the shared observer (component `api`), with the route template as its name; the streaming path records update events on the active span. | Ensure spans and trace attributes never contain passwords, session tokens, full payloads, account numbers, or other sensitive information. |
| Request logging | Structured logs include request method, URI, status, latency, IP, user agent, host, and content length; client and server errors are logged. | URLs can contain sensitive query parameters. Log fields must be sanitized and logs must not contain credentials or full financial payloads. Multiple request-logging middleware instances may also produce duplicate events. |
| Health checks | Liveness and readiness endpoints are registered. Readiness checks cache, main database, admin database, and storage with a five-second context timeout. | Keep responses minimal and ensure health endpoints do not expose connection details, credentials, internal addresses, or stack traces. Decide whether public access is needed. |
| Profiling endpoints | Pprof routes are registered when secured mode is disabled. | “Not secured” does not necessarily mean local development. Ensure these routes cannot be exposed to the public internet, and remove or strongly protect them in shared environments. |
| Route metadata | A route inventory endpoint exposes grouped, non-private route metadata and generated request/response interface descriptions. | Treat route inventories as internal documentation; protect them or publish only an intentionally public API specification. |
| Gzip | Responses are compressed, with exclusions for selected binary media and metrics paths. | Compression may reveal information when attacker-controlled and secret content are compressed together. Review sensitive response patterns and consider disabling compression on responses containing secrets. |

### 3.1 Implementation status

Every row is exercised through the whole middleware chain by the tests in [tests/packages/api](../tests/packages/api), over a real socket where the behavior needs one (body framing, timeouts, streams, shutdown). Configuration is read from the environment by `api.FromEnv` and refused at startup when unsafe (`API_*` in [.env.example](../.env.example)).

| Control | State | How it is built |
|---|---|---|
| Request id | built | `X-Request-Id` is generated per request; a client value is never trusted. It is in every error body. |
| Security headers | built | Set before the handler runs, so error, panic, redirect and stream responses carry them. A deliberately minimal CSP (`default-src 'none'`) fits a JSON API; `RandomNonce()` exists for a route that serves HTML. Expect-CT is not sent. |
| Panic recovery | built | A panic is a generic 500 with the request id. The log has the panic type, never the body or the message. |
| HTTPS redirect | built | Secured only. A plain `POST` is refused with 403, not redirected, so a body is never replayed. `X-Forwarded-Proto` is believed only from a trusted proxy. `/healthz` and `/readyz` are exempt. |
| Host allowlist | built | Separate from the CORS origins; defaults to their hosts only as a convenience. Probes are exempt, because a pod is probed by its own address. |
| Method allowlist | built | Configurable; unknown methods answer 405. |
| Body limit | built | A declared length is refused with 413 before the handler, and the server's own limit covers chunked and lying bodies. |
| Rate limiting | built | Per client address, bounded in memory. With a cache it also keeps a shared one-second window, so replicas agree. `api.Keyed` limits by another key (login, OTP, a tenant). The client address is the peer unless it is a trusted proxy. |
| Suspicious paths | built | Scanner paths answer 404 and are counted; no goroutine is started per request. |
| CORS | built | Exact origins only, never a wildcard, `Vary: Origin`, preflights answered before they cost a rate-limit token. |
| Timeouts and limits | built | Read, write, idle and header limits are set on the server and tested against a slow client. |
| Probes | built | `/healthz` checks nothing; `/readyz` runs the checks given to `WithReadiness` and returns names, never error text. |
| Pprof and route inventory | built | Never when Secured (`Validate` refuses pprof, and the inventory answers 404), and loopback-only otherwise. `Private` routes are never listed. |
| Event streams | built | `RegisterSSR`: NDJSON, the same middleware as any route, at most `MaxStreams` open (503 beyond), each ended at `StreamTimeout`, on shutdown, or when the client leaves. A failure is one generic event with the request id. |
| Graceful shutdown | built | `Stop` refuses new requests, ends open streams, waits for the requests in flight up to `ShutdownTimeout`, and does not wait for idle keep-alive connections. |
| Tracing and metrics | built | `Observe`, see [logs.md](logs.md). |
| Cookie hardening | not in this package | Cookies are created with explicit attributes by the auth service ([auth.go](../internal/pkg/auth/auth.go)), not rewritten afterwards. |
| Gzip | not built | Left out on purpose: a response that mixes a secret with attacker-controlled text should not be compressed. Compress at the gateway for routes that are safe. |
| Authentication, authorization, tenant checks, CSRF | not built | Route middleware. See section 5. |
| Idempotency, ledger rules, audit trail | not built | Business logic. See section 6. |

How the API (and every other service) is added to the application, and how its settings are read, is in [modules.md](modules.md).

Where the code lives, in [internal/pkg/api](../internal/pkg/api): `api.go` is the lifecycle (Start, Run, Stop), `api.mount.go` the middleware chain and the loopback-only routes, `api.middleware.go` the middleware themselves, `api.routes.go` the route table and registration, `api.stream.go` the event streams, `api.ratelimit.go` the shared window and `Keyed`, `api.config.go` the configuration, `api.health.go` the probes. What is not specific to the API lives in [internal/utils](../internal/utils): `Limiter` (the bounded per-key token bucket), `Gate` and `Inflight` (what `Stop` waits on), and the text helpers `Truncate`, `StripControl`, `SplitList` and `ASCIILower`, each tested under [tests/utils](../tests/utils).

Two things the tests showed that are worth knowing. A probe must never be host-filtered, or a healthy pod is restarted by the orchestrator. And a stream must write its last byte before its handler returns, because Hertz reuses the request context afterwards.

## 4. Priority fixes before production use

These are findings from the supplied implementation, not assumptions about other parts of the application. Resolve them and add regression tests.

### Priority 0 — Fix before handling sensitive data

1. **Apply security headers before the response is written.** The current security-header middleware calls the handler first and sets headers afterward. Handlers such as JSON responses and the NDJSON stream can commit or flush the response before those headers are set. Set headers before invoking the handler, and test ordinary, error, panic, and streaming responses by inspecting the actual HTTP response.

2. **Stop logging raw request payloads during panic recovery.** The tracing/recovery middleware reads request bodies and adds the body to panic logs. A payload can contain passwords, bearer tokens, personal data, credit data, or ledger instructions. Do not log complete request bodies. Record only sanitized metadata and a correlation ID; use an explicitly reviewed, redacted diagnostic mechanism only where necessary.

3. **Separate origins by environment.** The configured list combines production, staging, development, localhost, service hosts, duplicated entries, and plain-HTTP sites. A compromised lower-trust environment must not be allowed to make credentialed browser requests to production. Give each environment its own explicit HTTPS origin allowlist. Remove origins that are not required, remove plain HTTP from production, and do not use trailing-slash variants as origins.

4. **Enforce request-body limits without silent truncation.** The current limited reader can make an oversized body appear truncated to downstream code instead of reliably producing a 413 response. Use a request-size mechanism designed to report limit violations, handle unknown/streamed content lengths, and test bodies just below, exactly at, and above the limit. Ensure handlers do not process partial requests after a size violation.

5. **Fix client-visible error disclosure in event streams.** The streaming route currently sends the raw panic description or `err.Error()` to the client. After the response has started, return only a generic failure status/message and a correlation ID. Log the detailed failure on the server after redaction. Protect stream routes with the same authentication, authorization, tenant checks, rate limits, and timeouts as other sensitive endpoints.

### Priority 1 — Resolve during the next security hardening cycle

6. **Replace fragile Set-Cookie rewriting.** Create cookies with explicit attributes at the source. Review every cookie independently, including multiple Set-Cookie headers and cookies without a Path attribute. Use Secure in HTTPS production, HttpOnly for session cookies, and the narrowest SameSite policy compatible with the architecture. SameSite=None requires Secure and should only be used when cross-site cookie behavior is truly required.

7. **Use separate CORS and host configuration.** CORS origins and accepted HTTP Host values are different concepts. Keep separate, normalized, environment-specific allowlists. Validate the effective host and forwarded host only through a correctly configured trusted-proxy boundary. Do not trust arbitrary forwarded headers from the public internet. Remove internal service names and unused client domains from public allowlists.

8. **Make rate limiting distributed and identity-aware.** An in-memory limiter is local to a process; traffic can bypass the intended aggregate limit by reaching multiple replicas. Add gateway-level limits and a shared limiter where appropriate. Use separate quotas for login, password reset, OTP, search, exports, report generation, streaming, and money-moving endpoints. Combine IP limits with authenticated user, tenant, service identity, and business-flow limits.

9. **Protect profiling and operational endpoints.** Make pprof available only in local development or behind a private network and strong authentication. Restrict route inventory, metrics, diagnostic, and administrative endpoints. Test deployment configuration so a mistaken secured-mode flag cannot expose debugging facilities.

10. **Replace the fixed CSP nonce placeholder.** The configured CSP contains a literal nonce placeholder rather than a per-response unpredictable nonce. A nonce is only useful when generated securely for each response and applied consistently to the permitted script/style elements. Otherwise, remove the nonfunctional nonce and use a policy compatible with the frontend. Add a real reporting endpoint or remove unused reporting directives; configure Reporting-Endpoints if report-to is retained.

11. **Remove obsolete or unverified headers.** Expect-CT is obsolete in modern browsers and should not be treated as a meaningful control. Keep a documented, tested header policy. Validate the effects of COEP, COOP, and CORP against required frontend, storage, identity-provider, and document-preview integrations instead of assuming that stricter values are universally compatible.

12. **Use bounded suspicious-request processing.** Avoid spawning an unbounded goroutine for every suspicious request. Use a bounded worker queue or controlled synchronous operation, apply Redis timeouts, cap path lengths and cardinality, and define eviction/TTL policies for tracking keys. Apply independent request and concurrency limits to prevent abuse of the detection mechanism itself.

13. **Configure server-level timeouts and limits.** Explicitly set appropriate read-header, read, write, idle, and header-size limits on the HTTP server, considering legitimate long-lived streams. Add connection and concurrency limits at the reverse proxy or gateway. Request body limits alone do not prevent slow-client and connection-exhaustion attacks.

14. **Review the missing Content-Length rule.** Requiring a Content-Length header for POST, PUT, and PATCH rejects some legitimate streamed/chunked requests and is not a substitute for correct body-size enforcement. Decide whether these request formats are allowed, validate transfer behavior at the proxy, and use consistent HTTP framing between the edge and the Go server.

15. **Simplify and validate middleware ordering.** Confirm that all middleware is attached to every route regardless of the route registration sequence. Test that recovery, host checks, tracing, CORS, request-size limits, rate limiting, and security headers run on the routes that need them. Do not rely on comments about middleware ordering in place of tests.

## 5. Authentication and authorization requirements

The supplied code does not itself show a complete authentication or authorization system. Those protections must exist in route middleware and business logic; CORS, host filtering, hidden documentation, and rate limiting do not replace them.

- Require authentication for all non-public routes. Use a maintained identity provider or well-reviewed authentication implementation. Validate token signature, issuer, audience, expiry, not-before time, and permitted algorithms. Reject malformed, expired, revoked, or incorrectly scoped credentials.
- Keep access tokens short-lived. Rotate refresh tokens, detect refresh-token reuse, revoke sessions after credential changes or suspected compromise, and provide a secure logout/revocation process.
- Require MFA for privileged staff and administrators. Use step-up authentication for high-risk actions such as changing beneficiaries, posting large transactions, exporting sensitive data, changing permissions, or altering security settings.
- Enforce authorization at the function, record, and field level. For every request, verify the actor may perform that exact action against that exact cooperative, branch, member, account, ledger, and record. Never trust a tenant ID, member ID, account ID, role, balance, or approval flag supplied by the client without verifying it against the authenticated principal and server-side state.
- Apply deny-by-default permissions and least privilege. Separate member, teller, accountant, approver, auditor, support, system integration, and administrator roles. Do not use one broad service account for all workloads.
- Prevent mass assignment. Bind request fields to explicit input models and allow only fields intended for the requested action. Do not accept client-controlled status, ownership, permissions, posting state, or computed balances.
- Return generic authentication/authorization errors. Avoid confirming whether a sensitive member or account exists to callers who are not authorized to know.
- Test for broken object-level authorization by swapping identifiers across members, branches, cooperatives, and tenants on every read and write endpoint.

## 6. Ledger and financial transaction safeguards

These controls are essential if the API posts or reports financial balances. Middleware alone cannot guarantee ledger integrity.

### 6.1 Ledger invariants

- Use an append-only journal for posted financial entries. Do not edit or delete a posted transaction to correct it; record a controlled reversal or adjustment linked to the original transaction.
- Use double-entry accounting where applicable. Validate that debits and credits balance for each transaction and currency before committing it.
- Store monetary values using a suitable exact decimal or integer-minor-unit representation. Do not use binary floating-point arithmetic for balances, fees, rates, or postings. Define currency precision and rounding rules explicitly.
- Calculate amounts, fees, interest, penalties, and balances on the server using trusted configuration. Treat client-supplied calculations as display-only and untrusted.
- Perform all related journal entries, balance updates, idempotency records, and posting state changes atomically in a database transaction. Roll back the entire operation if any invariant fails.
- Use appropriate concurrency controls, row locks or optimistic version checks, and database constraints to prevent double spending, stale-balance updates, duplicated postings, and races between concurrent requests.
- Define and test invariants for account state, overdraft/credit limits, available balance, holds, reversal status, currency, and transaction sequence. Do not rely only on application-layer checks when a database constraint can enforce an invariant safely.
- Separate draft, pending, approved, posted, reversed, failed, and settled states. Define allowed state transitions explicitly; never allow a client to set an arbitrary status.

### 6.2 Idempotency and replay protection

- Require idempotency keys for money-moving and other retry-prone operations. Scope the key to the tenant, actor or client, operation, and relevant business context.
- Enforce uniqueness in persistent storage. Store the request fingerprint, processing state, and final result with the transaction so concurrent retries cannot create duplicate postings.
- Return the original result for a valid retry using the same key and same request. Reject reuse of the same key with a different payload.
- Define key retention that covers realistic retry windows and recovery scenarios. Do not rely on an in-memory cache as the only duplicate-payment protection.
- For partner callbacks and webhooks, verify the signature over the exact raw request bytes, validate the timestamp and replay window, and persist a unique event identifier before applying business effects. Use mutual TLS or private connectivity for high-trust integrations where practical.

### 6.3 Transaction approvals and auditability

- Use maker-checker or dual approval for high-risk amounts, manual adjustments, reversals, privileged transfers, and changes to limits or beneficiary details. The initiator must not approve their own restricted transaction.
- Bind approval to the actual transaction details: account, beneficiary, amount, currency, fees, and purpose. If any material detail changes, invalidate prior approval and require approval again.
- Maintain a durable, append-only business audit trail for posting, reversal, approval, rejected attempts, permission changes, account status changes, exports, and administrative actions.
- Record actor/service identity, tenant, action, target record, timestamp, outcome, reason/reference, and correlation/transaction ID. Avoid putting credentials or unnecessary personal information into the audit event.
- Restrict who can alter or delete audit data. Forward security-critical audit events to a separately controlled, access-restricted, tamper-evident log store.
- Reconcile ledger journals against balances and external settlement/bank records on a defined schedule. Alert on imbalance, duplicate postings, unexpected reversals, sequence gaps, out-of-policy adjustments, or delayed settlement.

### 6.4 Database integrity and tenant isolation

- Use separate database roles for migrations, normal API traffic, reporting, background workers, and administration. The runtime API role should not own tables, create roles, change schema, or have unrestricted administrative privileges.
- Grant the smallest set of table, schema, and column permissions needed. Keep superuser and database-owner credentials out of application runtime configuration.
- Consider PostgreSQL Row-Level Security as defense in depth for tenant data. Design policies carefully, test both reads and writes, and remember that superusers, roles with bypass privileges, and normally table owners can bypass policies. Application authorization remains mandatory.
- If using connection pooling, verify tenant/session context cannot leak from one request to another through reused connections. Set and clear request-scoped database context safely within each transaction.
- Use parameterized queries for all values. Never build SQL from untrusted input. Dynamic table names, sort fields, or identifiers must come from a strict server-side allowlist.
- Use database constraints for unique transaction references, idempotency keys, currency validity, foreign-key relationships, and other critical invariants.
- Restrict network access to PostgreSQL, Redis, object storage, and admin services to required workloads only. Require encrypted connections where supported and verify certificates when crossing untrusted networks.

## 7. Input validation and API behavior

- Require the correct Content-Type for each endpoint, validate Accept where needed, and reject malformed, oversized, unexpected, or unsupported payloads.
- Validate all path parameters, query parameters, headers, and body fields against explicit constraints, including type, length, range, format, and allowed values.
- Reject unknown fields for security-sensitive request types unless forward compatibility requires them and the behavior has been reviewed. Prevent duplicate or conflicting representations of critical fields.
- Use pagination with enforced maximum page sizes. Bound report ranges, export size, batch length, filter complexity, sort options, and query duration.
- Put explicit limits on expensive report generation, search, PDF creation, file uploads, and streaming operations. Use asynchronous jobs for large exports and give download links limited lifetime and narrow access.
- Avoid revealing internal stack traces, SQL errors, filesystem paths, secrets, hostnames, or implementation details in API error responses. Return stable public error codes and a correlation ID.
- Validate and sanitize filenames and file metadata; verify actual file content rather than trusting extensions or client MIME types. Store uploads outside executable paths and scan files where applicable.
- Protect any feature that fetches a URL on behalf of a caller against server-side request forgery. Prefer allowlisted destinations and block access to private, loopback, link-local, and cloud metadata addresses after DNS resolution, including redirect/rebinding cases.
- For cross-origin browser access, use narrowly scoped CORS policies and protect cookie-authenticated state-changing operations against CSRF. A permissive CORS configuration is not a CSRF defense.

## 8. Transport, browser, and response protection

- Require modern TLS at the public edge and between services where the network trust boundary warrants it. Disable obsolete protocols and weak cipher suites at the TLS terminator; automate certificate renewal and alert before expiry.
- Enable HSTS only for domains that are fully HTTPS-capable. Confirm subdomains support HTTPS before enabling includeSubDomains. Preload requires separate submission and operational commitment; the header alone does not add a domain to the preload list.
- Configure a restrictive, tested Content Security Policy for browser-facing content. Generate nonces per response when nonces are used, and make sure the exact nonce is applied to approved elements. Keep inline script exceptions out unless specifically justified.
- Apply security headers before sending response bytes and test them on redirects, errors, JSON, downloads, and event streams.
- Avoid compressing responses that combine secrets with attacker-controlled text where compression side channels could be relevant.
- Configure proxy trust explicitly. Trust forwarded protocol and client IP headers only from known reverse proxies. Verify that the public edge strips client-supplied forwarding headers before setting trusted ones.

## 9. Secrets, keys, and encryption

- Store database credentials, signing keys, API tokens, webhook secrets, and encryption keys in a managed secret store or the deployment platform's protected secret facility. Never commit production secrets to source control or write them to logs.
- Use separate secrets per environment and workload. Avoid shared credentials across development, staging, and production. Give each secret a named owner, purpose, rotation procedure, and access policy.
- Rotate credentials after suspected exposure, personnel changes where warranted, and on a defined lifecycle. Make revocation and emergency rotation testable.
- Use modern, authenticated encryption for sensitive data at rest and managed encryption keys where available. Define which fields need application-level encryption in addition to database/storage encryption.
- Protect key-encryption keys separately from encrypted data. Restrict decryption permissions, audit key use, and keep recovery procedures for lost or compromised keys.
- Encrypt backups and exports; apply access controls and expiry to temporary files, report artifacts, and signed URLs. Do not assume deleting a database record also deletes historical backups or generated files.
- Hash passwords with a suitable password hashing algorithm and parameters if password authentication is managed by this service. Do not encrypt passwords for later recovery.

## 10. Rate limits, resilience, and dependency protection

- Establish limits at the internet-facing gateway and within the service. Use shared enforcement when the application scales horizontally.
- Apply stricter limits to login, OTP issuance and verification, password resets, search, bulk endpoints, ledger postings, balance checks, report generation, and exports. Set quotas by identity and business context as well as IP.
- Add limits for concurrent requests, in-flight report jobs, stream connections, database work, and queue depth. Bound all queues and worker pools.
- Use context deadlines and cancellation for database, Redis, storage, HTTP client, and report-generation operations. Configure explicit timeouts on outbound clients and avoid unbounded retries.
- Use circuit breakers and bounded retries with backoff for unstable dependencies. Never automatically retry a money-moving operation unless it is made safe by durable idempotency.
- Define clear behavior if the cache or rate-limit store is unavailable. For sensitive actions, choose a deliberate fail-closed or gateway-enforced fallback rather than accidentally disabling protection.
- Avoid creating one background goroutine per attacker-controlled request. Bound background security work and monitor queue depth, dropped events, and dependency errors.
- Gracefully shut down the HTTP server and workers, stop accepting new work, and allow in-flight financial transactions to complete or roll back safely.

## 11. Logging, tracing, monitoring, and incident response

### Logging rules

- Do not log passwords, bearer tokens, cookies, session IDs, API keys, secret headers, database credentials, full identity documents, complete ledger payloads, or raw payment instructions.
- Avoid logging query strings by default because they may contain tokens or personal data. Record a normalized route pattern instead of arbitrary path values where possible.
- Sanitize newline/control characters and untrusted values before logging to prevent log injection. Limit lengths of user-controlled log attributes.
- Generate or validate a request/correlation ID at the trusted edge. Do not blindly trust a client-supplied ID as a security identity. Propagate correlation IDs without including secrets.
- Restrict access to logs and traces, encrypt their transport and storage, define retention periods, monitor for missing telemetry, and make security-critical events tamper-evident.
- Keep operational logs separate from the financial audit ledger. A log message is not proof that a transaction committed; the database transaction and business audit trail are authoritative.

### Alerts to define

Alert on repeated authentication failures, account lockouts, MFA failures, rate-limit spikes, suspicious-path scans, rejected hosts/origins, unusual exports, privilege changes, repeated authorization denials, unusual administrator activity, multiple idempotency conflicts, ledger imbalance, unexpected reversals, database permission errors, secrets access anomalies, logging/telemetry loss, and backup failures.

### Incident response

Maintain a written procedure with owners and escalation contacts for credential compromise, unauthorized ledger activity, data exposure, service denial, and suspected database tampering. Preserve evidence, revoke or rotate affected credentials, disable impacted integrations when necessary, assess affected tenants and transactions, reconcile ledger state, notify the proper internal/legal/regulatory stakeholders where required, and document lessons learned. Exercise the process instead of relying on an untested document.

## 12. Deployment and infrastructure controls

- Keep production, staging, development, and local configurations isolated. Production must fail startup when required security configuration is missing or invalid.
- Place the API behind a managed gateway or reverse proxy with TLS, DDoS protections, request and connection limits, and a clear trusted-proxy policy. Use a WAF as defense in depth, not as a substitute for application validation and authorization.
- Keep database, cache, object storage, admin APIs, and observability backends on private networks where possible. Do not expose internal services directly to the internet.
- Apply least-privilege identities to deployments, CI/CD jobs, workers, and cloud resources. Protect deployment approvals and production secrets; prevent forked or untrusted build jobs from accessing them.
- Pin and update dependencies. Run dependency vulnerability scanning, static analysis, secret scanning, container/image scanning, and infrastructure configuration checks in CI/CD. Review and remediate findings by severity.
- Build immutable releases from reviewed commits. Require code review and tests for security-sensitive changes, database migrations, auth policies, and route middleware.
- Maintain encrypted backups and point-in-time recovery where required. Regularly test restoration into an isolated environment and reconcile restored ledger balances and journal invariants.
- Define operational objectives for availability, recovery time, recovery point, retention, and audit evidence with product, compliance, and operations owners.

## 13. Testing and verification plan

Security controls must be tested at the HTTP boundary and against the real deployment topology, not only through unit tests.

### Middleware and HTTP tests

- Confirm the intended security headers are present on successful responses, 4xx/5xx responses, redirects, downloads, and NDJSON/event-stream responses.
- Verify production rejects unapproved hosts, development/staging origins, plain-HTTP origins, malformed host values, and untrusted forwarded headers.
- Verify credentialed CORS responses contain the exact approved origin only, never a wildcard, and that unauthorized origins cannot read protected responses.
- Verify oversized bodies reliably return 413 and no partial operation is performed, including when Content-Length is absent or misleading and when the request is streamed.
- Verify request timeouts, connection limits, rate limits, concurrency limits, and distributed limits behave correctly across multiple replicas.
- Verify cookie flags for every issued cookie and test multiple Set-Cookie headers.
- Verify panic recovery produces generic client responses while internal logs contain no secrets or raw financial payloads.
- Verify pprof, metrics, route inventory, internal health details, and admin functions are not publicly exposed unintentionally.

### Authentication and authorization tests

- Test expired, revoked, wrong-audience, wrong-issuer, malformed, and wrongly scoped tokens.
- Test horizontal and vertical privilege escalation across users, cooperatives, branches, members, accounts, reports, and administrative endpoints.
- Test mass assignment, IDOR/BOLA, field-level authorization, CSRF, refresh-token reuse, MFA bypass, and session revocation.
- Confirm every sensitive endpoint has an explicit authentication and authorization policy, including downloads, streams, background jobs, and exports.

### Financial integrity tests

- Test concurrent duplicate requests and idempotency-key reuse with both identical and different payloads.
- Test concurrent withdrawals/transfers, balance races, retries after timeouts, partial database failures, duplicate webhooks, reversed transactions, and crash recovery.
- Assert debits equal credits per currency and no failed transaction leaves partial ledger state.
- Test approval workflows and verify a material transaction change invalidates previous approval.
- Test audit events for every state transition and confirm application/runtime database credentials cannot rewrite posted ledger history or delete audit records.
- Run reconciliation and backup-restore exercises with representative, anonymized test data.

### Automated security gates

Use static analysis, software-composition analysis, secret scanning, unit/integration tests, API authorization tests, dynamic testing in a controlled environment, and periodic independent penetration testing. Track each finding to an owner, severity, due date, remediation, and retest evidence.

## 14. Recommended implementation order

1. **Immediate:** move security headers before response writes; stop logging raw payloads; stop returning raw event-stream errors; fix body-limit enforcement; separate the production origin list from lower-trust environments.
2. **Before production ledger traffic:** verify route authentication and tenant/object authorization; implement durable idempotency; enforce atomic ledger invariants and transaction audit trails; lock down pprof and internal endpoints; validate proxy trust and TLS.
3. **Next hardening cycle:** distributed and identity-aware rate limits; server timeouts and concurrency caps; correct cookie creation; real CSP nonce/reporting behavior; least-privilege database roles and tenant isolation.
4. **Ongoing:** alerts, reconciliation, dependency updates, key rotation, incident-response exercises, backup restoration, and recurring security testing.

## 15. Production readiness checklist

- [ ] Production, staging, and development origin/host allowlists are independent and minimal.
- [ ] Production has no unneeded plain-HTTP origins or publicly trusted development sites.
- [ ] TLS, trusted-proxy handling, HTTPS redirects, HSTS, and server timeouts are verified at the deployment edge.
- [ ] Security headers are present on actual responses, including streaming and error responses.
- [ ] Oversized body tests return 413 without processing partial input.
- [ ] Logs and traces are free of raw payloads, credentials, tokens, cookies, and unnecessary personal or financial data.
- [ ] Authentication and per-object/per-tenant authorization are enforced for every sensitive route.
- [ ] Administrative actions use strong authentication, MFA, least privilege, and appropriate approval workflows.
- [ ] Ledger posting is atomic, balanced, idempotent, concurrency-safe, and auditable.
- [ ] Corrections use reversal/adjustment flows rather than silent modification of posted entries.
- [ ] Rate limits work across replicas and protect sensitive business flows.
- [ ] Pprof, internal diagnostics, metrics, and route metadata are private or intentionally published.
- [ ] PostgreSQL, Redis, storage, secrets, backups, and telemetry have restricted access and appropriate encryption.
- [ ] Reconciliation, alerting, incident response, and backup restoration have been tested.
- [ ] Security findings have owners, tracked remediation, and retest evidence.

## 16. Reference material

Use these standards as living references when implementing and reviewing the controls above:

- [OWASP API Security Top 10 (2023)](https://owasp.org/www-project-api-security/): API-specific risks including broken object-level authorization, broken authentication, unrestricted resource consumption, security misconfiguration, and unsafe API consumption.
- [OWASP REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html): REST API transport, authentication, CORS, and error-handling guidance.
- [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html): least privilege, deny-by-default, and authorization checks.
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html): sensitive data exclusions, log protection, and monitoring.
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html): secret storage, least privilege, rotation, and lifecycle management.
- [OWASP Transaction Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html): protection of high-risk financial and sensitive operations.
- [OWASP Application Security Verification Standard](https://owasp.org/www-project-application-security-verification-standard/): a structured security verification checklist; the current stable release is ASVS 5.0.0.
- [PostgreSQL Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html): database-level row policies and their limitations.
- [Go net/http documentation](https://pkg.go.dev/net/http): request handling and body-size controls, including the behavior of MaxBytesReader.
- [Hertz documentation](https://www.cloudwego.io/docs/hertz/): the framework the API is built on, including its middleware model.

---

**Ownership:** The API/backend team should own the technical controls. Product, finance/operations, security, and compliance stakeholders should jointly approve ledger invariants, approval thresholds, audit retention, incident response, and production readiness. Update this README whenever the middleware, authentication model, deployment topology, or financial transaction flows change.