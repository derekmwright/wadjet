# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | Yes                |

## Reporting a Vulnerability

**Do not open a public issue for security vulnerabilities.**

Please report security vulnerabilities by emailing **security@citc.tech**. You should receive a response within 48 hours.

Include the following in your report:

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact
- Suggested fix (if any)

## Disclosure Policy

- We will acknowledge receipt within 48 hours
- We will confirm the vulnerability and determine its impact within 7 days
- We will release a fix within 30 days of confirmation for critical issues
- We will credit the reporter in the release notes (unless anonymity is requested)

We ask that you do not publicly disclose the vulnerability until a fix has been released.

## Security Features

Wadjet includes built-in security features for production deployments:

- **Authentication**: API keys, JWT, and mTLS
- **Authorization**: Role-based access control (RBAC) and attribute-based access control (ABAC)
- **Cell-level security**: Row-level filtering and column masking policies
- **Audit logging**: Query and access audit trail
- **TLS**: Encrypted transport for pgwire, HTTP, and gRPC

See [docs/security.md](docs/security.md) for configuration details.
