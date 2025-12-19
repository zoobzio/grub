# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| latest  | :white_check_mark: | Active development |
| < latest | :x: | Security fixes only for critical issues |

## Reporting a Vulnerability

We take the security of grub seriously. If you have discovered a security vulnerability in this project, please report it responsibly.

### How to Report

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security tab](https://github.com/zoobzio/grub/security) of this repository
   - Click "Report a vulnerability"
   - Fill out the form with details about the vulnerability

2. **Email**
   - Send details to the repository maintainer through GitHub profile contact information
   - Use PGP encryption if possible for sensitive details

### What to Include

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- **Type of issue** (e.g., data leakage, injection, race condition, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration required** to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit the issue
- **Your name and affiliation** (optional)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: Within 7 days, we will provide an initial assessment of the report
- **Resolution Timeline**: We aim to resolve critical issues within 30 days
- **Disclosure**: We will coordinate with you on the disclosure timeline

### Preferred Languages

We prefer all communications to be in English.

## Security Best Practices

When using grub in your applications, we recommend:

1. **Keep Dependencies Updated**
   ```bash
   go get -u github.com/zoobzio/grub
   ```

2. **Resource Management**
   - Call `Close()` on providers before application exit
   - Use `Health()` checks for monitoring
   - Handle context cancellation properly

3. **Error Handling**
   - Check all errors returned by grub operations
   - Use `errors.Is()` for sentinel error comparison
   - Log errors appropriately

4. **Input Validation**
   - Validate keys before storing
   - Sanitize user input before using as keys
   - Be cautious with user-controlled data in keys

5. **Provider Credentials**
   - Never store credentials in code
   - Use environment variables or secret managers
   - Rotate credentials regularly

6. **Data Sensitivity**
   - Don't store secrets directly in grub
   - Consider encryption for sensitive data
   - Be aware of data at rest and in transit

## Security Features

grub includes several built-in security considerations:

- **Type Safety**: Generic Service[T] provides compile-time type checking
- **Error Handling**: Sentinel errors for predictable error handling
- **Context Support**: All operations accept context for cancellation and timeouts
- **Lifecycle Management**: Proper resource cleanup via Lifecycle interface
- **No Global State**: Providers are explicitly passed, avoiding shared mutable state

## Automated Security Scanning

This project uses:

- **CodeQL**: GitHub's semantic code analysis for security vulnerabilities
- **gosec**: Go-specific security scanner
- **golangci-lint**: Static analysis including security linters
- **Codecov**: Coverage tracking to ensure security-critical code is tested

## Vulnerability Disclosure Policy

- Security vulnerabilities will be disclosed via GitHub Security Advisories
- We follow a 90-day disclosure timeline for non-critical issues
- Critical vulnerabilities may be disclosed sooner after patches are available
- We will credit reporters who follow responsible disclosure practices

## Credits

We thank the following individuals for responsibly disclosing security issues:

_This list is currently empty. Be the first to help improve our security!_

---

**Last Updated**: 2024-12-17
