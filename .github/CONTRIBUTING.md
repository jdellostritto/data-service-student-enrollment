# Contributing

Thank you for contributing to the Student Enrollment Data Service!

## Getting Started

1. Fork the repository
2. Clone your fork
3. Create a feature branch: `git checkout -b feat/your-feature`
4. See [QUICKSTART.md](../QUICKSTART.md) for development setup

## Development Workflow

1. Run tests before submitting: `make test`
2. Check code quality: `make sonar`
3. Ensure coverage is maintained (target: 32%+)
4. Push to your fork and create a Pull Request

## Commit Guidelines

Use conventional commits with co-author support:

```
feat(processor): add validation for null records

Implement deserialization checks to catch malformed
payload entries and route them to DLQ topics.

Co-authored-by: Jane Smith <jane@example.com>
```

Set up the commit template:
```bash
git config commit.template .github/commit-msg-template
```

Type conventions:
- `feat` - New feature
- `fix` - Bug fix
- `docs` - Documentation changes
- `test` - Test additions/changes
- `refactor` - Code refactoring
- `perf` - Performance improvements
- `chore` - Build/dependency updates

## Pull Request Process

1. Update README.md if needed
2. Ensure CI passes (tests + code quality)
3. Request review from maintainers
4. Address feedback and rebase if needed

## Code Quality Standards

- All tests must pass: `gradle test integrationTest`
- Code coverage must not decrease
- SonarQube quality gates must pass
- Integration tests required for processor changes

## Questions?

Open an issue or start a discussion in the repository.
