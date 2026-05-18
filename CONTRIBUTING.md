# Contributing to real-estate-analytics-pipeline

Thank you for contributing!

## Setup
docs: add CONTRIBUTING.mdgit checkout -b feat/your-feature
make install
make test
```

## Standards
- Formatter: **Black** (120 chars)
- Linter: **flake8**
- Imports: **isort**
- Run `make format && make lint` before committing

## Testing
- Add tests for new features
- Run: `make test-cov`
- Target: >80% coverage

## Commits (Conventional Commits)
```
feat: new feature
fix: bug fix
docs: documentation
chore: maintenance
```

## Pull Request
1. `make test && make lint` must pass
2. Describe changes clearly

By contributing, you agree to the MIT License.
