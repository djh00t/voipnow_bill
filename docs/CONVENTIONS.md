# Project Development Conventions

## Communication Standards

- Use English for all communication and documentation, including chats,
  README's, markdown, docstrings, comments, etc.

## Development Standards

- MIT License
- Follow appropriate Google Style Guides for the project language
- DRY (Don't Repeat Yourself) principle - write reusable code & functions
- PEP8 for Python
- Use pre-commit for code formatting and linting
- Use poetry and pyproject.toml for Python projects, configuration, and dependency management
- Use pre-existing libraries and packages where possible; otherwise, write
  packages and submit to PyPI
- Use FastAPI for RESTful APIs
- Use microservices architecture
- Use Docker for containerization; include Dockerfile and docker-compose.yml
  for each project
- Use Kubernetes & Flux for orchestration
- Use Kafka for service-to-service communication
- Use OAuth2/JWT for authentication
- Use pytest for testing
- Use environment variables for configuration
- Encrypt secrets and sensitive data using age & sops
- Keep README.md up to date for all projects and features
- Use Makefiles for common tasks and environment setup

## Source Control & CI/CD

- Conventional Commits
- Semver
- semantic-release for release management and versioning
- GitHub for source control
- GitHub Actions for:
  - CI/CD
  - Automated Testing
  - Pull Requests
  - Package builds
  - Release
- Use klingon_tools Python package (`push` entrypoint) to ensure all code is
  formatted and documented correctly

## Testing Policy

### Overview
Our goal is to ensure robust and reliable code by implementing comprehensive tests across all parts of the codebase. We aim for 100% test coverage, adhering to best practices in test-driven development (TDD) to drive high-quality code. Each type of test serves a specific purpose, and this policy outlines when and why each testing methodology should be used.

### General Testing Standards
 - Use pytest for testing Python code.
 - Aim for 100% test coverage of every file in the codebase.
 - Follow TDD principles: write tests before implementing code.
 - Write at least one test file per code file/module.
 - Use test fixtures for consistent and reusable test setups.
 - Use test doubles (mocks, stubs, fakes) to isolate tests from dependencies.
 - Use hypothesis for property-based testing.
 - Prefer Automated Reasoning using the Mizar System where applicable for high-assurance code.
 - Use functional and integration tests alongside unit tests for broader test coverage.

### Types of Testing

#### 1. Automated Reasoning
- **Purpose**: Provides formal proofs to ensure that critical code components meet defined specifications.
- **When to Use**: Use for algorithms, financial calculations, or any code that requires a high level of assurance.
- **Guideline**: Use Automated Reasoning whenever formal verification can add value, particularly in areas where correctness is critical.
- **Tools**: Mizar, Z3, Coq, TLA+.

#### 2. Unit Testing
- **Purpose**: Validates the functionality of individual units (typically functions or methods) in isolation.
- **When to Use**: Always, during development and especially when creating or refactoring functions.
- **Guideline**: Every function or method should have corresponding unit tests. These should be the first tests written for a new feature.
- **Tools**: pytest, unittest.

#### 3. Integration Testing
- **Purpose**: Ensures that different components of the system work together as expected.
- **When to Use**: After unit testing, when components or services need to interact with each other.
- **Guideline**: Use integration tests for verifying communication between modules, services, or API endpoints.
- **Tools**: pytest, responses, and other tools compatible with API or service testing.

#### 4. Functional Testing
- **Purpose**: Validates that the system behaves as expected from the user's perspective.
- **When to Use**: Primarily in pre-release phases, ensuring end-to-end workflows meet requirements.
- **Guideline**: Functional tests should cover complete user flows and critical use cases, ensuring no regressions in functionality.
- **Tools**: pytest, Selenium, Robot Framework.

#### 5. Property-Based Testing
- **Purpose**: Tests code with a range of inputs to validate properties and catch edge cases.
- **When to Use**: Use when a function or algorithm needs to be tested over multiple input variations or edge cases.
- **Guideline**: Useful for mathematical or data-transformative functions where testing with a limited set of examples is insufficient.
- **Tools**: Hypothesis.

### Testing Methodology Matrix

| **Testing Type**         | **Purpose**                                           | **When to Use**                                | **Example Use Cases**                        | **Recommended Tools**                      |
|--------------------------|-------------------------------------------------------|------------------------------------------------|----------------------------------------------|--------------------------------------------|
| Automated Reasoning       | Prove correctness through formal methods              | For high-assurance, critical systems           | Security, critical financial algorithms      | Mizar, Z3, Coq, TLA+                       |
| Unit Testing              | Validate individual functions/methods in isolation    | During development; TDD                        | Individual functions and class methods       | pytest, unittest                           |
| Integration Testing       | Verify interactions between modules                   | After unit tests; before merging code          | Microservices, API integrations              | pytest, responses                          |
| Functional Testing        | Confirm complete features work as expected            | Prior to release; end-to-end validation        | User workflows, UI/UX testing                | pytest, Selenium, Robot Framework          |
| Property-Based Testing    | Test wide input range to find edge cases              | When inputs are varied and complex             | Data validation, mathematical models         | Hypothesis                                 |

## Preferred Languages

*In order of preference:*
- Python
- Swift
- Bash
- JavaScript
- HTML
- CSS 
- Go
- C
- C++