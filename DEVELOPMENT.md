# Sparkling Water Development Guide

## Overview

**Sparkling Water** integrates [H2O-3](https://github.com/h2oai/h2o-3), a fast scalable machine learning engine, with [Apache Spark](https://spark.apache.org/). This project provides:

- **Core Integration**: Utilities to publish Spark data structures (RDDs, DataFrames, Datasets) as H2O-3's frames and vice versa
- **ML Capabilities**: DSL to use Spark data structures as input for H2O's algorithms
- **Multi-Language Support**: Scala (core), Python (PySparkling), and R (RSparkling) interfaces
- **Building Blocks**: Components to create ML applications utilizing Spark and H2O APIs

**Primary Technologies**:
- **Languages**: Scala 2.11/2.12, Python 3.6-3.11, R
- **Frameworks**: Apache Spark 2.3-3.5, H2O-3
- **Build System**: Gradle 7.6
- **Testing**: ScalaTest, pytest

**Related Documentation**:
- [README](README.rst) - Getting started and user documentation
- [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md) - Community guidelines
- [User Documentation](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/index.html) - Comprehensive user guide
- [FAQ](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/FAQ.html) - Frequently asked questions

---

## Architecture Overview

### High-Level System Architecture

Sparkling Water acts as a **bridge layer** between Apache Spark and H2O-3, enabling seamless data exchange and algorithm execution across both frameworks.

```
┌─────────────────────────────────────────────────────────────────┐
│                    User Application Layer                        │
│  (Scala/Python/R code using Sparkling Water APIs)               │
└────────────────────┬────────────────────────────────────────────┘
                     │
┌────────────────────┼────────────────────────────────────────────┐
│                    ▼                                             │
│              H2OContext (Core Bridge)                            │
│   ┌──────────────────────────────────────────────────┐          │
│   │  • Data Conversion (Spark ↔ H2O)                 │          │
│   │  • Backend Management (Internal/External)         │          │
│   │  • Configuration & Context Initialization         │          │
│   └──────────────────────────────────────────────────┘          │
│                                                                  │
│   ┌─────────────────────┐     ┌─────────────────────┐          │
│   │  Spark ML Wrapper   │     │   Scoring Engine    │          │
│   │  (ml module)        │     │   (scoring module)  │          │
│   └─────────────────────┘     └─────────────────────┘          │
└──────────────────┬─────────────────┬───────────────────────────┘
                   │                 │
         ┌─────────▼─────────┐  ┌───▼──────────┐
         │  Apache Spark     │  │   H2O-3      │
         │  ┌──────────────┐ │  │  ┌─────────┐ │
         │  │ RDD/DataFrame│ │  │  │ H2OFrame│ │
         │  │ Spark ML     │ │  │  │ Algos   │ │
         │  └──────────────┘ │  │  └─────────┘ │
         └───────────────────┘  └──────────────┘
```

### Backend Modes

Sparkling Water supports two deployment backends:

**Internal Backend**:
```
Spark Cluster
├── Driver (H2O client + Spark driver)
└── Executors
    └── Each executor runs H2O node
```

**External Backend**:
```
Spark Cluster              H2O Cluster (separate)
├── Driver ───────────────> H2O Client
└── Executors              └── H2O Nodes
```

### Main Modules and Responsibilities

| Module | Directory | Purpose |
|--------|-----------|---------|
| **core** | `/core` | Core integration layer, H2OContext, data converters, backend implementations |
| **ml** | `/ml` | Spark ML pipeline integration, H2O algorithm wrappers |
| **scoring** | `/scoring` | MOJO model scoring logic, lightweight inference engine |
| **py** | `/py` | PySparkling - Python API and bindings |
| **py-scoring** | `/py-scoring` | Python scoring package for MOJO models |
| **r** | `/r` | RSparkling - R interface |
| **repl** | `/repl` | Spark REPL integration, interactive shell support |
| **examples** | `/examples` | Example applications and use cases |
| **api-generation** | `/api-generation` | Code generation for Python/R APIs |
| **utils** | `/utils` | Shared utilities, logging, configuration helpers |
| **extensions** | `/extensions` | Additional features and extensions |
| **package** | `/package` | Full distribution assembly |
| **scoring-package** | `/scoring-package` | Lightweight scoring-only package |
| **assembly** | `/assembly` | JAR assembly with dependencies |
| **doc** | `/doc` | Sphinx-based documentation |

### Core Domain Logic

- **Data Conversion**: `core/src/main/scala/ai/h2o/sparkling/backend/converters/`
  - Spark RDD ↔ H2O Frame conversions
  - DataFrame schema mapping
  
- **H2OContext**: `core/src/main/scala/ai/h2o/sparkling/H2OContext.scala`
  - Central API for initializing and managing H2O cluster
  - Entry point for all Sparkling Water operations

- **ML Algorithms**: `ml/src/main/scala/ai/h2o/sparkling/ml/algos/`
  - Spark ML compatible H2O algorithm wrappers
  - Pipelines, estimators, transformers

### External Integrations

- **H2O-3**: Core machine learning engine (external dependency)
- **Apache Spark**: Distributed computing framework (required)
- **Cloud Platforms**: AWS EMR, Azure HDInsight, Google Cloud Dataproc, Databricks
- **Kubernetes**: Container orchestration support (`/kubernetes` module)

### Configuration Management

**Configuration Sources**:
1. **Spark Configuration**: Standard Spark properties via `spark.ext.h2o.*`
2. **H2OConf Object**: Programmatic configuration in code
3. **Environment Variables**: `SPARK_HOME`, `MASTER`, `H2O_HOME` (for custom H2O builds)
4. **Gradle Properties**: Build-time configuration in `gradle.properties` and version-specific property files

**Key Configuration Files**:
- `gradle.properties` - Main build configuration, version numbers
- `gradle-spark*.properties` - Spark version-specific settings
- `.scalafmt.conf` - Scala code formatting rules
- `greclipse.properties` - Eclipse formatter configuration

**Secrets Handling**:
- AWS credentials: Via `HADOOP_CONF_DIR` pointing to `core-site.xml`
- Never commit secrets to version control
- Use environment variables for sensitive data in CI/CD

---

## Repository Layout

```
sparkling-water/
├── api-generation/          # Code generators for Python/R APIs
├── assembly/                # Main JAR assembly with all dependencies
├── assembly-extensions/     # Extensions assembly
├── assembly-scoring/        # Scoring-only assembly
├── benchmarks/              # Performance benchmarks
│   └── src/bench/          # JMH benchmarks
├── bin/                     # Executable scripts
│   ├── sparkling-shell     # Scala shell launcher
│   ├── pysparkling         # Python shell launcher
│   └── run-example.sh      # Example runner
├── booklet/                 # Documentation booklet
├── ci/                      # CI/CD configuration
│   ├── Jenkinsfile*        # Jenkins pipeline definitions
│   ├── docker/             # Docker images for testing
│   └── aws/                # AWS deployment scripts
├── core/                    # **Core Sparkling Water library**
│   └── src/
│       ├── main/scala/     # Main source code
│       │   └── ai/h2o/sparkling/
│       │       ├── H2OContext.scala          # Main entry point
│       │       ├── H2OConf.scala             # Configuration
│       │       └── backend/                  # Backend implementations
│       ├── test/scala/     # Unit tests
│       ├── integTest/scala/# Integration tests
│       └── bench/scala/    # Benchmarks
├── dist/                    # Distribution packaging
├── doc/                     # **Sphinx documentation**
│   └── src/site/sphinx/
│       ├── devel/          # Developer docs (build, tests)
│       ├── deployment/     # Deployment guides
│       ├── ml/             # ML algorithm docs
│       └── tutorials/      # User tutorials
├── examples/                # **Example applications**
│   ├── src/main/scala/     # Scala examples
│   └── smalldata/          # Sample datasets
├── extensions/              # Additional features
├── gradle/                  # Gradle build configuration
│   ├── scala.gradle        # Scala build config
│   ├── python.gradle       # Python build config
│   ├── spark.gradle        # Spark integration
│   └── publish.gradle      # Maven publishing
├── kubernetes/              # Kubernetes deployment resources
├── macros/                  # Scala macros
├── ml/                      # **Spark ML integration**
│   └── src/main/scala/ai/h2o/sparkling/ml/
│       ├── algos/          # H2O algorithm wrappers
│       ├── models/         # Model classes
│       └── params/         # Parameter definitions
├── package/                 # Full package assembly
├── py/                      # **PySparkling (Python API)**
│   ├── src/ai/h2o/sparkling/
│   ├── tests/              # Python tests
│   └── examples/           # Python examples
├── py-scoring/              # Python scoring package
├── r/                       # **RSparkling (R API)**
├── r-cran/                  # CRAN package version
├── repl/                    # REPL integration for Flow UI
├── scoring/                 # Scoring engine
├── scoring-package/         # Lightweight scoring package
├── templates/               # Project templates
├── utils/                   # Shared utilities
├── build.gradle             # **Main build file**
├── settings.gradle          # Gradle multi-project settings
├── gradle.properties        # Build properties
├── .scalafmt.conf          # Scala formatting configuration
├── .gitignore              # Git ignore rules
├── LICENSE                  # Apache 2.0 license
├── README.rst              # User readme
└── CODE_OF_CONDUCT.md      # Code of conduct
```

**Key Entry Points**:
- **Scala API**: `core/src/main/scala/ai/h2o/sparkling/H2OContext.scala`
- **Python API**: `py/src/ai/h2o/sparkling/H2OContext.py`
- **Examples**: `examples/src/main/scala/` and `py/examples/`

---

## Getting Started for Development

### Prerequisites

**Required**:
- **Java**: JDK 8 or 11 (depending on Spark version - see `gradle-spark*.properties` for `minSupportedJavaVersion`)
- **Spark**: Download and set `SPARK_HOME` environment variable
- **Git**: For version control
- **Gradle**: Included via wrapper (`./gradlew`)

**Optional** (for specific modules):
- **Python**: 3.6 to 3.11 (for PySparkling development)
  - pip, virtualenv or conda
- **R**: For RSparkling development
- **Docker**: For running tests in containers
- **Kubernetes**: For testing K8s deployments

### Initial Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/h2oai/sparkling-water.git
   cd sparkling-water
   ```

2. **Set Environment Variables**:
   ```bash
   # Required: Point to your Spark installation
   export SPARK_HOME="/path/to/spark"
   
   # For local development
   export MASTER="local[*]"
   
   # Optional: For custom H2O builds
   export H2O_HOME="/path/to/h2o-3"
   ```

3. **Verify Java Version**:
   ```bash
   java -version
   # Should be Java 8 or 11 depending on Spark version
   ```

### Build the Project

**Full Build** (includes all modules):
```bash
./gradlew build
```

**Skip Tests** (faster build):
```bash
./gradlew build -x check
```

**Build Specific Module**:
```bash
./gradlew :sparkling-water-core:build
./gradlew :sparkling-water-ml:build
./gradlew :sparkling-water-py:build
```

**Create Distribution**:
```bash
./gradlew dist
# Output: ./dist/build/dist/
```

### Running Sparkling Water Locally

**Scala Shell**:
```bash
# After building distribution
export SPARK_HOME="/path/to/spark"
export MASTER="local[*]"
./bin/sparkling-shell

# In shell:
scala> import ai.h2o.sparkling._
scala> val hc = H2OContext.getOrCreate()
```

**Python (PySparkling)**:
```bash
./bin/pysparkling

# In Python:
>>> from pysparkling import H2OContext
>>> hc = H2OContext.getOrCreate()
```

**Run Example**:
```bash
./bin/run-example.sh AirlinesWithWeatherDemo
```

### IDE Setup

**IntelliJ IDEA** (Recommended for Scala):
1. Install Scala plugin
2. Import project as Gradle project
3. Set JVM option for tests: `-Dspark.testing=true`
4. Configure Java SDK matching Spark requirements

**VS Code**:
1. Install extensions: Scala (Metals), Python
2. Open project folder
3. Metals will import Gradle project automatically

---

## Running & Writing Tests

### Test Frameworks

- **Scala**: ScalaTest (version depends on Scala version - see `gradle.properties`)
- **Python**: pytest
- **Integration Tests**: Custom framework in `gradle/itest.gradle`
- **Benchmarks**: JMH (Java Microbenchmark Harness)

### Running Tests

**All Unit Tests**:
```bash
./gradlew test
```

**All Tests (including integration tests)**:
```bash
./gradlew check
```

**Unit Tests Only** (skip integration):
```bash
./gradlew build -x integTest
```

**Integration Tests Only**:
```bash
./gradlew integTest
```

**Specific Module Tests**:
```bash
./gradlew :sparkling-water-core:test
./gradlew :sparkling-water-ml:test
./gradlew :sparkling-water-py:test
```

**Single Test Class** (Scala):
```bash
./gradlew :sparkling-water-core:test --tests "*H2OContextTestSuite"
```

**Python Tests**:
```bash
cd py
# Tests run automatically with ./gradlew :sparkling-water-py:test
# Or manually:
pytest tests/
```

### Test Structure

**Scala Tests**:
- **Location**: `{module}/src/test/scala/`
- **Integration Tests**: `{module}/src/integTest/scala/`
- **Benchmarks**: `{module}/src/bench/scala/`
- **Naming**: `*TestSuite.scala` or `*Test.scala`
- **Example**:
  ```scala
  class MyFeatureTestSuite extends FunSuite with Matchers {
    test("my feature works") {
      // test code
    }
  }
  ```

**Python Tests**:
- **Location**: `py/tests/`
- **Naming**: `test_*.py` or `*_test.py`
- **Example**:
  ```python
  def test_my_feature():
      assert True
  ```

### Running Benchmarks

```bash
./gradlew bench
```

Benchmarks for specific module:
```bash
./gradlew :sparkling-water-core:bench
```

### Test Conventions

- Use meaningful test names describing the scenario
- Tests should be independent and repeatable
- Clean up resources (close H2OContext, stop Spark)
- Use test fixtures and helper methods from existing tests
- For tests requiring data, use files in `examples/smalldata/`

---

## Code Style & Conventions

### Languages and Versions

- **Scala**: 2.11 or 2.12 (default depends on Spark version)
- **Java**: JDK 8 or 11
- **Python**: 3.6, 3.7, 3.8, 3.9, 3.10, 3.11
- **R**: Compatible with CRAN requirements

### Code Formatting

**Scala Formatting**:
```bash
# Check formatting
./gradlew spotlessCheck

# Apply formatting
./gradlew spotlessApply
```

**Configuration**: `.scalafmt.conf`
- Max column width: 120
- ScalaDoc style documentation
- Single-line import selectors

**General Formatting**:
```bash
# Format all code (Scala, shell scripts, properties, etc.)
./gradlew spotlessApply
```

### Scala Conventions

**Naming**:
- Classes/Traits: `PascalCase` (e.g., `H2OContext`, `DataFrameConverter`)
- Methods/Variables: `camelCase` (e.g., `asH2OFrame`, `sparkContext`)
- Constants: `camelCase` with uppercase first letter for objects (e.g., `H2OContext`)
- Test suites: `*TestSuite` suffix

**Code Style**:
- Use type inference where clear: `val x = 5` instead of `val x: Int = 5`
- Prefer immutability: Use `val` over `var`
- Use pattern matching over if-else chains
- Write self-documenting code; add comments for complex logic
- Keep methods focused and single-purpose

### Python Conventions

- Follow PEP 8 style guide
- Use type hints for public APIs
- Docstrings for public classes and methods
- Snake_case for functions and variables
- PascalCase for classes

### File Organization

- One public class per file
- File name matches the class name
- Package structure follows `ai.h2o.sparkling.*`
- Group related functionality in packages

### Common Patterns

**H2O Context Usage**:
```scala
import ai.h2o.sparkling._
val hc = H2OContext.getOrCreate()
import hc.implicits._
```

**Data Conversion**:
```scala
// Spark to H2O
val h2oFrame = hc.asH2OFrame(dataFrame)

// H2O to Spark
val sparkDF = hc.asSparkFrame(h2oFrame)
```

**Configuration**:
```scala
import ai.h2o.sparkling._
val conf = new H2OConf()
  .setExternalClusterMode()
  .setCloudName("my-h2o-cluster")
val hc = H2OContext.getOrCreate(conf)
```

### Do's and Don'ts

**DO**:
- ✅ Use provided configuration mechanisms (H2OConf)
- ✅ Follow existing patterns in similar code
- ✅ Add tests for new features
- ✅ Update documentation for public APIs
- ✅ Handle resource cleanup properly

**DON'T**:
- ❌ Mix backend types in same application
- ❌ Modify Spark or H2O data structures directly when converters exist
- ❌ Skip error handling for user-facing APIs
- ❌ Commit commented-out code
- ❌ Use magic numbers; define constants

---

## Branching, Workflow & Commit Practices

### Branching Strategy

**Main Branches**:
- `master` - Main development branch, latest stable code
- `rel-*` - Release branches (e.g., `rel-3.46.0`)

**Feature Branches**:
- Create from `master`
- Naming: `feature/<descriptive-name>` or `fix/<issue-number>-description`
- Keep branches focused and short-lived
- Delete after merge

### Workflow

1. **Create Feature Branch**:
   ```bash
   git checkout master
   git pull origin master
   git checkout -b feature/my-new-feature
   ```

2. **Make Changes**:
   - Write code following style conventions
   - Add tests
   - Update documentation if needed

3. **Format and Test**:
   ```bash
   ./gradlew spotlessApply
   ./gradlew check
   ```

4. **Commit Changes**:
   ```bash
   git add .
   git commit -m "Add feature X"
   ```

5. **Push and Create PR**:
   ```bash
   git push origin feature/my-new-feature
   # Create PR on GitHub
   ```

### Commit Message Conventions

**Format**:
```
<type>: <short summary>

<optional detailed description>

<optional references>
```

**Types**:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting)
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Build process, dependencies

**Examples**:
```
feat: Add support for Spark 3.5

Implement compatibility layer for Spark 3.5 API changes
Update build configuration and dependencies

Closes #1234
```

```
fix: Resolve memory leak in DataFrame conversion

Clean up temporary H2O frames after conversion
Add test to verify resource cleanup
```

### Pull Request Process

1. **Create PR** using the template in `.github/pull_request_template.md`
2. **Description**: Explain what and why, not just how
3. **Link Issues**: Reference related issues
4. **Wait for CI**: All checks must pass
5. **Code Review**: Address reviewer feedback
6. **Approval**: At least one approval required
7. **Merge**: Squash or merge commits as appropriate

### Required Checks

- ✅ Build passes
- ✅ All tests pass
- ✅ Code formatting check passes
- ✅ No merge conflicts

---

## Local Development Tips

### Common Development Workflows

**Working on Core Features**:
1. Make changes in `core/src/main/scala/`
2. Run unit tests: `./gradlew :sparkling-water-core:test`
3. Run integration tests: `./gradlew :sparkling-water-core:integTest`
4. Test with examples: `./bin/run-example.sh <ExampleName>`

**Working on ML Algorithms**:
1. Modify algorithm in `ml/src/main/scala/ai/h2o/sparkling/ml/algos/`
2. Update corresponding tests
3. Run: `./gradlew :sparkling-water-ml:test`
4. Test in Sparkling Shell to verify API

**Working on Python API**:
1. Make changes in `py/src/`
2. Rebuild Python package: `./gradlew :sparkling-water-py:build`
3. Run Python tests: `./gradlew :sparkling-water-py:test`
4. Test interactively: `./bin/pysparkling`

**Updating Documentation**:
1. Edit RST files in `doc/src/site/sphinx/`
2. Build docs: `./gradlew :sparkling-water-doc:build`
3. View output in `doc/build/docs/`

### Debugging

**IntelliJ IDEA**:
1. Set breakpoints in code
2. Create "Remote JVM Debug" run configuration
3. Run tests with debug agent:
   ```bash
   ./gradlew :sparkling-water-core:test --debug-jvm
   ```
4. Attach debugger to port 5005

**Shell Debugging**:
```bash
# Enable verbose logging
export SPARK_PRINT_LAUNCH_COMMAND=1

# Run with debug logs
./bin/sparkling-shell --conf spark.ext.h2o.log.level=DEBUG
```

**Common JVM Options for Tests**:
```
-Dspark.testing=true          # Required for tests
-Dspark.ext.h2o.log.level=DEBUG  # Verbose H2O logs
```

### Troubleshooting

**Problem**: `./gradlew build` fails with "Cannot find spark-core"
- **Solution**: Set `SPARK_HOME` environment variable correctly
  ```bash
  export SPARK_HOME=/path/to/spark
  ```

**Problem**: Out of memory during build
- **Solution**: Increase Gradle memory
  ```bash
  export GRADLE_OPTS="-Xmx4g -XX:MaxMetaspaceSize=1g"
  ```

**Problem**: Tests fail with "Address already in use"
- **Solution**: H2O port conflict. Kill existing H2O processes:
  ```bash
  pkill -f h2o.jar
  ```

**Problem**: Python tests fail
- **Solution**: Ensure correct Python environment
  ```bash
  which python3
  python3 --version  # Should be 3.6-3.11
  ```

**Problem**: Scala formatting check fails
- **Solution**: Run auto-formatter
  ```bash
  ./gradlew spotlessApply
  ```

**Problem**: Version-specific build issues
- **Solution**: Check you're using correct property file
  ```bash
  ./gradlew build -Pspark=3.5  # Specify Spark version
  ```

### Platform-Specific Notes

**macOS**:
- May need to increase file descriptor limit: `ulimit -n 4096`
- Use Homebrew for Java/Spark installation

**Windows**:
- Use Git Bash or WSL for shell scripts
- See [Windows Tutorial](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/tutorials/run_on_windows.html)
- Set paths with forward slashes in environment variables

**Linux**:
- Ensure sufficient file descriptors: `ulimit -n 4096`
- Check Java is in PATH

---

## Database & Data Model

Sparkling Water does not maintain its own database. Instead:

### Data Storage

**Spark Side**:
- Data stored in Spark RDDs, DataFrames, Datasets
- Follows Spark's in-memory or disk-spill model
- Schema defined by Spark SQL types

**H2O Side**:
- Data stored in H2O Frames (distributed in-memory column store)
- Optimized for ML workloads
- Schema uses H2O's type system (numeric, categorical, string, time)

### Data Conversion

**Schema Mapping** (`core/src/main/scala/ai/h2o/sparkling/backend/converters/`):
- Spark types → H2O types: `SupportedTypes.scala`
- Handles: primitives, arrays, structs, timestamps
- Nullable fields converted to H2O's NA handling

**Key Conversions**:
```
Spark Type        → H2O Type
IntegerType       → Numeric column
StringType        → String/Categorical
TimestampType     → Time column
DoubleType        → Numeric column
ArrayType         → Multi-column expansion
```

### Data Constraints

- H2O frames are immutable after creation
- Column names must be unique
- Categorical encoding happens automatically for string columns in ML algorithms
- Missing values handled differently (Spark `null` vs H2O `NA`)

---

## Security, Performance & Quality Considerations

### Security

**Authentication**:
- H2O supports basic authentication and LDAP
- Configure via `H2OConf`: `.setUserName()`, `.setPassword()`
- Use SSL/TLS for production: `.setHashLogin(true)`, `.setUseSsl(true)`

**Sensitive Data**:
- Never commit credentials to Git
- Use environment variables or secret management systems
- AWS credentials via `HADOOP_CONF_DIR` pointing to secure `core-site.xml`
- Filter sensitive data from logs

**Authorization**:
- H2O Flow UI access control via authentication
- Spark security via standard Spark mechanisms

**Code Security**:
- Dependency scanning via Gradle
- Keep H2O and Spark versions up to date
- Review third-party dependencies

### Performance

**Known Considerations**:
- **Data Conversion Overhead**: Converting between Spark and H2O has cost
  - Minimize conversions in tight loops
  - Convert once, reuse H2O frames
  
- **Memory Management**:
  - H2O uses off-heap memory
  - Configure both Spark and H2O memory: `spark.executor.memory` and `spark.ext.h2o.node.memory`
  
- **Backend Choice**:
  - Internal backend: Better for small clusters, less network overhead
  - External backend: Better for large clusters, isolation
  
- **Caching**:
  - Cache Spark DataFrames before conversion if reused
  - H2O frames are already in memory

**Benchmarking**:
```bash
./gradlew bench
# Results in build/reports/benchmarks/
```

### Quality Gates

**CI Checks** (Jenkins - see `ci/Jenkinsfile*`):
- ✅ Build succeeds for all Spark versions (2.3-3.5)
- ✅ Unit tests pass
- ✅ Integration tests pass
- ✅ Code formatting (spotless)
- ✅ Scala compilation with no warnings
- ✅ Python tests
- ✅ R package checks

**Local Quality Checks**:
```bash
# Full check
./gradlew check

# Just formatting
./gradlew spotlessCheck

# All tests
./gradlew test integTest
```

**Coverage**: Not enforced by default, but encouraged for new code

---

## CI/CD & Deployment

### CI System

**Provider**: Jenkins (internal H2O.ai infrastructure)

**Pipeline Files**:
- `ci/Jenkinsfile` - Main build pipeline
- `ci/Jenkinsfile-kubernetes` - Kubernetes tests
- `ci/Jenkinsfile-databricks` - Databricks integration tests
- `ci/Jenkinsfile-release` - Release process
- `ci/Jenkinsfile-benchmarks` - Performance benchmarks

### CI Checks on PRs

When you open a PR:
1. **Build**: All modules compile
2. **Unit Tests**: `./gradlew test`
3. **Integration Tests**: `./gradlew integTest`
4. **Formatting**: `./gradlew spotlessCheck`
5. **Multi-version**: Tests on multiple Spark versions
6. **Platform Tests**: Linux primary, some Windows/Mac checks

### Test Environments

- **Local Mode**: Basic functionality
- **Standalone Cluster**: Multi-node behavior
- **YARN**: Hadoop ecosystem integration
- **Kubernetes**: Container orchestration
- **Cloud Platforms**: AWS EMR, Azure HDInsight, Databricks

### Preview Environments

- Not applicable (library project)
- Documentation previews available after merging to `master`

### Deployment Process (for Contributors)

**You typically won't deploy directly**, but here's what happens:

1. **PR Merged** → `master` branch updated
2. **Nightly Build** → Snapshot artifacts to S3
3. **Release** (by maintainers):
   - Tag created (e.g., `rel-3.46.0.2-1`)
   - Build all Spark versions
   - Publish to Maven Central
   - Update documentation site
   - Create GitHub release

**Artifacts Published**:
- Maven Central: `ai.h2o:sparkling-water-*`
- S3: Binary distributions
- PyPI: `h2o_pysparkling_*` packages
- CRAN: `rsparkling` package

---

## Extending the System

### Adding a New H2O Algorithm Wrapper

**Steps**:

1. **Create Algorithm Class** in `ml/src/main/scala/ai/h2o/sparkling/ml/algos/`:
   ```scala
   package ai.h2o.sparkling.ml.algos
   
   import ai.h2o.sparkling.ml.params._
   
   class H2OMyAlgorithm(override val uid: String)
     extends H2OAlgorithm[MyAlgorithmParameters]
     with H2OMyAlgorithmParams {
     
     def this() = this(Identifiable.randomUID("myAlgo"))
     
     override def fit(dataset: Dataset[_]): H2OMyAlgorithmModel = {
       // Implementation
     }
   }
   ```

2. **Define Parameters** in `ml/src/main/scala/ai/h2o/sparkling/ml/params/`:
   ```scala
   trait H2OMyAlgorithmParams extends H2OAlgoParams {
     // Define algorithm-specific parameters
   }
   ```

3. **Create Model Class**:
   ```scala
   class H2OMyAlgorithmModel(override val uid: String)
     extends H2OModel[MyAlgorithmMojoModel]
   ```

4. **Add Tests** in `ml/src/test/scala/`:
   ```scala
   class H2OMyAlgorithmTestSuite extends FunSuite with Matchers {
     test("algorithm trains successfully") {
       // Test code
     }
   }
   ```

5. **Generate Python/R APIs**:
   ```bash
   ./gradlew :sparkling-water-api-generation:generatePython
   ./gradlew :sparkling-water-api-generation:generateR
   ```

6. **Update Documentation**: Add algorithm docs in `doc/src/site/sphinx/ml/`

### Adding a New REST Endpoint

1. **Define Endpoint** in `core/src/main/scala/ai/h2o/sparkling/api/`:
   ```scala
   import water.api._
   
   class MyEndpoint extends AbstractH2OResource {
     @GET
     @Path("/myendpoint")
     def doGet(): MyResponseSchema = {
       // Implementation
     }
   }
   ```

2. **Register Endpoint** in `core/src/main/scala/ai/h2o/sparkling/backend/utils/RestApiUtils.scala`

3. **Test** via integration test

### Adding a New Configuration Option

1. **Add to H2OConf** in `core/src/main/scala/ai/h2o/sparkling/H2OConf.scala`:
   ```scala
   def setMyOption(value: String): H2OConf = {
     set("spark.ext.h2o.my.option", value)
   }
   
   def myOption: String = sparkConf.get("spark.ext.h2o.my.option", "default")
   ```

2. **Update SharedBackendConf** if needed for backend initialization

3. **Document** in `doc/src/site/sphinx/configuration/configuration.rst`

4. **Add Python binding** in `py/src/ai/h2o/sparkling/H2OConf.py`

### Adding a New Module

If creating a new Gradle subproject:

1. **Create Directory**: `mkdir my-module`

2. **Add to `settings.gradle`**:
   ```groovy
   include 'my-module'
   ```

3. **Create `build.gradle`** in `my-module/`:
   ```groovy
   description = "My Module Description"
   
   dependencies {
     api project(':sparkling-water-core')
     // other dependencies
   }
   ```

4. **Add to Published Projects** in root `build.gradle` if publishing

5. **Create Source Structure**:
   ```
   my-module/
   ├── build.gradle
   └── src/
       ├── main/scala/
       └── test/scala/
   ```

---

## Glossary & References

### Terminology

- **H2OContext**: Main entry point for Sparkling Water; manages H2O cluster lifecycle
- **H2OFrame**: H2O's distributed in-memory data structure (analogous to Spark DataFrame)
- **MOJO**: Model Object, Optimized - H2O's model serialization format
- **Backend**: Deployment mode (internal: H2O on Spark executors, external: separate H2O cluster)
- **Flow**: H2O's web UI for interactive ML
- **PySparkling**: Python interface to Sparkling Water
- **RSparkling**: R interface to Sparkling Water
- **DAI**: Driverless AI - H2O.ai's automated ML product

### Key Links

**Documentation**:
- [User Documentation](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/index.html)
- [API Documentation](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/api.html)
- [FAQ](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/FAQ.html)
- [Migration Guide](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/migration_guide.html)

**Development**:
- [GitHub Repository](https://github.com/h2oai/sparkling-water)
- [Issue Tracker](https://github.com/h2oai/sparkling-water/issues)
- [H2O-3 Repository](https://github.com/h2oai/h2o-3)
- [Stack Overflow - sparkling-water tag](https://stackoverflow.com/questions/tagged/sparkling-water)

**Downloads**:
- [Latest Releases](http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.5/latest.html)
- [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ai.h2o%22%20AND%20a%3Asparkling-water*)

**Related Projects**:
- [Apache Spark](https://spark.apache.org/)
- [H2O-3](http://docs.h2o.ai/)
- [H2O Driverless AI](https://www.h2o.ai/products/h2o-driverless-ai/)

**Community**:
- [H2O Community Slack](https://h2oai-community.slack.com/)
- [Discussion Forum](https://github.com/h2oai/sparkling-water/discussions)

---

## Additional Resources

### Examples

Explore `examples/` directory for:
- Basic data conversions
- Machine learning pipelines
- Integration with Spark ML
- Deployment scenarios

Run examples:
```bash
./bin/run-example.sh AirlinesWithWeatherDemo
./bin/run-example.sh ChicagoCrimeDemo
```

### Documentation

Build documentation locally:
```bash
./gradlew :sparkling-water-doc:build
# Output: doc/build/docs/
```

### Getting Help

1. Check [FAQ](http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/FAQ.html)
2. Search [existing issues](https://github.com/h2oai/sparkling-water/issues)
3. Ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/sparkling-water)
4. Open a [new issue](https://github.com/h2oai/sparkling-water/issues/new/choose)

---

**Happy Coding! 🚀**

For questions or contributions, see our [Code of Conduct](CODE_OF_CONDUCT.md) and feel free to open issues or pull requests.
