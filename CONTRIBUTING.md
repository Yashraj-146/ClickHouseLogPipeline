# Contributing

Thanks for looking at this project. It's primarily a portfolio/interview piece, but
it's built and tested the way a production service would be, and contributions that
keep it that way are welcome.

---

## Building the project

Requires JDK 17. Use the checked-in Maven Wrapper so everyone builds with the same
Maven version:

```bash
./mvnw clean package
```

This compiles the code, runs the full test suite, and produces
`target/clickhouselogpipeline-0.0.1-SNAPSHOT.jar`. Add `-DskipTests` only if you have
a specific reason to skip the suite - it needs no Docker daemon, no ClickHouse
instance, and no external Kafka broker, so there's normally no reason to.

To run the full stack (ClickHouse, Grafana, Kafka, Kafka UI, the app):

```bash
docker compose up --build
```

See [README.md](README.md) for endpoints, ports, and how to switch ingestion modes.

---

## Running the tests

```bash
./mvnw test
```

44 tests, well under 15 seconds, no external services required - every
`JdbcTemplate` is mocked, and the one test that needs a Kafka broker
(`KafkaModeIngestionIntegrationTest`) uses `@EmbeddedKafka`, an in-JVM broker. See the
"Running the Tests" section of [README.md](README.md) for the full breakdown of unit
vs. web-slice vs. integration tests.

Run a single test class:

```bash
./mvnw test -Dtest=BatchWriterTest
```

---

## Coding conventions

This project follows a small number of architectural rules deliberately, documented
in full in `CLAUDE.md`. The short version:

- **Controllers stay thin.** `LogController` only validates and delegates to
  `IngestionService`. No batching or persistence logic belongs there.
- **`BatchWriter` is the only component that writes to ClickHouse.** Both the direct
  and Kafka ingestion paths converge on `BatchWriter.enqueue()`. Don't add a second,
  parallel writer - if you need new storage, add a new writer component instead of
  overloading `BatchWriter`.
- **Kafka is a transport, not a replacement for the queue.** `LogConsumer` calls
  `BatchWriter.enqueue()` and nothing else; it never talks to ClickHouse directly.
- **Extend `LogDTO` for new metadata** (hostname, request id, region, etc.) instead of
  introducing a parallel DTO.
- **Kafka beans must stay optional.** Anything Kafka-related belongs behind
  `@ConditionalOnProperty(name = "pipeline.ingestion.mode", havingValue = "kafka")` so
  direct mode has zero dependency on a running broker.
- **Prefer configuration over hardcoding** for anything test suites or deployments
  might reasonably need to tune (see `pipeline.batch.*` in `application.properties`).

Java style otherwise follows standard Spring Boot conventions: constructor injection,
package-by-layer (`controller` / `service` / `kafka` / `config` / `dto`), and
`@DisplayName` on test classes/methods for readable test output.

---

## Submitting changes

1. Fork the repository and create a branch off `main`.
2. Make your change, keeping it scoped to one concern per PR.
3. Add or update tests for anything behavioral - `./mvnw test` must pass locally.
4. Use [Conventional Commits](https://www.conventionalcommits.org/) for commit
   messages (`feat: ...`, `fix: ...`, `docs: ...`, `test: ...`, `chore: ...`).
5. Open a PR against `main` and fill out the PR template. CI (build + test, and
   CodeQL) runs automatically on every PR and must pass before merge.
6. If your change affects setup, configuration, or behavior described in
   `README.md`, update it in the same PR.

Releases (a version tag such as `v1.2.0`) trigger a separate workflow that builds a
Docker image and publishes it to GitHub Container Registry - you don't need to do
anything for this beyond getting your change merged to `main`.
