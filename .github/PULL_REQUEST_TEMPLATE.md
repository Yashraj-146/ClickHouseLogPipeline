## Summary

<!-- What does this PR change, and why? -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor (no behavior change)
- [ ] Documentation
- [ ] CI/CD / tooling

## Architecture checklist

<!-- See CONTRIBUTING.md / CLAUDE.md for the full rules. Quick self-check: -->

- [ ] Controllers stay thin (validation + delegation only, no batching/persistence logic)
- [ ] `BatchWriter` remains the only component that writes to ClickHouse
- [ ] New ingestion metadata was added to `LogDTO`, not a new/parallel DTO
- [ ] No new dependency on Kafka in direct mode (Kafka beans stay behind `@ConditionalOnProperty`)

## Testing

<!-- How did you verify this? -->

- [ ] `./mvnw test` passes locally
- [ ] Added/updated tests for the change
- [ ] Manually verified against a running stack (if applicable), e.g. `docker compose up --build`

## Checklist

- [ ] CI is green
- [ ] Docs updated if behavior, config, or setup steps changed (`README.md`)
