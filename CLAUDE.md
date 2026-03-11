# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CCAS (Chess Club Admin System) is a Scala 3 application that pulls data from the Chess.com public API and stores it in a database for chess club management tasks: tracking membership, analysing member performance, and scouting players from other clubs.

## Build & Test Commands

This is an SBT project (Scala 3.6.4, SBT 1.10.11+).

- **Compile:** `sbt compile`
- **Run all tests:** `sbt test`
- **Run a single test suite:** `sbt "testOnly ccas.api.TestApiJsonParsing"` (fully qualified object name)
- **Continuous compile on change:** `sbt ~compile`
- **Interactive SBT shell:** `sbt` then run commands without the `sbt` prefix

Tests use ZIO Test (`ZIOSpecDefault`). SQL tests require a running PostgreSQL instance with a `ccas` database and `test` schema (see `src/test/resources/application.conf`).

## Architecture

### Layers

The codebase has two main layers:

1. **`ccas.api`** — Chess.com API models and client. Case classes model API JSON responses (e.g., `ApiPlayer`, `ApiClub`, `ApiDailyMatch`). Each API model companion object extends `JsonDecoding[T]` to provide a ZIO JSON decoder. API models are read-only data transfer objects; they are never written to the database directly.

2. **`ccas.analysis`** — Domain tables and business logic. `analysis.tables` contains database-persisted entities (`Player`, `PlayerSnapshot`, `Club`, `ClubMember`). `analysis.apps` contains runnable applications (e.g., `MembershipApp extends ZIOAppDefault`).

### Key Patterns

**Repository pattern with multi-database support:** Each table entity (e.g., `Club`, `Player`) follows an identical structure:
- A case class defines the table schema
- Its companion object extends `SqlRepoUtils`, which wires up a `RepoResolver`
- Inside the companion, a `sealed trait FooRepository` defines inline Quill queries and abstract methods
- Concrete `PostgresRepo` and `SqliteRepo` private case classes implement the trait for each database dialect
- Public static methods on the companion (e.g., `Club.selectAll`, `Player.insert`) delegate to `repoService`
- The `QuillWrapper` ZIO layer auto-detects the database type and provides the right `Quill` context

**Type safety with Subtypes:** Domain IDs and constrained values use `zio-prelude` `Subtype` via `CcasSubtype`/`CcasKeySubtype` (in `ccas.api.misc.subtypes`). These provide JSON codecs, SQL encodings, and validation in one place. Types: `PlayerId`, `ClubId`, `Username`, `ClubUrlName`, `Elo`, `Percentage`, etc.

**Enum serialization:** Enums extend `EnumJson[T]` for JSON (snake_case wire format, PascalCase in Scala) and/or `EnumSql[T]` for database persistence. Some enums override `jsonToEnum` for non-standard Chess.com API mappings.

**JSON decoding trait:** API model companions extend `JsonDecoding[T]`, which wraps `JsonDecoder` with convenience methods (`decodeZIO`, string extensions). The derived decoder is provided via `jsonDecoderDerived`. API models use `@jsonMemberNames(SnakeCase)` for field mapping.

**ZIO effect types:** SQL operations use `SqlTask[A]` (alias for `IO[SQLException, A]`). Repository operations return `RepoTask[A]` (alias for `RIO[Repo, A]`). The app is fully ZIO-based with `ZLayer` dependency injection.

### HTTP Client

`ChessComClient` wraps `zio-http` `Client` for making batched GET requests with automatic JSON decoding. It includes a default `User-Agent` header and a semaphore (default permits=1) for rate-limited Chess.com API access.

### Database

Uses Quill (`zio-protoquill`) with `SnakeCase` naming strategy. Database config is read from `application.conf` under the `database` prefix. Table names map from case class names via snake_case convention.

### Test Data

API JSON test fixtures live in `data/test/api/*.json`. Tests in `TestApiJsonParsing` validate that these parse correctly into API model types.
