# Repository Guidelines

## Project Structure & Modules
- Core libraries live in `core/src/main/scala` with tests in `core/src/test/scala`; this is where Otel4s tracing is wired into Redis4cats primitives.
- `effects` and `streams` add Cats-Effect and stream-specific integrations and depend on `core`.
- Documentation sources are in `docs` and the Typelevel site in `site`; site assets publish from `site/target/docs/site`.
- The build is a cross project (`rootJVM`/`rootJS`) targeting Scala 2.13 (default) and Scala 3; shared settings sit in `build.sbt` and `project/`.

## Build, Test, and Development Commands
- `sbt +test` — run the full test matrix across Scala 2.13 and 3 for JVM/JS.
- `sbt 'project rootJVM' test` (or `rootJS`) — faster scope when iterating on a single platform.
- `sbt scalafmtAll scalafixAll` — auto-format and organize imports; keep sources clean before pushing.
- `sbt headerCheckAll scalafmtCheckAll 'project /' scalafmtSbtCheck` — the CI formatter checks locally.
- `sbt docs/tlSite` — build the site; `sbt prepareCi` runs the full CI sequence (format, fix, test, docs, MiMa).
- Optional: `nix develop` enters a dev shell with JDK 11 and Node from the provided flake.

## Coding Style & Naming Conventions
- ScalaFmt 3.7.1 with 120-column limit and `scala213source3` dialect (Scala 3 in `scala-3/`); rely on `scalafmtAll`.
- Scalafix runs `OrganizeImports` (keeps unused imports; target dialect Scala 3).
- Follow package prefix `dev.profunktor.redis4cats.otel4s`; use PascalCase for types/objects, camelCase for methods/vals, and `*Test`/`*Suite` for tests.
- Prefer composition and pure functions; keep APIs minimal and effect-polymorphic when possible.

## Testing Guidelines
- Tests use MUnit and `munit.CatsEffectSuite`; place them under the matching `src/test/scala` module.
- Name tests descriptively via `test("does X") { ... }`; use `assertEquals`/`assert` helpers for clarity.
- Run `sbt +test` before PRs; add targeted property/unit tests when touching tracing behavior or command wrapping order.

## Commit & Pull Request Guidelines
- Use concise, imperative commit subjects (see history: “Refactor…”, “Update…”); keep one logical change per commit when feasible.
- PRs should describe the behavior change, note affected modules (core/effects/streams/site), and list verification steps or relevant commands run.
- Link related issues/PRs and call out cross-version or JS-specific impacts; include screenshots only when changing docs/site output.
