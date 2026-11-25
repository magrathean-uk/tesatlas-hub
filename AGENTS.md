# Tesatlas Hub

<!-- clean-development-policy:v6 -->
Purpose: Rust telemetry and sync service with an AppKit control app (public OSS Cargo workspace). See `Cargo.toml` and [Source layout](docs/architecture/source-layout.md) for ownership boundaries. The crate forbids unsafe Rust.

In the Tesatlas workspace, follow `../AGENTS.md`; the programme plan and passover (`.plan/master-plan.md`, `.plan/STATE.md`, reached through the `.plan` link) win on process, scope and commit policy. This repo is public: private notes, findings and receipts stay in the workspace `.plan/` or `~/dev/records/teslatlas/`, never here.

## Scope
- Packages: `compute/`, `protocol/`, `sdk-typescript/` (Apache-2.0, built and tested with the Hub); `home-assistant/` (Apache-2.0, parked, must keep working at every API change); `edge/` (AGPL-3.0, outside the Cargo workspace), `sdk-swift/`, `viewer/` (Apache-2.0): retired, source preserved, not built by the hub gates. Imported history stays unchanged.
- Goal 2: complete the Mac Hub. It ships as the GUI app with its per-user LaunchAgent only; the `.pkg` installer, its pre/postinstall scripts and the Rust `install`/`service` verbs are retired. In scope: macOS 13 runtime verification, local notifications, keep-awake, optional encrypted backups, per-device car scope, the JSON read API profile and the physical live sync wire. Linux, Debian, Docker and Pi come after the Mac Hub. No browser client in v7.
- GitHub is source storage: no CI, automated security workflows, GitHub Releases, binary publication, signing or deployment. No vehicle commands; no public ingress operated by Magrathean. Never commit credentials, private logs, location rows or real telemetry fixtures.
- Wire-contract changes start in the Protocol package. App changes belong to the App writer. One writer per repository; helpers are read-only. Preserve unrelated edits and private data.

## Validate the changed behaviour
Use the workspace runner and heavy-build lock (rustup 1.98.1). From this directory:

```sh
../scripts/dev/run.sh hub cargo check --locked
../scripts/dev/run.sh hub cargo test --locked --lib TEST_FILTER
../scripts/dev/run.sh hub cargo clippy --locked --all-targets -- -D warnings
```

- Replace `TEST_FILTER` with the affected test; broaden for shared behaviour or regressions. Standalone source work: [Build from source](docs/guides/build-from-source.md). For documentation, check links and factual claims; do not build the app to validate prose.
- AppKit: `scripts/test-macos-appkit-focused.sh TestClass/testMethod` with an actual XCTest selector (Xcode-beta when installed, then Xcode, unless `DEVELOPER_DIR` is set).
- Repository gates read `git ls-files`, so rerun them after staging added or deleted paths, and classify every new tracked file in `provenance-manifest.json`. Never force-add or remove files to satisfy them as part of a docs edit:

```sh
python3 scripts/verify-repository-layout.py
python3 scripts/verify-provenance.py
python3 scripts/verify-name-policy.py --strict
```

## Owner-controlled files
Legal files (owner, 2026-09-27): `NOTICE`, `LICENSE`, `.github/CONTRIBUTING.md`, `docs/legal/additional-terms.md`, `docs/legal/overview.md`, `docs/legal/trademarks.md`, everything in `docs/governance/`, and the checksum lock `docs/legal/owner-controlled-files.sha256`. Change them only on the owner's explicit instruction. Keep the copyright and attribution lines in `src/lib.rs` and the macOS `AppDelegate.swift` exactly as they are.

## Runtime and evidence
- Unsigned source runs need `TESATLAS_HUB_DEVELOPMENT=1` and `TESATLAS_HUB_DEVELOPMENT_MODE` of `fixture`, `standalone` or `private-lan`; follow [Source-run control app](macos/TesatlasHubApp/DEVELOPMENT.md) for paths and TLS. Bind loopback by default; `private-lan` needs an exact RFC1918 IPv4 bind, a matching HTTPS URL and port, TLS and existing authentication. Use an identified binary outside the product tree, never a `target/` symlink.
- `serve` holds a lifetime lock; stop it before `pair`, `backup` or `repair`. While serving use `/healthz` and `/readyz`. Large-history measurements use the release CLI built through the managed wrapper (`cargo build --locked --release --bin tesatlas-hub`).
- Done when the task's actual acceptance path ran, with one short receipt: repo@sha, command, host, result. A build, fixture or unit test does not prove live collection or the App pairing and sync path.

## Pending URL migration
The next release must apply [NEXT-RELEASE-URLS.md](NEXT-RELEASE-URLS.md): product sites move to `https://magrathean.uk/solutions/<slug>/` and support to `contact+<slug>@magrathean.uk`. Remove this section with that file once released.
