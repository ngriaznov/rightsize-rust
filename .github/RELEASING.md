# Releasing rightsize

Releases go to [crates.io](https://crates.io) as four crates. crates.io has no
namespace verification — publishing rights belong to whoever owns the crate
names, which the first publish claims permanently.

## One-time setup

1. Log in to crates.io with the ngriaznov GitHub account and verify the email
   address (crates.io refuses publishes from unverified accounts).
2. Create an API token (Account Settings → API Tokens; scope `publish-new` +
   `publish-update` is enough) and store it: `cargo login <token>`.

## Per release

1. Confirm `main` is green in CI, including `msb-windows`.
2. Bump `version` in all four `crates/*/Cargo.toml` files **and** in the
   intra-workspace dependency declarations that pin it (`rightsize = { path =
   "../rightsize", version = "..." }` in `rightsize-msb`, `rightsize-docker`,
   and `rightsize-modules`). Keep all four versions identical.
3. Move the CHANGELOG's `Unreleased` content under a dated `## [X.Y.Z]` heading,
   and switch the README/docs dependency snippets from the pre-release
   `{ git = ... }` form to registry versions.
4. Sanity-check the leaf crate's packaging (the only one that can be verified
   before its dependencies exist on the registry):

   ```sh
   cargo package -p rightsize
   ```

5. Publish in dependency order — each step must complete before the next, since
   every crate builds against the registry copy of its dependencies:

   ```sh
   cargo publish -p rightsize
   cargo publish -p rightsize-msb
   cargo publish -p rightsize-docker
   cargo publish -p rightsize-modules
   ```

   crates.io usually indexes a new crate within seconds; if a dependent publish
   fails with "no matching package", wait a moment and retry that step.
   (`cargo publish --workspace` automates this ordering but is still
   nightly-only; switch to it once it reaches stable.)

   Publishing is permanent: versions can be yanked (`cargo yank`) but never
   deleted, and the names cannot be reclaimed.
6. Commit the version/CHANGELOG changes, tag, and push:

   ```sh
   git tag vX.Y.Z && git push origin main vX.Y.Z
   ```

## Coordinates

```toml
[dev-dependencies]
rightsize = "X.Y.Z"
rightsize-modules = "X.Y.Z"   # backend-msb + backend-docker are default features
```
