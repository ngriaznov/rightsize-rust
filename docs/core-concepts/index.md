# Core Concepts

The `rightsize` crate is the backend-agnostic core: the `Container` builder and its
RAII `ContainerGuard`, `Network` for alias-based connectivity, the `Wait` readiness
strategies, and `MountableFile` for getting host data into a guest. Everything in
this section applies identically regardless of which backend (`rightsize-msb` or
`rightsize-docker`) ends up running the container — that's the whole point of the
`SandboxBackend` trait boundary (see [How It Works](../how-it-works.md)).

- [Containers & Guards](./containers-and-guards.md) — the builder, the RAII lifecycle,
  and the two-tier cleanup story.
- [Wait Strategies](./wait-strategies.md) — the four built-in readiness checks and
  when to reach for a custom one.
- [Networking](./networking.md) — cross-container alias resolution and its limits on
  the microVM backend.
- [Files & Resources](./files-and-resources.md) — mounting host files into a
  container, and memory limits.
