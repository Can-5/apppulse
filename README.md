# AppPulse

**Lightweight system-metrics modeling for Orbit: smoothing + alert rules, no capture.**

> Orbit companion module — phase 2/4 of the [Orbit ecosystem](https://github.com/Can-5/orbit).
> Standalone crate: usable without Orbit, without other modules, and without network.

## Implemented now

- Sample / RingBuffer<T> (capped, evicts oldest) / Ema with jitter-safe re-seeding
- AlertRule + AlertTracker: consecutive-streak alerts with one-shot firing

## Not implemented yet (explicitly out of scope for this crate)

- No /proc or perf readings — sampling lives in the Orbit host (ProcessMetrics permission)
- No UI

## Build & test

```sh
cargo test  # 7 tests
```

Dependencies: serde, thiserror. No cross-repo path dependencies, no Git dependencies, no downloads at build time beyond crates.io.

## Privacy

Metrics stay on device; there is no network code and no telemetry, ever.

## License

MIT © The Orbit contributors. See [LICENSE](./LICENSE).
