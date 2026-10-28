# Changelog

## [0.2.0-alpha](https://github.com/Dynatrace/openfeature-provider-go/compare/v0.1.1-alpha...v0.2.0-alpha) (2026-10-28)


### ⚠ BREAKING CHANGES

* update flagd-core to the latest major ([#30](https://github.com/Dynatrace/openfeature-provider-go/issues/30))

### ✨ New Features

* bound cdn fetch duration and skip poll ticks that contend with an in-flight fetch ([#21](https://github.com/Dynatrace/openfeature-provider-go/issues/21)) ([60d2da4](https://github.com/Dynatrace/openfeature-provider-go/commit/60d2da489779ee742c6b35bd889c67eccfc8bfeb))
* update flagd-core to the latest major ([#30](https://github.com/Dynatrace/openfeature-provider-go/issues/30)) ([974ea45](https://github.com/Dynatrace/openfeature-provider-go/commit/974ea45dfbb5b5a7d954efa51593c0a24daa57fd))


### 🧹 Chore

* bump fm-provider-test-harness to v1.2.0 and skip [@sse](https://github.com/sse) scenarios ([#27](https://github.com/Dynatrace/openfeature-provider-go/issues/27)) ([6939b6c](https://github.com/Dynatrace/openfeature-provider-go/commit/6939b6c3def63d5005b097453bf5c679811be2aa))
* bump fm-provider-test-harness to v1.3.0 ([#29](https://github.com/Dynatrace/openfeature-provider-go/issues/29)) ([7329214](https://github.com/Dynatrace/openfeature-provider-go/commit/7329214cff2c937ba73e53272677d5670c691f17))
* **deps:** bump github.com/moby/go-archive from 0.2.0 to 0.3.0 in /e2e ([#23](https://github.com/Dynatrace/openfeature-provider-go/issues/23)) ([6be995c](https://github.com/Dynatrace/openfeature-provider-go/commit/6be995c9cab004eff8a3887feab98a6682800922))
* **deps:** bump github.com/moby/sys/user from 0.4.0 to 0.4.1 in /e2e ([#24](https://github.com/Dynatrace/openfeature-provider-go/issues/24)) ([b0f2830](https://github.com/Dynatrace/openfeature-provider-go/commit/b0f2830cb578f8983c8b71c3409ebc5a9272817c))
* upgrade GitHub Actions to Node 24, ref: ASDY-27739 ([#18](https://github.com/Dynatrace/openfeature-provider-go/issues/18)) ([346b0c9](https://github.com/Dynatrace/openfeature-provider-go/commit/346b0c95dd22829915ea37fd3df6db137ba9c2a4))


### 📚 Documentation

* align README with the standard provider format ([#19](https://github.com/Dynatrace/openfeature-provider-go/issues/19)) ([3bdec25](https://github.com/Dynatrace/openfeature-provider-go/commit/3bdec2577cc5bbf919acd7081e60b0313a7138e8))

## [0.1.1-alpha](https://github.com/Dynatrace/openfeature-provider-go/compare/v0.1.0-alpha...v0.1.1-alpha) (2026-08-28)


### 🧹 Chore

* bump flagd-core to v0.16.2 ([#16](https://github.com/Dynatrace/openfeature-provider-go/issues/16)) ([5cd7d92](https://github.com/Dynatrace/openfeature-provider-go/commit/5cd7d922cdf797455250e9900e233d9dd6934c5a))

## [0.1.0-alpha](https://github.com/Dynatrace/openfeature-provider-go/compare/v0.0.2-alpha...v0.1.0-alpha) (2026-08-27)


### ⚠ BREAKING CHANGES

* rename WithMetricsURL to WithMetricsOrigin, accept origin-only value ([#14](https://github.com/Dynatrace/openfeature-provider-go/issues/14))

### ✨ New Features

* rename WithMetricsURL to WithMetricsOrigin, accept origin-only value ([#14](https://github.com/Dynatrace/openfeature-provider-go/issues/14)) ([9e61a22](https://github.com/Dynatrace/openfeature-provider-go/commit/9e61a221d2132bc741df7174957ddab8f650ec02))


### 🐛 Bug Fixes

* validate configOrigin is a bare origin, not just path-free ([#12](https://github.com/Dynatrace/openfeature-provider-go/issues/12)) ([15e6218](https://github.com/Dynatrace/openfeature-provider-go/commit/15e62181a035635930f88dac94154acbdce6992d))


### 🧹 Chore

* upgrade GitHub Actions to Node 24 compatible versions ([#11](https://github.com/Dynatrace/openfeature-provider-go/issues/11)) ([715cc8d](https://github.com/Dynatrace/openfeature-provider-go/commit/715cc8d59d53de3d9109673800045daf3c16a03e))

## [0.0.2-alpha](https://github.com/Dynatrace/openfeature-provider-go/compare/v0.0.1-alpha...v0.0.2-alpha) (2026-06-10)


### ✨ New Features

* CDN fetcher, polling manager, and internal package structure ([#3](https://github.com/Dynatrace/openfeature-provider-go/issues/3)) ([b739901](https://github.com/Dynatrace/openfeature-provider-go/commit/b739901a259ba7a53b1a6c94c6e3265d735b6142))
* DynatraceProvider — state machine, events, flagd-core evaluation ([#4](https://github.com/Dynatrace/openfeature-provider-go/issues/4)) ([f24ff5b](https://github.com/Dynatrace/openfeature-provider-go/commit/f24ff5b6cf81c214a1ee48306f00487763ed4eb4))
* evaluation metrics aggregation, hook, and flusher ([#8](https://github.com/Dynatrace/openfeature-provider-go/issues/8)) ([361ceba](https://github.com/Dynatrace/openfeature-provider-go/commit/361ceba537d5319d6f3d26a708d205c1b66fe0c8))
* scaffold module, CI, and provider stub ([#1](https://github.com/Dynatrace/openfeature-provider-go/issues/1)) ([64a47de](https://github.com/Dynatrace/openfeature-provider-go/commit/64a47dec5c295f0fc0d0877197bd76e673d88709))
* SDK key validation, CDN URL derivation, and retry strategy ([#2](https://github.com/Dynatrace/openfeature-provider-go/issues/2)) ([f8fc5af](https://github.com/Dynatrace/openfeature-provider-go/commit/f8fc5afc413d1b4ccd9c34f6ee391e144675237c))


### 🧹 Chore

* refactors for simplicity and go-ishness ([#9](https://github.com/Dynatrace/openfeature-provider-go/issues/9)) ([2ac7fa0](https://github.com/Dynatrace/openfeature-provider-go/commit/2ac7fa01a127d7fa37ec0553ddd7734b5b69e4a7))
