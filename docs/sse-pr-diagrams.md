# SSE work: diagrams per PR

Placeholder notes, see [`sse-handover.md`](sse-handover.md). Mermaid, so it renders in GitHub, GitLab and most markdown viewers. PR A (#28) and PR B (#36) are written. PR 2 and PR 3 are planned.

## Overview

```mermaid
flowchart LR
  main["main"] --> A["PR A (#28)<br/>open the stream"]
  A --> B["PR B<br/>catch up, relax polling"]
  B -.-> P2["PR 2 (planned)<br/>message handling"]
  P2 -.-> P3["PR 3 (planned)<br/>lifecycle"]
```

## PR A: open the advertised stream

```mermaid
sequenceDiagram
  participant P as Provider
  participant C as CDN
  participant S as SSE stream
  P->>C: GET config
  C-->>P: flags + eventStreams
  Note over P: no entry or WithSSEDisabled means polling only
  P->>S: open stream (150 s read timeout)
  S--xP: refused or down
  Note over P: keep retrying with jitter, stay READY, keep polling
```

## PR B: catch up and relax polling on connect

```mermaid
sequenceDiagram
  participant P as Provider
  participant C as CDN
  participant S as SSE stream
  Note over P: no stream yet, poll every 10 s
  P->>S: open stream
  S-->>P: connected
  P->>C: catch-up GET
  C-->>P: config
  Note over P: connected, poll every 10 min as a safety net
```

## PR 2 (planned): handle messages

```mermaid
sequenceDiagram
  participant S as SSE stream
  participant P as Provider
  participant C as CDN
  S->>P: message
  alt type is refetchConfig
    P->>C: conditional GET (If-None-Match)
    C-->>P: 200 new config or 304
    Note over P: changed flags emit PROVIDER_CONFIGURATION_CHANGED
  else unknown, empty or unparseable
    Note over P: dropped, stream stays open
  end
  Note over P: one fetch at a time, a colliding re-fetch is deferred and coalesced
```

## PR 3 (planned): lifecycle

Provider state follows CDN fetch results only.

```mermaid
stateDiagram-v2
  [*] --> READY: init ok
  READY --> STALE: any fetch fails
  STALE --> ERROR: grace period expires
  STALE --> READY: fetch succeeds
  ERROR --> READY: fetch succeeds
```

The stream only picks the poll cadence. It never changes the state.

```mermaid
stateDiagram-v2
  Connected: poll every 10 min
  Disconnected: poll every 10 s
  [*] --> Disconnected
  Disconnected --> Connected: stream connects, catch-up fetch
  Connected --> Disconnected: stream drops
```
