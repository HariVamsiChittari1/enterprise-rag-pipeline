# ADR 0001: Audio transcription via async Batch (replacing inline Fast)

Status: Accepted (design) — implementation pending
Date: 2026-09-22
Supersedes: the Fast-transcription writer choice in `docs/AUDIO_RAG_PROPOSAL.md` (audio only)

## Context

The deployed audio writer calls Azure Speech **Fast transcription** synchronously inside a
Durable Functions `process_document_activity`. In the `rg-rag-aca-e2e-20260827` environment
audio documents consistently freeze at `status=processing/stage=acl` and never persist chunks.

Evidence (this environment, App Insights + metrics + live repro):
- Transcription itself works: `POST .../speechtotext/transcriptions:transcribe → 200 OK` ×2.
- Not embedding throttle: raising the shared `text-embedding-3-large` TPM 30→150K changed nothing.
- Not an ongoing OpenAI throttle: the account is idle (0 requests) when ingestion is stopped.
- Not OOM: Function peak memory 1069 MB of 2048 MB.
- Not a normal scale-in: Flex Consumption gives in-flight executions a **60-minute grace during
  scale-in** (Microsoft Learn), so a 20–30 s activity would survive.
- Live repro captured the **Python worker process recycling ~40 s after start, inside the audio
  activity window** — killing the durable activity before it writes a terminal state, with no
  exception (worker process death, not a caught error).

Root cause: the **long synchronous Fast-transcription call blocks the Function worker**, and the
worker is recycled mid-call, interrupting the activity. Microsoft's guidance: Fast transcription is
for "short, interactive, predictable-latency" clips; **Batch transcription** (async submit → poll →
retrieve) is the recommended pattern for audio in storage. (Residual evidence gap: the exact
worker-exit trigger is not captured — Flex has no SCM logstream and App Insights samples the audio
activity — but the disproven alternatives + the recycle-during-blocking-call correlation are
sufficient to choose the async pattern.)

## Decision

For audio only, replace the inline Fast call with **async Batch transcription**, decoupled into
two failure-isolated steps on the existing Functions host:

1. **Submit** (fast, non-blocking): download the SharePoint audio (existing step), upload it to a
   private **audio-staging** blob container, `POST transcriptions:submit` (contentUrls = staging
   blob, `locale`, `wordLevelTimestampsEnabled=true`, `timeToLiveHours=48`; **no**
   `destinationContainerUrl`), record the transcription `self` URI, set the document lifecycle
   state to `awaiting_transcription`, return.
2. **Retrieve** (timer-driven poller, ~every 5 min): for each `awaiting_transcription` doc, `GET`
   the transcription URI; on `Succeeded` call `GET {self}/files`, download the `Transcription`
   result JSON from its `contentUrl` (a Microsoft-hosted, SAS-embedded, temporary URL — no storage
   of ours), build the transcript, chunk, embed, write chunks, mark `ready`, then delete the batch
   job + staging blob; on `Failed`/expired, mark `failed`.

Text/Office documents keep the current synchronous path — unchanged.

## Contracts / boundaries (Software Decoupling Standard)

- **Speech ⇄ Storage (source only):** Batch requires the source audio in Blob storage that Speech
  can reach. Per Microsoft Learn, storage firewall (incl. resource-instance rules) governs the
  account's **public endpoint**, so `publicNetworkAccess=Disabled` makes the account unreachable by
  Speech. The shared Functions/Durable account is `Disabled` and stays that way. We therefore add a
  **dedicated `audio-staging` storage account** (decision S2): `publicNetworkAccess=Enabled` +
  `defaultAction=Deny` + a **resource-instance rule** for the Speech account (`Microsoft.Cognitive
  Services/accounts`), `allowSharedKeyAccess=false`, `allowBlobPublicAccess=false`. Speech reads it
  via its **system-assigned MI** (`Storage Blob Data Reader`, plain URL, no SAS); the VNet Function
  writes/deletes via a **private endpoint** (`Storage Blob Data Contributor`) — PE traffic is allowed
  regardless of the firewall. Exposure is confined to transient audio; the shared account is
  untouched. Results are **not** written to our storage: we omit `destinationContainerUrl` (which
  per Microsoft Learn requires an ad-hoc SAS *and* a fully externally-open destination account —
  incompatible with this posture; the only MI/no-SAS destination is BYOS, deferred). Results land in
  the **Microsoft-managed** container and are read via the files API `contentUrl`.
- **Function ⇄ Speech:** batch REST over the existing Speech **private endpoint** (custom domain),
  MI token `cognitiveservices.azure.com/.default`, existing `Cognitive Services Speech User` role.
- **Submit vs Retrieve:** separate activities/triggers; the long transcription runs in the Speech
  service, never blocking a Function worker → removes the recycle failure mode.
- **Idempotency:** submit is keyed by document version (dedupe on re-run); retrieve is safe to
  re-poll; job + staging blob deleted after successful chunk write.
- **Manifest/chunk contract:** unchanged (schemaVersion 1; audio chunks as today). Only the
  *timing/ownership* of transcription changes.

## Alternatives considered

- **B — harden the sync Fast path** (checkpoint transcript after the call; reap `processing`-stuck
  docs). Treats the symptom, keeps the blocking call and the recycle risk. Rejected as the primary
  fix; the recovery-gap parts are still worth doing.
- **Container Apps Job for audio.** Valid (Microsoft's long-running-job host) but adds a new compute
  surface + queue; larger blast radius than reusing Functions with Batch. Deferred.
- **AKS.** Rejected — Microsoft explicitly steers to Container Apps unless direct Kubernetes APIs
  are required.

## Consequences

- Removes the worker-recycle failure mode; aligns with Microsoft bulk-audio guidance.
- Adds: a dedicated **audio-staging storage account** (public endpoint Enabled but Deny-gated to
  the Speech resource instance only) + its blob private endpoint, Speech-MI `Storage Blob Data
  Reader` + Function-MI `Storage Blob Data Contributor` on it, a submit/poll state
  (`awaiting_transcription`), and a retrieval timer. No results container/RBAC (results use the
  Microsoft-managed container + files API). Higher per-file latency (minutes) but reliable and
  non-blocking.
- Open risk: the result `contentUrl` is a public `*.blob.core.windows.net` URL, so the
  VNet-integrated Function needs outbound egress to reach it — verify during Phase 3/e2e.
- **Run finalization:** a full-sync run finalizes while its audio docs are still parked at
  `stage=TRANSCRIBING`; the poll timer owns their transition to `ready`. `compute_run_counters`
  classifies these as a distinct `RunCounters.transcribing` count (excluded from `processing`), so
  they do not trip the "cannot finalize while documents are nonterminal" guard. Without this, a
  small or audio-only run finishes before the 5-minute poll timer and finalization fails, leaving
  the run at `status=RUNNING` and blocking both full-sync and delta/acl-resync. Residual: a new
  full-sync may start while a prior run's audio still transcribes; re-discovery of the not-yet-ready
  file is reconciled by the existing duplicate-version repair.
- Config: `AUDIO_TRANSCRIPTION_PROVIDER` gains `speech_batch`; new staging/results/TTL/poll settings.

## Implementation phases (see tracker `.copilot-tasks/audio-batch-transcription.md`)

1. `speech_batch.py` adapter (submit/get/download-result/delete), injectable transport + unit tests;
   config additions. No infra/deploy.
2. Infra: dedicated audio-staging storage account (Enabled + Deny + resource-instance rule for
   Speech) + its blob private endpoint, Speech-MI `Storage Blob Data Reader` + Function-MI `Storage
   Blob Data Contributor` on it, app settings; Bicep + contract tests. (No results container/RBAC.)
3. `services.py` audio split (submit → `awaiting_transcription`) + retrieval timer/orchestration +
   tests.
4. Validate (tests) → deploy → e2e (audio docs reach `ready`, chunks retrievable, retrieval cites
   the audio source).

## Validation

Audio docs reach `ready` with `writtenChunkCount>0` and `isRetrievable=true`; a retrieval query
cites the audio source. Rollback trigger: docs still freeze after decoupling ⇒ the recycle is
unrelated to the blocking call (reopen the worker-exit evidence gap with non-sampled logging).
