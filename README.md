# Enterprise RAG Pipeline

Secure, ACL-trimmed RAG system that ingests Markdown, PDF, DOCX, PPTX, and XLSX — plus optional WAV, MP3, and FLAC audio (transcribed via Azure Speech batch) — from one SharePoint document library and serves grounded answers with per-document security trimming. Ingestion preserves native source identity, adds required visual descriptions, and then chunks, enriches, and embeds canonical content into Cosmos DB. Retrieval uses an LLM query planner to route one planned query through the standard path and two or more planned queries through an Agent Framework path with automatic fallback.

## Architecture

- **Ingestion:** Azure Functions (Flex Consumption) with Durable Functions orchestration
- **Retrieval:** Hybrid RAG (standard + agentic) on Azure Container Apps with automatic routing
- **Durable Backend:** Durable Task Scheduler (fresh instance ID per run, tracked via Cosmos)
- **Storage:** Cosmos DB NoSQL (ingestion-runs, source-documents, search-chunks, retrieval-config, service-audit)
- **AI Services:** Document Intelligence, optional Content Understanding rollback, Azure AI Language, Azure AI Speech (optional audio transcription), Azure OpenAI
- **Auth:** Managed Identity (Azure services) + Certificate credential (Microsoft Graph)
- **Networking:** VNet-integrated with Private Endpoints

For full Azure resource SKUs, RBAC roles, and network configuration, see [docs/AZURE_RESOURCES.md](docs/AZURE_RESOURCES.md).

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed diagrams and data model.

Browse the [documentation index](docs/README.md) for setup, API, configuration, operations, evaluation, and troubleshooting guidance.

## How It Works

**Ingestion (full-sync):**

```text
POST /api/ingestion/full-sync → HTTP 202 + status polling URL
Orchestrator: activate → discover → [fan-out in waves] → finalize
Per-document: ACL verify → Download → Extract native semantics and required visuals → Chunk → Enrich → Embed → write ineligible chunks → admit generation → READY
```

**Incremental sync:** Microsoft Graph webhooks push change notifications. A daily reconciliation timer runs delta queries as a safety net, weekly ACL resync re-verifies permissions, and a 10-minute lifecycle reconciliation repairs interrupted transitions, duplicate ready versions, and orphan chunks.

**Retrieval:** `POST /api/query` → query planning → embed → ACL-filtered Cosmos search → LLM answer generation with `[S#]` citations.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed sequence diagrams.

## Configuration

Use [docs/CONFIGURATION.md](docs/CONFIGURATION.md) as the single reference for deployment inputs, generated runtime settings, defaults, accepted values, and secret handling. Retrieval catalog properties are defined in the [catalog property reference](docs/API_REFERENCE.md#catalog-property-reference).

## Query API

Use [tools/script_query_retrieval.py](tools/script_query_retrieval.py) for an interactive deployed query. See [docs/API_REFERENCE.md](docs/API_REFERENCE.md) for the complete HTTP contract, authentication, request and response schemas, limits, and error codes.

## Idempotent Re-Runs

Full sync skips a prior-ready file when its source eTag and available modification timestamp are unchanged. Use `retry-failed` for failed documents instead of re-running full sync. Delta failures retain the previous cursor and are replayed by a later tick. See [ARCHITECTURE.md](docs/ARCHITECTURE.md#idempotent-re-runs-skip-if-ready) for details.

## Security

See the [architecture security model and known limitations](docs/ARCHITECTURE.md#security-model), the [Azure identity and network inventory](docs/AZURE_RESOURCES.md), and the [production security gates](docs/PRODUCTION_READINESS.md#security-gaps-and-boundaries).

## Local Development

```powershell
python -m venv .venv-py312
.\.venv-py312\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## Deployment

Use [docs/AZURE_SETUP.md](docs/AZURE_SETUP.md) for the guarded deployment procedure and [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md) for post-deployment verification. The executable authority is [scripts/deploy.ps1](scripts/deploy.ps1).

## Project Structure

```text
├── azure.yaml             # azd service definitions
├── pyproject.toml         # Python project metadata
├── requirements-dev.txt   # Dev dependencies (pytest, jsonschema)
├── app/
│   ├── function_app.py    # DFApp: endpoints + orchestrators + activities
│   ├── config.py          # Environment configuration
│   ├── host.json          # Function timeout (30 min)
│   ├── requirements.txt   # Runtime dependencies
│   ├── ingestion/
│   │   ├── models.py      # Domain models + schema v1 contracts
│   │   ├── services.py    # Business logic: activate, discover, process, finalize, delta-sync, ACL resync
│   │   ├── repository.py  # Cosmos persistence + retry
│   │   ├── lifecycle_repository.py  # Document lifecycle: retire, ACL refresh, delta cursor
│   │   ├── source_connector.py      # SharePoint connector protocol + implementation
│   │   ├── graph.py       # Microsoft Graph: discovery, ACL, download, delta
│   │   ├── extraction.py  # Document Intelligence and Content Understanding strategies
│   │   ├── chunking.py    # Token-based page-aware chunking
│   │   ├── enrichment.py  # Language AI: key phrases, entities, summary
│   │   ├── embedding.py   # OpenAI embedding
│   │   ├── telemetry.py   # Ingestion audit to Cosmos
│   │   ├── subscription.py # Graph webhook subscription lifecycle
│   │   └── errors.py      # TerminalDocumentError, StaleFenceError
│   └── retrieval/
│       ├── agent.py       # Agent Framework agent factory
│       ├── auth.py        # EasyAuth + GraphGroupResolver
│       ├── config.py      # Retrieval configuration
│       ├── cosmos.py      # SecureCosmosRetriever (ACL-filtered queries)
│       ├── cosmos_registry.py # Multi-instance Cosmos fan-out
│       ├── main.py        # FastAPI app with hybrid routing
│       ├── service.py     # RagService (plan → sanitize → retrieve → generate)
│       ├── tools.py       # Agent Framework search tool (multi-instance)
│       ├── telemetry.py   # LLM audit to Cosmos
│       ├── Dockerfile     # Retrieval container image
│       └── kubernetes/    # Retained inactive AKS manifest
├── infra/                 # Bicep modules (Cosmos, Functions, VNet, PEs)
├── evaluation/            # Evaluation schemas (ground-truth, experiments)
├── tests/                 # Unit tests (ingestion, retrieval, infra)
├── docs/                  # Architecture, API, configuration, setup, readiness, demo, and infrastructure guides
└── data/                  # Policy PDFs; generated Cosmos exports are ignored
```
