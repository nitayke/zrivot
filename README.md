# Zrivot Enrichment Pipeline

A Flink 2.1 streaming job that enriches documents arriving from Kafka, with support for both **realtime** and **offline (reflow)** modes.

## Architecture Overview

```
                          ┌──────────────────────────────────────────────────┐
                          │               REALTIME MODE                      │
                          │                                                  │
  ┌─────────────┐        │  ┌─────────┐   keyBy(docId)   ┌──────────────┐  │
  │ Raw Kafka   │────────▶│  │ Source  │ ───────────────▶ │ Boomerang    │  │
  │ Topic       │        │  │ (per CG)│                   │ Enrichment   │──┼──┐
  └─────────────┘        │  └─────────┘                   └──────────────┘  │  │
                          └──────────────────────────────────────────────────┘  │
                                                                               │
                          ┌──────────────────────────────────────────────────┐  │  ┌──────────┐   ┌──────────────┐
                          │              REFLOW (per enricher)               │  ├─▶│  Joiner  │──▶│ Kafka Output │
                          │                                                  │  │  │ (keyBy   │   │    Topic     │
  ┌─────────────┐        │  ┌───────┐   ┌───────────┐   ┌──────────────┐   │  │  │  docId)  │   └──────────────┘
  │ Reflow      │────────▶│  │ Count │──▶│  ES Fetch │──▶│ Boomerang    │───┼──┘  └──────────┘
  │ Kafka Topic │        │  │ +Slice│   │ (keyBy    │   │ Enrichment   │   │
  └─────────────┘        │  └───────┘   │  sliceId) │   └──────────────┘   │
                          │              └───────────┘                       │
                          └──────────────────────────────────────────────────┘
```

## Key Design Principles

### SOLID & Clean Code
- **Single Responsibility**: Each class has one job (e.g., `BoomerangEnrichmentFunction` only handles enrichment with update-count guard)
- **Open/Closed**: New enrichers are added via configuration + implementing the `Enricher` interface — no pipeline code changes needed
- **Liskov Substitution**: All enrichers implement a common `Enricher` contract
- **Interface Segregation**: `Enricher` interface is minimal (init, enrich, close)
- **Dependency Inversion**: Pipeline builders depend on abstractions (`Enricher`), not concrete implementations

### Fault Isolation
- Each enricher runs independently with its own Kafka consumer group
- If one enricher's API fails, it emits a *failure result* — other enrichers are NOT affected
- The joiner collects whatever enrichments are available (success or failure) and emits after timeout
- Realtime Kafka failures → consumer group lag (retried on next poll)
- Reflow ES failures → Flink retries the operator (data is safe in ES)

### Boomerang Update Count
- Keyed state per `(documentId, enricherName)` tracks the latest `boomerangUpdateCount`
- Stale events (lower count than state) are dropped before enrichment executes
- Prevents re-processing outdated versions of a document

## Pipeline Modes

### REALTIME
Reads raw documents from Kafka **and** runs reflow for all enrichers. Both streams feed into the same joiner.

### OFFLINE
Only runs the reflow pipelines (no Kafka raw input). Used for batch re-enrichment.

## Configuration

All settings are in `pipeline-config.yaml`:

```yaml
pipeline:
  mode: REALTIME          # or OFFLINE

kafka:
  bootstrapServers: localhost:9092
  rawTopic: raw-documents
  outputTopic: enriched-documents

elasticsearch:
  hosts:
    - http://localhost:9200
  index: enriched-documents

joiner:
  timeoutMs: 30000

reflow:
  sliceThreshold: 10000   # docs above this count get sliced
  maxSlices: 10
  fetchBatchSize: 1000

enrichers:
  - name: geo-enricher
    consumerGroup: geo-enricher-group
    reflowTopic: reflow-geo
    reflowConsumerGroup: reflow-geo-group
    className: com.zrivot.enrichment.ApiEnricher
    properties:
      apiUrl: http://geo-service:8080/enrich
      timeoutMs: "5000"
```

### Adding a New Enricher

1. Implement the `Enricher` interface (or reuse `ApiEnricher`)
2. Add a new entry under `enrichers:` in the config YAML
3. Deploy — no code changes to the pipeline itself

## Reflow Mechanics

Each enricher has a dedicated reflow Kafka topic. A reflow message describes "what changed":

```json
{
  "enricherName": "geo-enricher",
  "queryCriteria": { "match": { "country": "IL" } },
  "timestamp": 1740000000000
}
```

The pipeline:
1. Counts matching documents in Elasticsearch
2. If too many → slices the query into N parallel slices
3. `keyBy(sliceId)` distributes fetch work across task slots
4. Fetches documents from ES per slice
5. Sends each document through the boomerang-guarded enricher

## Building & Running

```bash
mvn clean package -DskipTests
flink run target/zrivot-enrichment-pipeline-1.0.0-SNAPSHOT.jar [config-path]
```

## Project Structure

```
src/main/java/com/zrivot/
├── ZrivotJob.java                          # Entry point
├── config/
│   ├── PipelineConfig.java                 # Top-level YAML config loader
│   ├── PipelineMode.java                   # REALTIME / OFFLINE enum
│   ├── EnricherConfig.java                 # Per-enricher configuration
│   ├── ElasticsearchConfig.java            # ES connection settings
│   └── ReflowConfig.java                   # Reflow tuning parameters
├── model/
│   ├── RawDocument.java                    # Input document model
│   ├── EnrichmentResult.java               # Per-enricher output (success/failure)
│   ├── EnrichedDocument.java               # Fully enriched output document
│   ├── ReflowMessage.java                  # "What changed" message
│   └── ReflowSlice.java                    # ES query slice for parallel fetch
├── enrichment/
│   ├── Enricher.java                       # Enricher interface (SPI)
│   ├── EnricherFactory.java                # Reflective enricher creation
│   ├── ApiEnricher.java                    # HTTP API enricher implementation
│   └── BoomerangEnrichmentFunction.java    # Flink operator with update-count guard
├── joiner/
│   └── EnrichmentJoinerFunction.java       # Collects all enrichments per document
├── reflow/
│   ├── ReflowCountAndSliceFunction.java    # Counts ES docs, creates slices
│   └── ReflowDocumentFetchFunction.java    # Fetches docs from ES per slice
├── pipeline/
│   ├── PipelineOrchestrator.java           # Wires everything together
│   ├── RealtimePipelineBuilder.java        # Builds per-enricher realtime sub-pipeline
│   └── ReflowPipelineBuilder.java          # Builds per-enricher reflow sub-pipeline
├── kafka/
│   ├── KafkaSourceFactory.java             # Creates typed Kafka sources
│   └── KafkaSinkFactory.java               # Creates the output Kafka sink
└── serde/
    ├── JsonDeserializationSchema.java       # Generic JSON deserialiser
    └── EnrichedDocumentSerializationSchema.java  # Output serialiser
```
