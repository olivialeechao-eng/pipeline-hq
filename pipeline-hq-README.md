# pipeline-hq

> A team of AI agents that continuously engineers a data pipeline. Humans govern. Agents build.

## The Problem

DE teams spend the majority of their time on reactive maintenance — fixing broken pipelines, investigating incidents, chasing schema drift. The obvious fix is agents. The non-obvious problem is that agents don't know when they're wrong, and most teams have no infrastructure to tell them.

`pipeline-hq` is the governance layer that makes autonomous DE agents actually trustworthy in production.

## What This Is

A multi-agent system where AI agents function as a DE team — detecting issues in a live pipeline, generating tickets, routing work, and engineering improvements autonomously. Humans are the engineering manager: reviewing PRs in GitHub, leaving comments that become training signal, and monitoring agent performance over time.

Agents run. Humans govern. The pipeline keeps improving.

## The Thesis

The DE role is shifting. Less building pipelines, more governing the systems that manage them.

Agents cannot evaluate their own performance. They don't know what they don't know. A bug agent can write a fix, open a PR, and mark the ticket resolved — while the root cause is still there because business logic changed last quarter. PR merge rate and time-to-merge are the only honest ground truth.

Humans are not a temporary scaffold until agents get smarter. They are a permanent and essential component of any honest agentic system. Not as an approval gate — as a governance layer. The system runs without them. Their PR reviews make it better.

The pipeline is untouched by human hands. Every change — whether surfaced by a detector or proposed by a human — flows through the same agent path: ticket → specialist → PR → human review → merge. Humans propose via ticket. Agents implement. The separation is absolute.

The current baseline was created as a bootstrap — a practical starting state given that a pipeline needed to exist before the agent system could run. The logical endpoint of this architecture is a Bootstrap Agent that generates the pipeline itself from a human-authored requirements spec. At that point the governance model applies from the very first commit. Humans write specs. Agents write code. The PR is the only path to production, always.

`pipeline-hq` is what that governance infrastructure looks like.

If you understand data governance, agent governance is the same concept applied to a different domain. Data governance asks: who's allowed to access data, who owns it, how is quality enforced, how do you know when it degrades. Agent governance asks the same questions about a different subject: who's allowed to change the pipeline, who reviews the output, how is quality enforced, how do you know when an agent degrades. The problems are identical — ownership, quality enforcement, degradation detection, and audit trails. The subject shifts from data assets to agent outputs.

The two aren't separate concerns either — agents are actively doing data governance work. The Pipeline Detector watches the stream for anomalies, schema drift, and quality issues and surfaces them as tickets. But agents belong on the detection side of data governance, not the enforcement side. Detection is pattern matching — well-defined, repeatable, safe to automate. Enforcement touches data contracts, compliance requirements, and downstream consumers where a wrong autonomous decision has real blast radius. That stays on the same path as everything else: ticket → specialist → PR → human review → merge. Agents surface the issue. Humans approve the fix.

## What Humans Actually Do

Humans do three things — exactly three:

1. **Review and merge (or reject) agent PRs in GitHub** — async, no blocking, normal engineering review
2. **Submit ticket ideas** — via a Streamlit input panel embedded in the governance dashboard. Plain English description goes in, a Prefect task runs the LLM formatting call, the human confirms type, priority, and scope before it enters the Router. From that point the path is identical — same Router, same specialists, same PR output. Fully instrumented: the formatting call is traced via Phoenix, the ticket is written to Iceberg like everything else.
3. **Monitor the governance dashboard** — read-only view of agent performance in Streamlit

Humans do not write pipeline code. They do not open PRs. They do not hotfix the baseline. If a human sees a pipeline problem, they submit a ticket. An agent fixes it.

## Architecture

```
PySpark streaming pipeline (fake credit card data + synthetic enrichment)
↓
Agent Detector ──────────────────────────────────────────────────────┐
Pipeline Detector — watches the stream, surfaces raw events          │
│                                                                     │
├── [autonomous] ────────────────────────────────────────────────────┤
│                                                                     ↓
└── [human ticket idea] ──────────────────────────── Ticket Generator → Jira-schema ticket
                                                               ↓
                                               Router — dispatches to correct specialist
                                                               ↓
                                   ┌──[Bug]────→ Bug Agent ────→ PR
                                   ├──[Task]───→ Task Agent ───→ PR
                                   ├──[Spike]──→ Spike Agent ──→ PR
                                   └──[Story]──→ Story Agent ──→ Router (back-edge)
                                                               ↓
                                          Human reviews PR in GitHub
                                                               ↓
                              Prefect flow polls GitHub API on PR close
                                                               ↓
                          PR metrics written to Iceberg (source of truth)
                                                               ↓
                    LangGraph checkpointer → Neon (Postgres, dedicated)
                                                               ↓
                  arize-phoenix SDK traces every agent call → Phoenix Cloud
                                                               ↓
                   Streamlit reads Iceberg tables — read-only governance dashboard
```

## The Agent Team

| Agent | Input | Output | Responsibility |
|-------|-------|--------|----------------|
| Bootstrap Agent *(future)* | Human requirements spec | Baseline pipeline PR | Generates the initial pipeline from a spec, seeded with best practices |
| Pipeline Detector | PySpark stream | Raw event | Watches the data stream, surfaces anomalies, bugs, and optimization opportunities |
| Agent Detector | Phoenix Cloud traces + Iceberg PR analytics | Raw event | Monitors agent behavior, surfaces regressions, latency degradation, routing errors |
| Ticket Generator | Raw event or human ticket idea | Jira-schema ticket | Formats the input into a structured ticket with type, priority, description |
| Router | Ticket | Specialist dispatch | Reads ticket type → routes to correct specialist. Only agent that knows about specialists. |
| Bug Agent | Bug ticket | GitHub PR | Investigates root cause, writes fix, verifies pipeline runs cleanly, opens PR |
| Task Agent | Task ticket | GitHub PR | Implements discrete change, verifies pipeline runs cleanly, opens PR |
| Spike Agent | Spike ticket | GitHub PR | Researches architectural question, writes design doc, opens PR |
| Story Agent | Story ticket | Sub-tickets → Router | Breaks large work into sub-tasks, feeds each back into the Router |

**Single responsibility is intentional.** Every agent has one input, one output, one job. The Router is the only agent that knows about the others.

**Two detectors, two domains.** The Pipeline Detector watches the data stream. The Agent Detector watches the agent system itself — reading from Phoenix Cloud traces and Iceberg PR analytics to surface regressions before a human has to reject a PR to discover them. Both feed the same Ticket Generator. Same path, same governance.

**The Ticket Generator is the single entry point for all work** — autonomous events from both detectors and ideas submitted by humans produce the same Jira-schema ticket and flow through the same Router. There is no separate human fast-path.

## The LangGraph Graph

```
Pipeline Detector → Ticket Generator → Router
Agent Detector    → Ticket Generator ↗
Human Ticket Idea → Ticket Generator ↗
                                          ├──[Bug]────→ Bug Agent ────→ PR
                                          ├──[Task]───→ Task Agent ───→ PR
                                          ├──[Spike]──→ Spike Agent ──→ PR
                                          └──[Story]──→ Story Agent ──→ Router (back-edge)
```

Every edge is deterministic except at the Router — the only conditional node. The Story Agent is the only node with a back-edge: it decomposes large work into sub-tickets and feeds them back through the same Router. No special cases. No new routing logic needed.

## Pipeline Verification and CI

Verification lives in GitHub Actions CI, not in the agent.

The agent's graph is:

```
load_relevant_files → plan_changes → implement_changes → push_to_branch → open_pr → get_human_approval
        ↑                                                                                     |
        └──────────────────────────────── CHANGES_REQUESTED ──────────────────────────────────┘
```

The agent pushes its changes to a branch, opens a PR, and waits. GitHub Actions runs the pipeline on PR open and reports pass/fail as a CI check. The human reviews code and CI result together — the same workflow they'd use reviewing any junior engineer's PR.

**Why CI failure routes through the human, not back to the agent automatically:**

The instinct is to have the agent auto-retry on CI failure — detect the red check, read the logs, fix the issue, push again, repeat until green. This looks more autonomous. It produces a worse system.

When the agent silently retries, the retries are hidden from the governance layer specifically — not from the world in general. Application logs could capture every attempt, but the Agent Detector doesn't read logs. It reads two things: the Iceberg PR metrics table (`merged`, `time_to_merge_hrs`, `commits_after_first_review`, `ci_failed_at_any_point`, computed once on PR close) and Phoenix traces of LLM calls. If the agent retries before opening the PR, none of that activity reaches either system — the PR that finally opens looks like a clean one-shot success, indistinguishable in the metrics from an agent that got it right the first time. Turning retry-count-in-logs into a governance signal would mean building a second pipeline to mine, parse, and aggregate logs into something Iceberg-shaped — at which point you've largely rebuilt what a CI check gives you for free, since a CI check is already a structured PR-level event that slots into the existing schema with zero extra plumbing.

When the human is in the loop on CI failures, every failure is visible and attributable using the metrics pipeline that already exists. The human sees a red CI check, glances at the logs, leaves a comment — "fix the import error on line 12" — and moves on. The agent picks it up in the existing `CHANGES_REQUESTED` rework loop, fixes it, pushes again, CI reruns. The comment becomes training signal. The Agent Detector can read "this agent has CI failures on 40% of its PRs over the last two weeks" and generate a ticket. That's the honest ground truth, without needing a log-mining layer to get there.

The human work is minimal — spot the failure, leave one comment. The agent does the actual fixing. But those touchpoints are exactly what makes the governance layer meaningful rather than decorative. More human touchpoints up front, better signal over time.

**This is a Phase 1 decision, not a permanent one.** CI-only verification is the simplest thing that gets an honest signal into Iceberg with no new infrastructure. If it turns out the human touchpoints are too frequent in practice, or the project wants agents to self-heal on trivial CI failures (lint errors, import typos) without waiting on a human, that's a Phase 2 addition: either structured retry logging that feeds its own Iceberg table (so retries become a first-class governance signal instead of noise), or a bounded agent-side auto-retry limited to a narrow class of failures, with every retry still traced through Phoenix so it isn't actually hidden. Either path is additive to the CI check, not a replacement for it — CI stays the source of truth for pass/fail either way.

**A high CI failure rate is a signal, not an expected cost of doing business.** The design assumes agents *should* succeed most of the time — a well-scoped Bug or Task ticket, given the current pipeline code, should reasonably produce a PR that passes CI on the first try. But that's an assumption, not a measured baseline yet. Phase 1 is deliberately the observation period: run the agents, let CI failures and PR outcomes accumulate in Iceberg, and see what the actual failure rate looks like before deciding anything needs to change. If it turns out agents are failing CI often, that's useful information on its own — it says something is off (underspecified tickets, stale or missing agent context, a model that isn't strong enough for the task) rather than being a reason to reach straight for Phase 2 tooling like retry logging or agent self-retry. Build the enhancement once the data says it's warranted, not preemptively. That's also the Agent Detector's job once it's live — read the accumulated Iceberg metrics and Phoenix traces, and generate a ticket if the failure rate for a given agent looks like a regression rather than noise.

**The Spark concurrency constraint:**

PySpark runs in local mode on a single machine — one JVM, one SparkSession at a time. Two pipeline runs triggered simultaneously would conflict. In practice this isn't a problem: CI runs are isolated per PR, and the pipeline's `finally` block stops Spark cleanly after each run.

This is a local mode constraint, not an architectural one. In production:

- **Databricks** — each run gets its own cluster, fully isolated, no concurrency limit
- **EMR Serverless** — pay-per-second, no cluster to manage, scales to zero between runs, multiple jobs run in parallel natively
- **Modal** — generous free tier, pay-per-second, PySpark runs in isolated containers

The pipeline code is identical across all three — it's a config change, not a rewrite. `local[*]` in the SparkSession master becomes a cluster URI or is removed entirely for managed services.

## How Agent Context Works

Each specialist runs with the minimum context needed to do its job well. More context isn't always better — irrelevant content dilutes the signal and increases cost and latency.

**Phase 1 context — pipeline code + ticket**

The current pipeline code is fetched directly from GitHub at call time. This is not a retrieval problem — the agent needs the exact current file, not something semantically similar to it. The triggering ticket is already in state. That's sufficient for Phase 1: the agent knows what the pipeline currently does and what it's being asked to change.

```
New ticket arrives
→ GitHub API fetches current pipeline file
→ Specialist runs with: current pipeline code + ticket
```

**What actually makes good context — architecture, not PRs**

The instinct to feed past PR diffs as context is understandable — "show the agent what worked before." But PR diffs are examples of past solutions, not knowledge about how to solve problems. A diff tells the agent what was done. It doesn't tell the agent why the pipeline is structured the way it is, what patterns the codebase follows, or what the human reviewer expects.

What actually helps a specialist produce work that merges cleanly:

- **Architecture docs and design decisions** — why the bronze/silver/gold structure exists, what each layer is responsible for, conventions the codebase follows. This is stable knowledge that doesn't live in any individual PR.
- **Spike agent outputs** — the Spike agent's job is to research architectural questions and write design docs. Those outputs are exactly the kind of "here is why we do it this way" knowledge that helps a Task or Bug agent make better implementation decisions. A knowledge base that accumulates Spike outputs over time is more valuable than one that accumulates PR diffs.

PR diffs have more value for the **Bug agent** than the Task agent — "here is how we diagnosed and fixed a similar issue before" is useful when doing root cause analysis. For implementation work, architectural context is the higher-signal input.

**What stays out of context**

Raw PR comments are not retrieved. Most review comments are too local — "rename this variable," "rebase on main" — and the noise outweighs the signal. The structured PR metrics (merge outcome, time to merge, CI result) are the signal for the governance layer, not for agent context.

**If corpus volume grows — RAG and when it earns its place**

Once a meaningful body of Spike outputs and architecture docs has accumulated, a vector index over that knowledge base earns its place: pre-embed the corpus, retrieve the most relevant sections, pass them to the specialist alongside the pipeline code and ticket. The Iceberg tables remain the source of truth; the vector index is compute that sits on top of them, rebuildable any time. Qdrant is in the stack for this future state.

The crossover point is practical, not theoretical: when the knowledge base is large enough that passing it all in a single prompt stops being cheap and fast, retrieval is the right call. For a small, stable corpus — ten architecture docs — stuffing the full context is simpler. For a growing corpus of Spike outputs accumulated over months, you retrieve the most relevant sections instead.

**Why RAG is still useful even with large context windows**

Modern frontier models have large context windows — 128K to 1M tokens depending on the model. The "my docs don't fit" problem is mostly solved. But that's not the main reason to use RAG. The reasons that still hold:

- **Cost** — filling a large context window on every agent call is expensive. Retrieving three to five relevant chunks is far cheaper per call.
- **Latency** — large contexts are slower to process. Retrieval keeps the prompt lean.
- **Lost in the middle** — LLMs perform measurably worse when the relevant information is buried deep in a large context. Retrieval surfaces the right content and puts it at the front where the model pays attention to it.
- **Growing knowledge bases** — you can't re-stuff a context window every time a new Spike output is added. A vector index handles updates cleanly.

**RAG vs. hybrid search**

RAG and hybrid search are different concepts that often get conflated. RAG is the overall pattern: retrieve relevant context, then pass it to the LLM so the model generates a grounded answer. It says nothing about how you retrieve — just that you fetch before you generate.

Hybrid search is a specific retrieval method. It combines two signals: dense retrieval (vector/semantic search via embeddings, which finds conceptually similar content even when exact words don't match) and sparse retrieval (keyword/lexical search like BM25, which finds exact term matches). The two are blended — usually via Reciprocal Rank Fusion — because they catch different failure modes. Dense search handles paraphrases and synonyms. Sparse search handles proper nouns, table names, agent identifiers, and exact terminology that embeddings tend to blur.

In this project, hybrid search matters when retrieving Spike outputs that contain specific technical terms — Delta Lake, Iceberg, `process_batch`, `DELTA_GOLD_FLAGGED` — that a pure vector search might miss if the query is phrased differently. Pure semantic search would find conceptually related content; hybrid search would also catch the exact table name.

Qdrant supports hybrid search natively. The implementation adds a sparse vector alongside each dense embedding at index time. At query time, both scores are computed and fused. The Iceberg tables remain the source of truth either way — the retrieval method only affects what gets surfaced, not where the data lives.

**How feedback actually reaches specialists — two update rhythms**

Specialists consume context on two distinct cadences, and conflating them is a common design mistake.

*Per-call context* is always fresh and always exact: the current pipeline file fetched from GitHub, the triggering ticket already in LangGraph state. This happens on every specialist invocation and requires no caching or pre-processing.

*Periodic context* is distilled knowledge that accumulates over time: patterns the Agent Detector has identified across many PRs, conventions the human reviewer consistently enforces, blind spots a particular specialist keeps hitting. This doesn't change per ticket — it changes when enough evidence accumulates to warrant an update.

The right shape for periodic context is a per-agent markdown file committed to the repo — a distilled CLAUDE.md-style document the specialist reads as part of its system context. For example:

```markdown
## Bug Agent — Known Patterns
- Always check upstream joins when fraud_rate spikes in batch aggregations
- bronze→silver transformations expect event_time not raw Time column
- process_batch assumes non-null Amount — add null guard before aggregating
```

This file is not written by hand. The Agent Detector reads PR patterns from Iceberg, identifies recurring issues, and runs an async LLM call to update the file. The specialist never touches Iceberg directly — it just reads the distilled output. The LLM call happens once when a pattern emerges, not on every ticket.

This means the Agent Detector has two related but distinct jobs: generating tickets when regressions are detected, and maintaining the per-agent knowledge files that prevent those regressions from recurring. Both jobs read the same Iceberg PR metrics. The ticket path is reactive — something went wrong. The knowledge file update is proactive — here is what to do differently next time.

The Qdrant retrieval layer sits on top of this same knowledge base once it grows large enough that injecting the whole file stops being cheap. Until that crossover point, the distilled markdown is injected directly. Same data, different delivery mechanism depending on corpus size.

## How PR Metrics Are Collected

The human never leaves GitHub. Normal PR review is the feedback mechanism.

When a PR closes, a Prefect task fires and makes four GitHub API calls:

```
GET /pulls/{pr_number}          → merged, merged_at, created_at, total commits, head SHA
GET /pulls/{pr_number}/reviews  → timestamp of first review
GET /pulls/{pr_number}/commits  → commits after first review timestamp
GET /commits/{sha}/check-runs   → whether CI failed at any point
```

From those four calls, the task derives and writes a single row to the Iceberg PR metrics table:

```
pr_id | ticket_type | agent | merged | merged_at |
time_to_merge_hrs | commits_after_first_review | ci_failed_at_any_point
```

**Why these metrics**

`merged` and `time_to_merge_hrs` are the primary quality signal — did the agent's work ship, and how quickly? A PR that takes three days and multiple review cycles is a different signal than one that merges in an hour.

`commits_after_first_review` is a cleaner rework proxy than formal review state changes. Engineers rarely use `CHANGES_REQUESTED` more than once — they comment inline and let new commits flow through. Counting commits pushed after the first review captures the actual back-and-forth without depending on review state discipline.

`ci_failed_at_any_point` is objective and unambiguous. Either the check run conclusion was `failure` or it wasn't. A pattern of CI failures followed by fix commits across an agent's PRs is a meaningful quality signal — the agent is producing code that doesn't pass tests on the first try.

**What is not stored**

Raw PR comments are not written to Iceberg. The diffs and comments live in GitHub — there is no reason to copy them. The metrics table is narrow by design: computed once on PR close, queryable for dashboard and retrieval filtering, nothing more.

## Data & Persistence

### The Architectural Principle

Every layer of the stack follows the same principle: **raw data lives in open formats on storage you own, compute is swappable, self-hosting is possible on every component where data sensitivity could become a requirement.**

The key distinction is between the **data layer** and the **compute layer**. Lock-in on compute is acceptable — compute is stateless, indexes are rebuildable, engines are swappable. Lock-in on the data layer is where migration cost compounds over time. The stack is designed around that distinction.

A second invariant applies specifically to the agent layer: **every LLM call in the system is traced through Phoenix, without exception.** This is not a best-effort logging policy — it's a hard architectural constraint. Any LLM call that runs outside Phoenix is a blind spot in agent performance monitoring. You can't measure output quality drift, latency degradation, or retrieval quality for calls you can't see. The constraint is what makes the Agent Detector's regressions detectable and what makes the PR metrics attributable to specific agent behavior rather than noise. Any component that would require an LLM call to happen outside Phoenix is, by that fact alone, the wrong component.

This is not a cloud agnostic story — cloud agnostic is an engineering preference without a concrete business trigger. This is specific risk mitigation against scenarios that actually happen as companies grow: compliance requirements blocking a third-party vendor, enterprise customers requiring data inside their VPC, vendor pricing changes, acquisitions creating cross-cloud data problems.

### Storage — Open Format, Owned Data

**Delta Lake for the pipeline layer (bronze/silver/gold)**
The pipeline layer uses Delta Lake — an open table format that separates storage from compute. The storage path is local for this portfolio project and an S3 URI in production. Same architecture, one-line change. Any engine that speaks Delta or Parquet can read it: Spark, Trino, DuckDB, Athena. No warehouse vendor holds the data.

Delta is kept for the pipeline layer because the streaming support is mature and nothing downstream reads those tables except the pipeline itself. Delta and Iceberg are both Parquet underneath with a metadata layer on top — migrating between them is a format swap, not a data migration.

**Apache Iceberg for the operational data layer (PR metrics, agent run records)**
All operational data — PR metrics and agent run records — is stored as Iceberg tables on local disk (S3 in production). Iceberg is Apache-governed, has the broadest multi-engine adoption of any open table format (Snowflake, BigQuery, Redshift, Trino, Flink, Spark all read it natively), and sits on open Parquet files you own completely.

The PR metrics table is narrow and purpose-built — computed once on PR close, never updated, always queryable. DuckDB reads it in-process for the Streamlit dashboard and for pre-filtering candidate PRs before the LLM retrieval step.

**Neon (serverless Postgres) — dedicated to LangGraph checkpointing**
Neon has one job: LangGraph state. LangGraph's native Postgres checkpointer saves graph execution state at every step — fault tolerance, time travel debugging, human-in-the-loop support. Neon IS Postgres, so this is a direct integration with no extra tooling. Scale-to-zero means no cost between agent runs.

Neon is not used as a general data store. PR metrics and agent run records live in Iceberg. The separation is intentional: Postgres combines storage and compute in a single process — you can't swap the query engine and point it at the same files the way you can with Iceberg on object storage. Neon's role is scoped to state that genuinely needs a transactional database, not structured data that benefits from open format portability.

Neon's copy-on-write branching is useful for agent eval — a branch is a full clone of live checkpoint state, created instantly at near-zero cost. Spin one up to test a new agent version against real graph execution history, delete it when done, nothing touches production.

### Compute — Swappable Query Layer

**DuckDB — analytics query engine**
The Streamlit governance dashboard reads Iceberg tables directly via DuckDB. No warehouse, no extra infrastructure — DuckDB reads Parquet natively, runs in-process, and returns results. The same DuckDB query that powers the dashboard also runs pre-filter steps against PR metrics when needed. Swapping to Trino, Athena, or Spark for larger-scale analytics is a query engine change, not a data migration.

**Qdrant — vector query index (future scale)**
Phase 1 context is pipeline code and ticket — no retrieval needed. As the system accumulates Spike agent outputs and architecture docs, a vector index over that knowledge base earns its place: pre-embed the corpus, retrieve the most relevant sections, pass them to the specialist. Qdrant is self-hostable, open source, and runs as a single binary. The index is pure compute — rebuildable any time from source data. It is in the stack for this future state, not the current one.

**arize-phoenix SDK → Phoenix Cloud**
Every agent call is traced via the arize-phoenix instrumentation layer (OpenTelemetry-native). Traces ship to Phoenix Cloud (hosted Phoenix OSS). Swapping to self-hosted Phoenix is a single env var change: `PHOENIX_COLLECTOR_ENDPOINT`. No agent code changes. In a production deployment where traces contain sensitive business logic, self-hosted Phoenix inside a private VPC is the right call — the OTel portability makes that trivial.

### Inference — Provider Agnostic

**OpenAI SDK pointed at Groq by default**
Groq's API is OpenAI-compatible. Swapping to any OpenAI-compatible provider (Fireworks, Cerebras, Together) is an env var change. Anthropic's API is not OpenAI-compatible — it requires the Anthropic SDK directly. A thin wrapper over both SDKs handles provider normalization without a third-party abstraction library. The wrapper is ~200 lines, covers every realistic provider, and has zero supply chain risk — just two provider SDKs maintained by the companies whose APIs they wrap.

## The PR as the Unit of Output

Every specialist produces a PR. Not a report, not a log entry — a PR with real code or a real design doc that a human can review, comment on, and merge or reject.

The PR is:
- The natural async review boundary (no blocking, no approval gates)
- The ground truth signal for agent output quality (merge rate, time to merge, CI failures)
- Familiar to any engineering reviewer ("it works exactly like managing a junior DE")

## What Gets Monitored Per Agent

- **PR merge rate** — what percentage of this agent's PRs does the human actually ship?
- **Time to merge** — how long from PR open to merge? Trending up is a quality signal.
- **Commits after first review** — how much back-and-forth before the human is satisfied?
- **CI failure rate** — is this agent producing code that passes tests on the first try?
- **Output quality drift** — are these metrics getting better or worse over time?
- **Latency** — is this agent slowing down?
- **Routing accuracy** — is the Router dispatching to the right specialist?

No generic observability tool measures these because no generic tool knows what "good output" means for your specific agents. That definition is what this project builds.

## Stack

### Frameworks

| Tool | Purpose | Notes |
|------|---------|-------|
| LangGraph | Agent orchestration + stateful multi-agent workflows | Open source |
| arize-phoenix | AI observability + evaluation instrumentation | Open source (ELv2), OTel-native |
| DeepEval | CI/CD eval gate — per-agent PR quality checks | Open source (Apache 2.0) |
| Prefect | General workflow orchestration + GitHub API polling + PR metrics sync | Open source |
| OpenAI SDK + Anthropic SDK | LLM inference — thin wrapper normalizes both | Provider-backed, no third-party abstraction |

### Infrastructure

| Tool | Purpose | Cost | Self-hosting path |
|------|---------|------|-------------------|
| Phoenix Cloud | Trace collection + eval storage (hosted Phoenix OSS) | Free tier | Single env var → Railway or self-hosted |
| Prefect Cloud | Scheduling + orchestration | Free Hobby tier | Self-hosted Prefect server |
| Neon | LangGraph checkpoints only (serverless Postgres) | Free tier | Any Postgres host — connection string swap |
| Qdrant Cloud | Vector index — future scale optimization | Free Cloud tier | Self-hostable — open source, single binary, no deps |
| Groq | LLM inference hardware — fast open weight model serving | Free tier | Any OpenAI-compatible provider — env var swap |
| PySpark | Streaming pipeline (local mode) | Free | EMR Serverless, Databricks, Modal — config change, same pipeline code |
| DuckDB | Iceberg query engine — dashboard + pre-filter retrieval step | Free | In-process, no infrastructure |
| Streamlit | Read-only governance dashboard | Free Community Cloud | Self-hosted |
| GitHub | PR output + human feedback surface + content source of truth | Free | — |

Everything free. No Docker required.

### Stack Decisions

**CI over agent-side verification**
The instinct is to have the agent verify its own changes before opening a PR — clone the branch, run the pipeline, check the exit code, only open the PR if it passes. This looks more autonomous. It produces a worse system. When the agent silently retries on failure, every failure is hidden — you can't tell if the agent is improving or just retrying until it gets it right. Verification lives in GitHub Actions CI instead. The agent opens the PR, CI runs the pipeline, the human sees code and CI result together. If CI is red, the human leaves a comment and the existing `CHANGES_REQUESTED` rework loop handles it. Every failure is visible, attributable, and becomes training signal for the Agent Detector. More human touchpoints up front, better signal over time.

**Spark concurrency on a single machine**
Local mode Spark runs one SparkSession per JVM. Two simultaneous pipeline runs on the same machine will conflict. In practice this isn't a problem — CI runs are isolated per PR, and the pipeline's `finally` block stops Spark cleanly after each run. When the project scales to multiple concurrent pipelines, the fix is a move to EMR Serverless or Modal where each run is isolated by default.

**Architecture docs and Spike outputs over PR diffs as agent context**
The instinct to use past PR diffs as agent context — "show the agent what worked before" — turns out to be the wrong abstraction. PR diffs are examples of past solutions. What actually helps a specialist produce work that merges cleanly is knowing why the pipeline is structured the way it is, what patterns the codebase follows, and what the architectural decisions behind it were. That knowledge lives in architecture docs and Spike agent outputs, not in diffs.

Phase 1 context is deliberately minimal: current pipeline code and the ticket. Once a body of Spike outputs and architecture docs accumulates, a vector index over that knowledge base is the right next step — not a retrieval system over PR diffs. The crossover point where Qdrant's overhead is justified is when the knowledge base is large enough that you can't pass it all to the LLM in a single call. Until then, retrieve the most relevant sections via similarity search and pass them directly.

**GitHub as content source of truth, Iceberg for metrics only**
PR diffs, comments, and check run logs live in GitHub. There is no reason to copy them into Iceberg — they are fetched at agent call time via the GitHub API when a specialist needs them. What lives in Iceberg is the narrow metrics table: six derived columns computed once on PR close from four GitHub API calls. This keeps the Prefect sync task small, the Iceberg table narrow, and the content layer in the system that owns it.

**Derived metrics over raw event storage**
`time_to_merge_hrs`, `commits_after_first_review`, and `ci_failed_at_any_point` are derived from GitHub API data on PR close and stored directly. Recomputing them from raw events at query time would require joining across multiple endpoints on every dashboard load and every retrieval pre-filter. Computing once and storing is the right call. The derivation logic is simple enough that it doesn't need its own abstraction — it lives in the Prefect task.

**Iceberg for operational data over Postgres (pgvector)**
Postgres combines storage and compute in a single process — the query engine isn't independently swappable from the data files. Iceberg is genuine storage-compute separation: the metrics table is open Parquet files on disk, and any engine reads them. DuckDB today, Trino or Athena tomorrow, no data migration either way. Neon is retained for LangGraph checkpointing where Postgres is genuinely the right tool — graph state needs a transactional database, not open format portability.

**Delta Lake for the pipeline layer, Iceberg for the operational layer**
The pipeline tables (bronze/silver/gold) stay on Delta because Spark streaming support is more mature there and nothing outside the pipeline reads those tables. The operational tables use Iceberg because multiple tools query them and Iceberg has broader engine adoption. Both sit on Parquet. Converting between them is a metadata swap if that ever becomes a reason to standardize.

**Qdrant Cloud over Pinecone**
Pinecone is managed-only with no self-hosting path, and closed source. No clean answer to a compliance or enterprise VPC requirement. Qdrant is fully open source, self-hostable as a single binary. When the vector index is needed, the index is pure compute — built from Iceberg-stored source data, rebuildable any time.

**Neon over Supabase**
Neon's scale-to-zero maps to the bursty agent run pattern — no cost between runs. Neon's copy-on-write branching provisions a full clone of live checkpoint state, not an empty schema. Both properties are the right fit for LangGraph checkpointing. Supabase's continuous compute and schema-only branching are the wrong fit for both.

**Thin inference wrapper over LiteLLM**
LiteLLM solves a real problem but the attack surface of wrapping fifty providers is large, BerriAI had a security incident in their hosted proxy, and the realistic provider set is three: Groq (default), any OpenAI-compatible provider, and Anthropic. A ~200-line wrapper over the OpenAI and Anthropic SDKs covers all three with zero third-party abstraction in the critical path. The inference layer is security-sensitive — agents are opening PRs on production pipelines.

**Query engine portability — what "swappable compute" actually means**
DuckDB uses standard SQL. The queries written against the Iceberg metrics table — filtering by agent and ticket type, computing aggregates for the dashboard — transfer directly to Athena, BigQuery, Trino, or Spark with essentially no changes to the SQL itself.

What is not portable is the catalog and registration layer. DuckDB uses `iceberg_scan('path/to/table')`. Athena uses Glue as the catalog. BigQuery uses BigLake external tables. The migration cost lives in catalog setup, not in a data migration — you're registering table paths, not moving data. To keep that cost low: write plain SQL with no DuckDB-specific functions, use parameterized table paths rather than hardcoded `iceberg_scan()` calls.

**arize-phoenix over Langfuse**
Langfuse v3 self-hosting requires ClickHouse, Redis, and S3 in addition to Postgres. Phoenix's RAG evaluation primitives and OTel-native instrumentation are a better fit. Swapping the Phoenix backend is a single env var change.

**Human ticket submission via Streamlit, not a Claude Skill**

The original design used a Claude Skill as the human ticket entry point — plain English in, Jira-schema ticket out, posted directly to Prefect via a conversational interface. The skill had real appeal: typed Pydantic output with no schema stuffed into a prompt, conversational pre-approval to catch misclassified tickets before they entered the Router, natural language input so a human never had to know what `issuetype` looks like.

It was replaced for two reasons, and the first is architectural: every LLM call in this system is traced through Phoenix. That is not a preference — it is a hard constraint. The Agent Detector reads Phoenix traces to surface regressions. The PR metrics are only attributable to specific agent behavior if you can see the calls that produced them. A call that runs inside claude.ai is invisible to Phoenix. That alone disqualifies the Claude Skill regardless of volume.

The second reason is volume. Human tickets in this system are not occasional — new features, new products, new pipelines all originate as human tickets, making the human path a primary input source alongside the detectors. At that volume, the formatting call is the one place where output quality degradation would be hardest to detect and easiest to misattribute. A pattern of human-submitted tickets producing more specialist rework could mean the skill is misclassifying ticket types, or it could mean humans just submit harder work. Without a Phoenix trace on the formatting call, there is no way to tell.

The fix is to move human ticket submission into the Streamlit governance dashboard — a plain English input field, a Prefect task that runs the LLM formatting call, a confirmation step where the human verifies type, priority, and scope before the ticket enters the Router. Same natural language input. Same pre-approval quality gate. The formatting call is now a traced Prefect task, the confirmed ticket is written to Iceberg with a `source: human` field, and human-ticket PR outcomes are directly comparable to detector-ticket outcomes in the same metrics table.

The Streamlit dashboard goes from read-only to having one write surface — and that surface is the right place for it. The human is already there monitoring agent performance. Submitting a ticket is a natural next action from the same view.

### Tools Evaluated and Not Used

**pgvector as source of truth for feedback data** — Postgres combines storage and compute in a single process. Replaced by Iceberg tables (source of truth) + GitHub API (content). Neon is retained for LangGraph checkpointing where Postgres is the right tool.

**Pinecone** — managed-only with no self-hosting path, closed source. Replaced by Qdrant when vector search is needed.

**LiteLLM** — large attack surface, BerriAI security incident in hosted proxy. Replaced by thin wrapper over the OpenAI and Anthropic SDKs.

**Langfuse** — v3 self-hosting requires ClickHouse + Redis + S3 on top of Postgres. Phoenix's RAG eval primitives and OTel portability are a better fit.

**Arize AX** — closed-source, proprietary OLAP database, data on Arize's servers with no clean exit path on free tier, 15-day retention limit.

**LangSmith** — self-hosting is Enterprise only. Vendor lock-in by design.

**Confident AI platform** — DeepEval the framework covers the CI/CD eval gate. The hosted platform adds overhead that isn't justified.

**Supabase** — continuous compute regardless of activity, schema-only branching. Replaced by Neon for LangGraph checkpointing.

**Claude Skill as the human ticket entry point** — worked well for low-volume human ticket submission: conversational pre-approval, typed Pydantic output, natural language input. Replaced for two reasons. First, the LLM formatting call runs inside claude.ai and cannot be traced through Phoenix — violating the invariant that every LLM call in the system is observable. Second, human ticket volume is not low: new features, new products, and new pipelines all originate as human tickets, making the human path a primary input source rather than an edge case. At that volume the formatting call is the one place where output quality degradation would be hardest to detect and easiest to misattribute. Replaced by a Streamlit input panel that posts to a Prefect task — the formatting call is a traced Prefect task through Phoenix, the confirmed ticket lands in Iceberg with a `source: human` field, and human-ticket PR outcomes are fully comparable to detector-ticket outcomes in the same metrics table.

## Roadmap

**Phase 1 is the starting point, and it is scoped to this proof of concept only.** It is what gets built and demonstrated in this repo.

**Phases 2 onward are future design thinking.** They describe where the architecture would go next and why. They are not commitments, and the design decisions in this README for those layers (Neon, Qdrant, the Agent Detector, the full agent roster, and so on) are intended direction rather than built functionality. A few early pieces of those phases already exist in the repo because the POC needed them. They are noted below.

Status reflects the current `main` branch.

- [~] **Phase 1 — POC starting point:** Pipeline + Pipeline Detector + Ticket Generator + Router + one specialist opens a PR end to end
  - Built: PySpark/Delta credit card fraud pipeline (`pipelines/`), Ticket Generator (`agents/ticket_generator.py`, LangGraph with validate-and-retry against a Jira-schema model), Task Agent (`agents/task_pr_agent.py`, LangGraph: load files → plan → implement → open PR → human review loop), and a Streamlit ticket queue where a human approves a ticket before the Task Agent runs
  - Not built yet: Pipeline Detector, Router (tickets are approved by a human and sent to the Task Agent directly), Bug and Spike agents, and the Prefect job that wakes the Task Agent to poll PR review state (stubbed)
- [~] **Phase 2 — Checkpointing, tracing, eval gate** *(future design, partly started)*
  - Built: arize-phoenix tracing of every LLM call to Phoenix Cloud via OpenTelemetry
  - Not built: Neon checkpointer (the Task Agent uses an in-memory `MemorySaver` for now) and the DeepEval CI/CD gate
- [~] **Phase 3 — GitHub → Iceberg metrics** *(future design, partly started)*
  - Built: Iceberg PR metadata table (PyIceberg with a local SQLite catalog) and a Phoenix spans table, both written by ingestion scripts in `ingestion/`
  - Not built: the Prefect flow that fires on PR close, derived metrics (`time_to_merge_hrs`, `commits_after_first_review`, `ci_failed_at_any_point`), and the GitHub Actions CI check
- [ ] **Phase 4 — Agent Detector** reading Phoenix traces + Iceberg PR analytics and generating tickets autonomously *(future design)*
- [~] **Phase 5 — Streamlit governance dashboard** *(future design, early version exists)*
  - Built: dashboard reading Iceberg through DuckDB, with a PR Metadata page (total PRs, merged, merge rate) and an Agents page (spans, latency, token counts, estimated cost)
  - Not built: per-agent merge and rework rates over time, CI failure rate, routing accuracy
- [ ] **Phase 6 — Full agent roster** including Story Agent with Router back-edge *(future design)*
- [~] **Phase 7 — README, architecture diagram, deploy Streamlit publicly, write-up** *(README and diagram done; deployment and write-up not started)*
- [ ] **Phase 8 — Migrate Phoenix Cloud → self-hosted Phoenix OSS on Railway** *(future design)*
- [ ] **Phase 9 — Bootstrap Agent:** generates baseline pipeline from human requirements spec, governance applies from commit one *(future design)*
- [ ] **Phase 10 — Vector index over Spike outputs and architecture docs** → Qdrant retrieves relevant knowledge base sections as specialist context, if scale warrants *(future design)*
- [ ] **Phase 11 — Structured retry signal or bounded agent self-retry on CI failure**, additive to the CI check, if warranted *(future design)*

## Why I Built This

Most agent portfolio projects build one agent that does one thing. This is different.

The hard problem isn't building an agent. It's running a team of them reliably — giving each a single clear responsibility, routing work correctly between them, observing each one independently, and creating a feedback loop where human judgment continuously improves the system without requiring extra steps from the human.

There's also a governance question most projects skip entirely: who's allowed to change the pipeline, and how? In `pipeline-hq`, the answer is unambiguous. Agents change the pipeline. Humans review those changes. That boundary isn't a limitation — it's the design. It's what makes the governance layer meaningful rather than decorative.

The stack choices reflect a consistent principle: raw data lives in open formats on storage you own, compute is swappable at every layer, self-hosting is possible on every component where data sensitivity could become a requirement. The distinction that matters is between the data layer and the compute layer — lock-in on compute is acceptable because compute is stateless and indexes are rebuildable. Lock-in on the data layer is where migration cost compounds. Every tool choice in this stack is designed around that distinction.

The specific tools will change. The principle won't. Open format storage, swappable compute, portable instrumentation — every layer is already in the right shape for whatever replaces the current tooling.

Same DE skills, different runtime. Pipeline observability → agent observability. On-call for pipeline failures → governance infrastructure for agent failures.

That's what pipeline-hq is.

## Getting Started

This runs the Phase 1 POC only.

```bash
git clone https://github.com/olivialeechao-eng/pipeline-hq
cd pipeline-hq
pip install -e .   # Python 3.11+, Java required for PySpark
```

Create a `.env` file with:

- `GROQ_API_KEY` — LLM inference (OpenAI-compatible Groq endpoint)
- `PHOENIX_API_KEY`, `PHOENIX_COLLECTOR_ENDPOINT`, `PHOENIX_BASE_URL` — Phoenix Cloud tracing and span ingestion
- `GITHUB_TOKEN`, `GITHUB_REPO` — lets the Task Agent open PRs (`owner/repo`); without them it falls back to reading local files and cannot open a PR

Then launch the app:

```bash
streamlit run apps/Ticket_Queue.py
```

The app has four pieces: a ticket queue where a human approves tickets, a ticket submission page, a PR metadata page, and an agents (traces) page.

`NEON_DATABASE_URL` and `PREFECT_API_KEY` are not needed yet. They belong to later, future-design phases.

### Repo layout

```
agents/          Ticket Generator and Task Agent (LangGraph)
agent_helpers/   Jira-schema ticket model
apps/            Streamlit app: ticket queue, submission, PR metadata, agent traces
ingestion/       GitHub PR metadata and Phoenix spans → Iceberg
queries/         DuckDB queries over the Iceberg tables
pipelines/       PySpark + Delta credit card fraud pipeline
tickets/         Generated Jira-schema tickets (JSON)
.claude/         Claude Code settings and skills
```
