# LangGraph4j in AIA Voice Insights — A Detailed Guide

This document explains, from first principles, what LangGraph4j is and exactly
how this codebase (`voice-insights-api`) uses it to run the 11-agent AIA
Singapore recommendation pipeline. It assumes no prior LangGraph4j knowledge.
Every code excerpt below is copied verbatim from this repository, so you can
open the real files side-by-side with this doc.

**Library version used:** `org.bsc.langgraph4j:langgraph4j-core:1.9.1`
(declared in `pom.xml`, `<langgraph4j.version>1.9.1</langgraph4j.version>`).

**Where the graph lives in this repo:**
```
src/main/java/com/aia/voiceinsights/api/graph/
├── RecommendationState.java          — our State type
└── RecommendationGraphFactory.java   — builds and wires the graph

src/main/java/com/aia/voiceinsights/api/service/
├── RecommendationOrchestrationService.java  — invokes the graph, owns the thread pool
├── RecommendationAgentService.java          — the actual LLM calls each node runs
└── RecommendationEventBus.java              — turns node events into a live SSE stream
```

---

## Part 1 — What problem is LangGraph4j solving here?

Before touching any API, it's worth being clear about *why* this pipeline is
a "graph" instead of just... a Java method that calls 11 other methods in
order.

If you wrote the pipeline as plain sequential code, it would look like:

```java
NeedAnalysisResult need = analyzeNeed(profile);
RiskAnalysisResult risk = analyzeRisk(profile);
AffordabilityResult afford = analyzeAffordability(profile);
MergedInsights merged = merge(need, risk, afford);
CustomerPersonaResult persona = buildPersona(profile, merged);
// ...and so on for 11 steps
```

That works, but it has three problems this system specifically needed to
solve:

1. **`need`, `risk`, and `affordability` don't depend on each other** — they
   should run *in parallel*, not one after another, to cut latency. Plain
   sequential Java doesn't express "these three can run concurrently, but
   nothing after them can start until all three finish" without you hand-rolling
   threads and joins yourself.
2. **Every step's progress needs to be observable from the outside** (a
   browser watching a live SSE stream) *while the pipeline is still running*,
   not just at the end. A plain method call gives you nothing until it
   returns.
3. **The pipeline is expected to keep growing** (it went from 4 steps to 11
   steps during this project). A hand-rolled orchestration method gets uglier
   with every new step; a graph structure stays declarative.

LangGraph4j gives you a small, explicit vocabulary for exactly this shape of
problem:

| Concept | What it means in plain English |
|---|---|
| **State** | A shared bag of data (a `Map<String,Object>` under the hood) that every step can read from and write to. |
| **Node** | One step of work — a Java function that reads the current State and returns the *changes* it wants to make to it. |
| **Edge** | A directed link saying "after node A finishes, go run node B." |
| **Graph** | The whole node+edge structure, defined once. |
| **Compiled graph** | The graph, validated and turned into something you can actually run. |
| **Invocation** | Actually running the compiled graph once, from `START` to `END`, driving the State forward one node at a time (or several nodes in parallel, when the graph shape allows it). |

Everything below is these five concepts, and how this codebase configures
them.

---

## Part 2 — The core API, tour by example

This section walks the actual LangGraph4j classes this project imports.
Every one of these is a real class from `langgraph4j-core-1.9.1`.

### 2.1 `AgentState` — the State type

```java
// org.bsc.langgraph4j.state.AgentState (library source)
public class AgentState {
    private final java.util.Map<String, Object> data;

    public AgentState(Map<String, Object> initData) {
        this.data = new HashMap<>(initData);
    }

    public final Map<String, Object> data() { ... }        // read-only view of everything

    public final <T> Optional<T> value(String key) {         // read one key, type-safely
        return ofNullable((T) data().get(key));
    }
}
```

`AgentState` is deliberately generic: it's just a typed wrapper around a
`Map<String, Object>`. There's no schema enforced at compile time — a node
asks for `state.<NeedAnalysisResult>value("needResult")` and gets back an
`Optional<NeedAnalysisResult>` that's empty if that key was never written
(hasn't run yet, or the graph took a path that skips it).

**This project's State type** (`RecommendationState.java`) does nothing more
than give `AgentState` a name and a factory constructor:

```java
public class RecommendationState extends AgentState {
    public RecommendationState(Map<String, Object> initData) {
        super(initData);
    }
}
```

Why subclass at all, if it adds no fields? Because `StateGraph<State extends
AgentState>` is generic over your State type — subclassing lets the compiler
know every node in *this* graph works with *this* recommendation-specific
state, rather than a generic untyped one. It's a marker/identity type, not a
data-holding type — the recommendation-specific data (need/risk/persona/etc.)
still just lives in the underlying `Map`, addressed by string keys.

### 2.2 `StateGraph<State>` — defining the graph shape

This is the builder you use to declare nodes and edges. Three methods matter
for this project:

```java
// org.bsc.langgraph4j.StateGraph (library source, trimmed)
public StateGraph<State> addNode(String id, AsyncNodeAction<State> action) throws GraphStateException;
public StateGraph<State> addEdge(String sourceId, String targetId) throws GraphStateException;
public CompiledGraph<State> compile() throws GraphStateException;
```

- `addNode(id, action)` registers one step of work under a string name.
- `addEdge(sourceId, targetId)` says "after `sourceId` finishes, run
  `targetId` next."
- `compile()` validates the graph (e.g. every node reachable from `START`,
  no dangling edges) and returns something you can actually execute.

There are two special node names, inherited from the `GraphDefinition`
interface `StateGraph` implements:

```java
// org.bsc.langgraph4j.GraphDefinition (library source)
String START = "__START__";
String END   = "__END__";
```

You never write a node with these ids yourself — you use them as the source
or target of an edge to say "this is where the graph begins" / "this is
where it ends."

**A critical, easy-to-miss detail about `addEdge` and fan-out:** calling
`addEdge` twice with the *same* `sourceId` does **not** overwrite the first
edge — it *appends* a second target, turning that node into a fan-out point.
From the library source:

```java
public StateGraph<State> addEdge(String sourceId, String targetId) throws GraphStateException {
    var newEdge = new Edge<>(sourceId, new EdgeValue<State>(targetId));
    int index = edges.elements.indexOf(newEdge);
    if (index >= 0) {
        // sourceId already has an edge — APPEND this target rather than replace
        var newTargets = new ArrayList<>(edges.elements.get(index).targets());
        newTargets.add(newEdge.target());
        edges.elements.set(index, new Edge<>(sourceId, newTargets));
    } else {
        edges.elements.add(newEdge);
    }
    return this;
}
```

This is *exactly* the mechanism this project's parallel fan-out relies on —
see Part 3.

### 2.3 `NodeAction` / `AsyncNodeAction` — what a node actually is

A node's "action" is just a function from the current State to a *partial
update* — the set of keys it wants to add/change, not the whole new State.

```java
// org.bsc.langgraph4j.action.AsyncNodeAction (library source, trimmed)
@FunctionalInterface
public interface AsyncNodeAction<S extends AgentState> extends Function<S, CompletableFuture<Map<String, Object>>> {
    CompletableFuture<Map<String, Object>> apply(S state);

    static <S extends AgentState> AsyncNodeAction<S> node_async(NodeAction<S> syncAction) {
        return t -> {
            try {
                return completedFuture(syncAction.apply(t));
            } catch (Exception e) {
                return failedFuture(e);
            }
        };
    }
}
```

`NodeAction<S>` (not shown above) is the synchronous version — a plain
function `State -> Map<String,Object>` that can throw. Since this project's
node bodies are ordinary blocking calls (HTTP calls to OpenAI, JDBC writes),
every node is written as a plain synchronous lambda and wrapped with
`AsyncNodeAction.node_async(...)` to satisfy the graph's async node contract.
You'll see this exact pattern in every `addNode` call in this codebase (Part
3).

Note the `try { ... } catch (Exception e) { return failedFuture(e); }` inside
`node_async`: the library *does* support a node throwing and failing the
whole graph run. **This project deliberately never lets that happen** — every
node method in `RecommendationGraphFactory` has its own internal
`try/catch` that turns a failure into a fallback result instead of letting it
propagate. Part 6 explains why.

### 2.4 `CompiledGraph<State>` — running the graph

```java
// org.bsc.langgraph4j.CompiledGraph (library source, trimmed)
public final class CompiledGraph<State extends AgentState> {
    @Deprecated(forRemoval = true)
    public Optional<State> invoke(Map<String,Object> inputs) { ... }
    // non-deprecated replacement: invoke(GraphInput, RunnableConfig)
}
```

`invoke(...)` runs the whole graph, `START` to `END`, and gives you back the
*final* State once nothing is left to run. This project uses the simple
(deprecated-but-still-supported) 3-argument-free overload:

```java
RecommendationState finalState = graph.invoke(Map.of("started", true))
        .orElseThrow(() -> new IllegalStateException("Recommendation graph produced no final state"));
```

`Map.of("started", true)` is the *initial* partial state fed in at `START` —
this project doesn't actually read a `"started"` key anywhere downstream; it
exists only because `invoke` needs a non-null input map, and every node here
gets its real input (the `CustomerProfile`) from a closure variable instead
of from State (see Part 3.2). The `Optional` is empty only if the graph
produced literally no output, which would indicate a structural bug (e.g. no
path reaches `END`).

---

## Part 3 — This system's actual graph

### 3.1 The topology

Here's the exact graph `RecommendationGraphFactory.build()` constructs,
redrawn from its Javadoc:

```
        START
          |
   +------+------+
   v      v      v
 need   risk  affordability      ← run concurrently, fan out from START
   +------+------+
          v
        merge                    ← fan-in: waits for all three branches
          v
       persona
          v
   productScoring
          v
  productShortlist
          v
    ragValidation
          v
   complianceCheck                ← moved before summary — see note below
          v
        summary
          v
     salesReport
          v
        END
```

Two shapes appear here:

- **Fan-out / fan-in** (`START → {need, risk, affordability} → merge`): three
  nodes share the same source, so they run concurrently; `merge` is the
  single node all three point *into*, so it only runs once all three are
  done.
- **Straight chain** (`merge → persona → … → salesReport → END`): every
  remaining node has exactly one predecessor and one successor — a simple
  linear pipeline.

**Why compliance runs before summary:** originally `summary` (the
customer-facing pitch) ran before `complianceCheck`. That meant the pitch
text was written before anyone knew whether the recommendation was actually
compliant. The edges were reordered (`ragValidation → complianceCheck →
summary → salesReport`) so the summary agent is handed the compliance
verdict and can write a caveated explanation ("we need to clarify your age
before we proceed...") when compliance didn't pass, instead of confidently
pitching something compliance is about to reject.

### 3.2 Building the graph — `RecommendationGraphFactory.build()`

```java
public CompiledGraph<RecommendationState> build(String runId, CustomerProfile profile) throws GraphStateException {
    StateGraph<RecommendationState> graph = new StateGraph<>(RecommendationState::new);

    graph.addNode("need", AsyncNodeAction.node_async(state -> runNeed(runId, profile)));
    graph.addNode("risk", AsyncNodeAction.node_async(state -> runRisk(runId, profile)));
    graph.addNode("affordability", AsyncNodeAction.node_async(state -> runAffordability(runId, profile)));
    graph.addNode("merge", AsyncNodeAction.node_async(state -> runMerge(runId, profile, state)));
    graph.addNode("persona", AsyncNodeAction.node_async(state -> runPersona(runId, profile, state)));
    graph.addNode("productScoring", AsyncNodeAction.node_async(state -> runProductScoring(runId, profile, state)));
    graph.addNode("productShortlist", AsyncNodeAction.node_async(state -> runProductShortlist(runId, state)));
    graph.addNode("ragValidation", AsyncNodeAction.node_async(state -> runRagValidation(runId, state)));
    graph.addNode("complianceCheck", AsyncNodeAction.node_async(state -> runComplianceCheck(runId, profile, state)));
    graph.addNode("summary", AsyncNodeAction.node_async(state -> runSummary(runId, profile, state)));
    graph.addNode("salesReport", AsyncNodeAction.node_async(state -> runSalesReport(runId, profile, state)));

    graph.addEdge(StateGraph.START, "need");
    graph.addEdge(StateGraph.START, "risk");
    graph.addEdge(StateGraph.START, "affordability");
    graph.addEdge("need", "merge");
    graph.addEdge("risk", "merge");
    graph.addEdge("affordability", "merge");
    graph.addEdge("merge", "persona");
    graph.addEdge("persona", "productScoring");
    graph.addEdge("productScoring", "productShortlist");
    graph.addEdge("productShortlist", "ragValidation");
    graph.addEdge("ragValidation", "complianceCheck");
    graph.addEdge("complianceCheck", "summary");
    graph.addEdge("summary", "salesReport");
    graph.addEdge("salesReport", StateGraph.END);

    return graph.compile();
}
```

Walking through this line by line, for a beginner:

1. **`new StateGraph<>(RecommendationState::new)`** — creates an empty graph
   builder. The constructor argument is an `AgentStateFactory<State>` — a
   function that knows how to build a fresh `RecommendationState` from a
   `Map<String,Object>`. `RecommendationState::new` (a constructor reference)
   satisfies that directly, because `RecommendationState`'s constructor
   signature (`Map<String,Object> -> RecommendationState`) already matches.

2. **The three `addEdge(StateGraph.START, ...)` calls** — this is the fan-out
   from Part 2.2 in action: three separate calls with the *same* `sourceId`
   (`StateGraph.START`) each append a new target, so `START` ends up with
   three outgoing edges. LangGraph4j's runtime sees a node with multiple
   outgoing edges and runs all of them concurrently.

3. **The three `addEdge("X", "merge")` calls** — same append mechanism, but
   here it's the *fan-in*: three different `sourceId`s (`need`, `risk`,
   `affordability`) all target `merge`. LangGraph4j won't run `merge` until
   every edge pointing into it has fired — i.e., until all three parallel
   branches have completed.

4. **Every remaining `addEdge` call** — a straight A→B link, since each of
   those `sourceId`s only appears once.

5. **`graph.compile()`** — validates the whole structure (every node
   reachable, `START` has at least one outgoing edge, etc.) and returns a
   `CompiledGraph<RecommendationState>` ready to `invoke()`.

**Why is this method called fresh for every run**, instead of building the
graph once at startup and reusing it? Look closely at the lambdas:
`state -> runNeed(runId, profile)`. `runId` and `profile` are **closure
variables** — each node's lambda captures the specific run's id and customer
profile at the moment `build()` is called. A `CompiledGraph` built for one
customer can't be reused for another, because its node functions are
permanently bound to that customer's data. So
`RecommendationOrchestrationService` calls `graphFactory.build(runId,
profile)` fresh, once per recommendation run (see Part 5).

---

## Part 4 — Anatomy of a single node, line by line

Every node method in this codebase follows the exact same five-part shape.
Here's the simplest one, `runNeed`, annotated:

```java
private Map<String, Object> runNeed(String runId, CustomerProfile profile) {
    // 1. Tell the outside world this step has started (drives the live UI).
    eventBus.publish(runId, RecommendationEvent.agentStarted("need"));
    Instant start = Instant.now();

    try {
        // 2. Do the actual work — in this case, one LLM call.
        NeedAnalysisResult result = agentService.analyzeNeed(profile);

        // 3. Persist BOTH what this step was given and what it produced —
        //    the audit trail (see RecommendationStore.saveAgentStep).
        store.saveAgentStep(runId, "need", "COMPLETED", profile, result);

        // 4. Tell the outside world this step finished, with its result.
        eventBus.publish(runId, RecommendationEvent.agentCompleted("need", result));
        logCompleted("need", runId, start);

        // 5. Return the STATE UPDATE — not the whole state, just what changed.
        return Map.of("needResult", result);

    } catch (Exception e) {
        // Same shape, but for the failure path: a fallback result instead
        // of an exception, so the graph keeps running.
        NeedAnalysisResult fallback = new NeedAnalysisResult(List.of(), List.of(), List.of(),
                "Need analysis failed: " + e.getMessage());
        store.saveAgentStep(runId, "need", "FAILED", profile, fallback);
        eventBus.publish(runId, RecommendationEvent.agentFailed("need", e.getMessage()));
        logFailed("need", runId, start, e);
        return Map.of("needResult", fallback);
    }
}
```

The one line that's specific to LangGraph4j (everything else is this
project's own plumbing) is the **return statement**: `Map.of("needResult",
result)`. This is the *partial state update* concept from Part 1 made
concrete — `runNeed` doesn't return a `RecommendationState`, it returns just
the one key it's responsible for. LangGraph4j takes that map and merges it
into the shared State (Part 5 explains exactly how).

Every other node follows this same shape, just reading more keys out of
`state` first when it needs upstream results — e.g. `runMerge` needs all
three parallel results:

```java
private Map<String, Object> runMerge(String runId, CustomerProfile profile, RecommendationState state) {
    eventBus.publish(runId, RecommendationEvent.agentStarted("merge"));
    Instant start = Instant.now();

    NeedAnalysisResult need = state.<NeedAnalysisResult>value("needResult")
            .orElseGet(() -> new NeedAnalysisResult(List.of(), List.of(), List.of(), "Not available"));
    RiskAnalysisResult risk = state.<RiskAnalysisResult>value("riskResult")
            .orElseGet(() -> new RiskAnalysisResult(List.of(), "Unknown", "Not available"));
    AffordabilityResult affordability = state.<AffordabilityResult>value("affordabilityResult")
            .orElseGet(() -> new AffordabilityResult("Unknown", "Not available", "Not available"));
    // ...
}
```

This is `AgentState.value(key)` from Part 2.1 — `state.<NeedAnalysisResult>value("needResult")`
reads the exact key `runNeed` wrote, typed as `Optional<NeedAnalysisResult>`.
Because `merge` only runs after LangGraph4j has confirmed *all three* of
`need`/`risk`/`affordability` completed (the fan-in from Part 3.1), these
three `.value(...)` calls are guaranteed to find real data — the
`.orElseGet(...)` fallbacks here are defensive programming, not something
that's expected to trigger in normal operation.

Further downstream, nodes read the keys written by *whichever* node
happens to have produced them — e.g. `runSalesReport` (the last node) reads
eight different keys, one from nearly every prior node, because the final
report needs to cite everything the pipeline established:

```java
private Map<String, Object> runSalesReport(String runId, CustomerProfile profile, RecommendationState state) {
    MergedInsights merged = requireMerged(state);                    // from "merge"
    CustomerPersonaResult persona = state.<CustomerPersonaResult>value("personaResult")...   // from "persona"
    ProductScoringResult scoring = state.<ProductScoringResult>value("scoringResult")...     // from "productScoring"
    ProductShortlistResult shortlist = requireShortlist(state);      // from "productShortlist"
    RagValidationResult validation = requireValidation(state);       // from "ragValidation"
    RecommendationSummaryResult summary = state.<RecommendationSummaryResult>value("summaryResult")...  // from "summary"
    ComplianceCheckResult compliance = state.<ComplianceCheckResult>value("complianceResult")...        // from "complianceCheck"
    // ...
}
```

This is the entire mental model of "State" in practice: it's a growing bag
of results that every node can read from, and each node only ever adds to
it, never removes from it.

---

## Part 5 — State merge semantics: how a `Map` return value becomes shared state

When a node returns `Map.of("needResult", result)`, what actually happens to
it? Internally, `AgentState` has a static merge function:

```java
// org.bsc.langgraph4j.state.AgentState (library source, trimmed)
public static Map<String, Object> updateState(Map<String,Object> state, Map<String,Object> partialState, Map<String,Channel<?>> channels) {
    if (partialState == null || partialState.isEmpty()) return state;

    Map<String,Object> updatedPartialState = updatePartialStateFromSchema(state, partialState, channels);

    return Stream.concat(state.entrySet().stream(), updatedPartialState.entrySet().stream())
            .collect(toMapRemovingItemMarkedForRemoval());   // merge function: (old, new) -> new
}
```

Reading this as a beginner: it takes the *old* state map and the node's
*partial update* map, concatenates their entries, and collects them into a
fresh map — where, if the same key appears in both (i.e. the node overwrote
something that already existed), **the new value wins**. This is called
"last-write-wins" merging.

**The `channels` parameter is the escape hatch for anything fancier** — a
`Channel<T>` lets you plug in custom merge logic for a specific key (e.g.
"append to a list instead of overwriting," or "sum two numbers instead of
replacing"). This project doesn't configure any channels. Why that's safe is
spelled out directly in `RecommendationState`'s Javadoc:

```java
/**
 * State keys written by the recommendation graph's nodes: "needResult",
 * "riskResult", "affordabilityResult" ... "mergedInsights" ...
 * Every key is written by exactly one node, so no custom
 * {@link org.bsc.langgraph4j.state.Channel} reducers are needed — the
 * default last-write-wins merge is already correct.
 */
```

In other words: because every one of the 11 keys this graph uses
(`needResult`, `riskResult`, `affordabilityResult`, `mergedInsights`,
`personaResult`, `scoringResult`, `shortlistResult`, `validationResult`,
`complianceResult`, `summaryResult`, `salesReportResult`) is written by
*exactly one* node and read (never written) by every downstream node, the
"last write wins" default behaves identically to "the only write wins" —
there's never a conflict to resolve. If a future node needed to, say,
accumulate a growing list that multiple nodes append to, *that* is when a
custom `Channel` would become necessary — a good thing to know if you extend
this graph later, but not something this pipeline currently needs.

---

## Part 6 — Running the graph: `invoke()`, cloning, and why nodes never throw

### 6.1 Where `invoke()` is called

`RecommendationOrchestrationService.runGraph()` is the only place this
project calls `invoke()`:

```java
private void runGraph(String runId, CustomerProfile profile) {
    Instant startedAt = Instant.now();
    try {
        CompiledGraph<RecommendationState> graph = graphFactory.build(runId, profile);
        RecommendationState finalState = graph.invoke(Map.of("started", true))
                .orElseThrow(() -> new IllegalStateException("Recommendation graph produced no final state"));

        if (finalState.<MergedInsights>value("mergedInsights").isEmpty()) {
            ensureMergedInsights(runId, profile, finalState);   // defensive fallback, see below
        }

        store.markRunStatus(runId, "COMPLETED", null);
        log.info("Recommendation run {} completed in {} ms", runId, Duration.between(startedAt, Instant.now()).toMillis());
    } catch (Exception e) {
        store.markRunStatus(runId, "FAILED", e.getMessage());
        eventBus.publish(runId, RecommendationEvent.runFailed(e.getMessage()));
        log.warn("Recommendation run {} failed after {} ms: {}", runId, Duration.between(startedAt, Instant.now()).toMillis(), e.getMessage(), e);
    } finally {
        eventBus.complete(runId);
    }
}
```

Note this method itself is called on a background thread — `startRun()`
(the method the REST controller calls) does `executor.submit(() ->
runGraph(runId, profile))` and returns the `runId` immediately, so the HTTP
`POST /api/customers/{id}/recommendations` call doesn't block for the ~20-30
seconds the full 11-agent run actually takes. The caller then watches
progress over SSE (Part 7).

### 6.2 What happens on every node transition: cloning via Java serialization

This is the single most important "gotcha" to understand if you're going to
write new node code for this graph. Internally, every time a node finishes,
`CompiledGraph` takes a **snapshot** of the state at that point — used for
things like checkpointing and the `NodeOutput` stream. That snapshot is
produced by `cloneState`:

```java
// org.bsc.langgraph4j.CompiledGraph (library source)
State cloneState(Map<String,Object> data, RunnableConfig runnableConfig) throws ... {
    if (runnableConfig.isCloneStateDisabled()) {
        return stateGraph.getStateSerializer().stateOf(data);
    }
    return stateGraph.getStateSerializer().cloneObject(data);   // ← this is the default path
}
```

`cloneObject` goes through the configured `StateSerializer`. Unless you
configure something else, `StateGraph`'s default constructor
(`new StateGraph<>(RecommendationState::new)`, exactly what this project
uses) wires up `ObjectStreamStateSerializer` — which, true to its name, uses
Java's built-in `ObjectOutputStream`/`ObjectInputStream` to serialize and
deserialize every value in the state map:

```java
// org.bsc.langgraph4j.serializer.std.ObjectStreamStateSerializer (library source, trimmed)
public final void writeData(Map<String, Object> data, ObjectOutput out) throws IOException {
    mapSerializer.write(data, mapper.objectOutputWithMapper(out));
}
```

**The consequence: every class you put into State must implement
`java.io.Serializable`.** This is why every result record in this
codebase — `NeedAnalysisResult`, `RiskAnalysisResult`, `MergedInsights`,
`CustomerPersonaResult`, `ProductScoringResult`, `RagValidationResult`,
`ComplianceCheckResult`, `SalesReportResult`, and every other type that ends
up in a node's return `Map` — is declared as:

```java
public record NeedAnalysisResult(
        List<String> protectionGaps,
        List<String> recommendedCategories,
        List<String> matchedProductNames,
        String rationale
) implements Serializable { }
```

If you ever add a 12th node whose result type forgets `implements
Serializable`, the graph will compile fine and even start running fine — but
it will throw a `NotSerializableException` the moment LangGraph4j tries to
clone the state after that node runs. This is the #1 thing to remember when
extending this graph.

### 6.3 Why every node has its own try/catch (instead of trusting `node_async`)

Recall from Part 2.3 that `AsyncNodeAction.node_async` already has failure
handling built in — a thrown exception becomes a `failedFuture`, which would
fail the whole graph invocation. This project **deliberately never uses
that path**. Every single node method wraps its real work in its own
`try/catch` and always returns a normal (successful) `Map`, just with either
a real result or a clearly-marked fallback result:

```java
} catch (Exception e) {
    NeedAnalysisResult fallback = new NeedAnalysisResult(List.of(), List.of(), List.of(),
            "Need analysis failed: " + e.getMessage());
    store.saveAgentStep(runId, "need", "FAILED", profile, fallback);
    eventBus.publish(runId, RecommendationEvent.agentFailed("need", e.getMessage()));
    return Map.of("needResult", fallback);   // still a normal, successful return!
}
```

This is a deliberate architectural choice, documented in
`RecommendationGraphFactory`'s class Javadoc:

> A node never lets an LLM/parsing failure abort the whole graph: on error
> it publishes `agent_failed` and still returns a (marked) fallback result,
> so downstream steps still run against a well-formed (if degraded) input
> instead of the whole pipeline dying on one bad call.

Think about what the alternative would mean: if `runProductScoring` threw
because OpenAI returned malformed JSON, and that exception propagated up
through `node_async`'s `failedFuture`, the *entire* recommendation run would
die right there — no shortlist, no RAG validation, no summary, no report,
and the advisor gets nothing. By catching locally and substituting a
fallback (e.g. an empty scoring list with a message explaining what
happened), the pipeline degrades gracefully instead of collapsing, and every
downstream node's `.orElseGet(...)` defensive read still gets *something*
sensible to work with.

This is also why every `run*` helper method at the bottom of
`RecommendationGraphFactory` has `requireMerged`, `requireShortlist`,
`requireValidation` — small private methods that read a key with a sane
fallback, used by any node that isn't 100% certain its upstream dependency
succeeded:

```java
private MergedInsights requireMerged(RecommendationState state) {
    return state.<MergedInsights>value("mergedInsights").orElseGet(() -> new MergedInsights(
            new NeedAnalysisResult(List.of(), List.of(), List.of(), "Not available"),
            new RiskAnalysisResult(List.of(), "Unknown", "Not available"),
            new AffordabilityResult("Unknown", "Not available", "Not available"),
            "Not available"));
}
```

### 6.4 The one place a genuine LangGraph4j-level failure IS handled: `runGraph`'s outer try/catch

Even with every node catching its own errors, `graph.invoke(...)` itself
could still throw — e.g. if `graphFactory.build()` throws a
`GraphStateException` because the graph is malformed (a bug, not a runtime
LLM failure), or if some unexpected runtime error escapes LangGraph4j's own
internals. That's what the outer `try/catch` in `runGraph()` (Part 6.1) is
for — it's the last line of defense, marking the whole run `"FAILED"` in the
database and publishing a `run_failed` SSE event so the UI doesn't hang
forever waiting for events that will never come.

---

## Part 7 — From node callback to live UI: the event bus

LangGraph4j itself has no idea this project has a browser watching a live
progress animation — that's entirely this codebase's own plumbing, built on
top of the "node just called `eventBus.publish(...)`" pattern from Part 4.

```java
// RecommendationEventBus.java
@Service
public class RecommendationEventBus {
    private final Map<String, Sinks.Many<RecommendationEvent>> sinks = new ConcurrentHashMap<>();

    public Flux<RecommendationEvent> subscribe(String runId) {
        return sinkFor(runId).asFlux();
    }

    public void publish(String runId, RecommendationEvent event) {
        sinkFor(runId).tryEmitNext(event);
    }

    public void complete(String runId) {
        Sinks.Many<RecommendationEvent> sink = sinks.remove(runId);
        if (sink != null) sink.tryEmitComplete();
    }

    private Sinks.Many<RecommendationEvent> sinkFor(String runId) {
        return sinks.computeIfAbsent(runId, id -> Sinks.many().replay().all());
    }
}
```

One `Sinks.Many` (a Project Reactor multicast publisher) per in-flight run,
keyed by `runId`. Every `eventBus.publish(runId, ...)` call inside a node
method pushes one event into that run's sink. `Sinks.many().replay().all()`
means a subscriber that connects *slightly late* (the browser's `GET
/api/recommendations/{runId}/stream` arriving a beat after the `POST` that
started the run) still receives every event from the beginning, not just
events from the moment it connected.

`RecommendationController.stream()` exposes this as Server-Sent Events:

```java
@GetMapping(value = "/recommendations/{runId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<String> stream(@PathVariable String runId) {
    return eventBus.subscribe(runId).map(this::toJson);
}
```

So the full chain, end to end, for a single node completing is:

```
runNeed() finishes
    → eventBus.publish(runId, agentCompleted("need", result))
    → Sinks.Many.tryEmitNext(event)
    → RecommendationController's Flux<String> emits one more JSON line
    → browser's EventSource fires onmessage
    → useRecommendationStream.ts's applyEvent() updates React state
    → AgentCard re-renders with a green checkmark
```

None of this is LangGraph4j — it's this project's own event bus wired to
each node's own explicit publish calls. LangGraph4j only guarantees *when*
each node runs relative to the others (the graph's edges); everything about
*observing* that execution from outside was built on top, node by node.

---

## Part 8 — The audit trail: `RecommendationStore.saveAgentStep`

The other thing every node does, alongside publishing an event, is persist
its input and output:

```java
store.saveAgentStep(runId, "need", "COMPLETED", profile, result);
```

```java
// RecommendationStore.java
public void saveAgentStep(String runId, String agentKey, String status, Object input, Object output) {
    String inputJson = input == null ? null : mapper.writeValueAsString(input);
    String outputJson = output == null ? null : mapper.writeValueAsString(output);
    jdbc.update("""
            INSERT INTO %s.recommendation_agent_results (run_id, agent_name, status, input_json, result_json, completed_at)
            VALUES (?, ?, ?, ?::jsonb, ?::jsonb, now())
            ON CONFLICT (run_id, agent_name) DO UPDATE SET ...
            """.formatted(schema), runId, agentKey, status, inputJson, outputJson);
}
```

This is a second, independent way this project observes what the graph did
— not via events (live, transient, only useful while connected), but via
Postgres rows (permanent, queryable after the fact). `GET
/api/recommendations/{runId}` reconstructs a full step-by-step view of a
run from these rows, ordered by `RecommendationStore.PIPELINE_ORDER` (a
plain `List<String>` this project maintains by hand, matching the graph's
edge order — LangGraph4j itself doesn't expose "the canonical execution
order of this graph" as an API, so this project just keeps that list in
sync manually).

Again: this is not a LangGraph4j feature. LangGraph4j gave this project the
*shape* (parallel-then-sequential execution); the audit trail is bespoke
code that every node happens to call into, using the `runId`/`agentKey`
each node already has in scope.

---

## Part 9 — Concurrency: two different thread pools, and why

It's easy to conflate "LangGraph4j runs nodes in parallel" with "this
project only has one thread pool." There are actually two, at different
layers:

1. **`RecommendationOrchestrationService`'s pool** — one thread *per
   in-flight recommendation run* (a whole `graph.invoke(...)` call occupies
   one thread for its entire ~20-30 second lifetime, since `invoke` is a
   blocking call from the caller's point of view):

   ```java
   private final ExecutorService executor = new ThreadPoolExecutor(
           4, MAX_CONCURRENT_RUNS /* = 16 */, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(200));
   ```

2. **`RecommendationAgentService`'s pool** — used *inside* a single node,
   to bound and time out each individual OpenAI call:

   ```java
   private final ExecutorService llmExecutor = Executors.newFixedThreadPool(24);
   ```

   Every LLM call goes through this pool with a 45-second timeout and up to
   3 retries with backoff (see `RecommendationAgentService.call()` /
   `callOnce()`), so one hung or rate-limited OpenAI request can't block a
   node — and therefore an entire run — forever.

**How does LangGraph4j actually achieve "need/risk/affordability run
concurrently" if each node's *own* work (an OpenAI call) is a plain
blocking Java method call?** LangGraph4j's own internal execution engine (not
shown in this doc's excerpts — it lives in `CompiledGraph`'s private
execution loop) is what submits multiple ready-to-run nodes onto its own
internal async machinery once it sees they share no unmet dependency — it
sees `need`, `risk`, and `affordability` are all reachable from `START` with
no edges *into* them from each other, and dispatches all three at once. This
project doesn't manage that fan-out itself — it only declares the *shape*
(via `addEdge`) and lets LangGraph4j's engine decide when to fire each node.

---

## Part 10 — Common gotchas (a checklist for extending this graph)

If you add a 12th node to this pipeline, here's everything that bit us (or
could have) while building the first 11:

1. **Every type you put into `Map<String,Object>` from a node must implement
   `Serializable`.** See Part 6.2. Forgetting this compiles fine and fails
   at runtime, mid-graph, with a `NotSerializableException`.
2. **`addEdge(sourceId, targetId)` appends, it doesn't replace.** Calling it
   twice with the same `sourceId` creates a fan-out, not a "the second call
   overwrote the first" bug. If you *meant* to replace an edge, you can't —
   you'd need a different graph shape (e.g. conditional edges).
3. **A node's return `Map` is a partial update, not the new state.** Only
   include the keys you're actually setting. Returning `Map.of()` is valid
   (no-op update) if a node has nothing new to contribute.
4. **Reading a key before it's guaranteed to exist returns `Optional.empty()`,
   not an exception.** Always use `.orElseGet(...)` for keys written by a
   node that isn't a guaranteed-completed predecessor (see the
   `requireMerged`/`requireShortlist`/`requireValidation` helpers).
5. **A `CompiledGraph` is built once per run, not once per process.** Because
   node lambdas close over `runId`/`profile`, don't try to cache/reuse a
   `CompiledGraph` across different customers — call `graphFactory.build(...)`
   fresh every time (already how `RecommendationOrchestrationService` does
   it).
6. **Let a node fail *into* a fallback, not out as an exception**, unless you
   genuinely want one bad node to abort the entire run. Follow the
   try/catch-and-substitute-a-fallback pattern from every existing node
   (Part 6.3), not `node_async`'s default failedFuture behavior.
7. **Update `RecommendationStore.PIPELINE_ORDER`** (and the frontend's
   mirrored `PIPELINE_ORDER` in `useRecommendationStream.ts`) when you add
   or reorder a node — LangGraph4j doesn't expose this ordering as an API,
   so it's maintained by hand in two places and must be kept in sync with
   the actual `addEdge` chain in `RecommendationGraphFactory`.
8. **`langgraph4j-spring-ai` is a declared dependency but isn't actually
   used** — every LLM call in this codebase goes through Spring AI's
   `ChatModel` directly (`RecommendationAgentService`), not through any
   LangGraph4j/Spring-AI integration class. It's a plain Spring `@Service`
   bean, wired into the graph only as a collaborator each node method calls.

---

## Part 11 — Glossary

| Term | Meaning in this codebase |
|---|---|
| **State** | The shared `Map<String,Object>` (wrapped as `RecommendationState`) every node reads from and writes to. |
| **Node** | One pipeline step — in this project, always a `private Map<String,Object> run*(...)` method wrapped with `AsyncNodeAction.node_async(...)`. |
| **Edge** | A `graph.addEdge(sourceId, targetId)` call — "run targetId after sourceId." |
| **Fan-out** | Multiple edges sharing one `sourceId` → those target nodes run concurrently. Used for `START → {need, risk, affordability}`. |
| **Fan-in** | Multiple edges sharing one `targetId` → that node waits for all of them. Used for `{need, risk, affordability} → merge`. |
| **`StateGraph`** | The builder you call `addNode`/`addEdge`/`compile()` on. |
| **`CompiledGraph`** | The validated, runnable graph — call `.invoke(...)` on this. |
| **`AgentStateFactory`** | A function `Map<String,Object> -> State`; `RecommendationState::new` satisfies it. |
| **Partial state update** | What a node returns — only the keys it's adding/changing, merged (last-write-wins) into the existing State. |
| **`Channel`** | An optional per-key custom merge strategy; not used in this project (every key has exactly one writer). |
| **Serialization / cloning** | LangGraph4j snapshots State via Java's `ObjectOutputStream`/`ObjectInputStream` after every node — hence every state value type must implement `Serializable`. |
| **`START` / `END`** | Special node ids (`"__START__"` / `"__END__"`) marking the graph's entry and exit. |

---

## Part 12 — Where to go from here

- Read `RecommendationGraphFactory.java` top to bottom now — every method in
  it should map directly onto a section of this document.
- If you want to add a new agent to the pipeline: pick where it fits in the
  chain, add an `addNode`/`addEdge` pair, write a `run*` method following the
  exact shape in Part 4, make its result type `implements Serializable`, and
  update the two `PIPELINE_ORDER` lists (Part 10, item 7).
- The official LangGraph4j docs/examples (outside this repo) are useful for
  features this project *doesn't* use yet — conditional edges (branching
  based on state, via `addConditionalEdges`), subgraphs (`addNode(id,
  CompiledGraph<State>)`), and checkpointing/persistence (`CompileConfig`'s
  `checkpointSaver`) — none of which this pipeline currently needs, since
  every run is a single linear-with-one-fan-out pass that always runs to
  completion or fails outright.
