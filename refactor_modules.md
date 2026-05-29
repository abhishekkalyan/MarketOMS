# Claude Code Task: Refactor MarketOMS into Separated Bounded Modules

## Context

You are working on a zero-GC, mechanically sympathetic sell-side Order Management System (OMS)
built in Java 21+ using Aeron Cluster, Agrona, and Aeron IPC transport. The existing codebase
lives in a single Maven/Gradle project at the repo root with this package structure:

```
src/main/java/com/sellside/oms/
  codec/          OrderFlyweight.java, OrderFields.java
  statemachine/   OrderState.java, OrderStateMachine.java
  validation/     OrderValidationEngine.java, SymbolUniverse.java, ValidationResult.java
  algo/           AlgoExecutionEngine.java, IcebergAlgoEngine.java,
                  TwapAlgoEngine.java, SmartOrderRouter.java
  cluster/        OmsClusteredService.java, ClusterMessageType.java,
                  AeronEgressPublisher.java, SequenceTracker.java
  fix/            FixFields.java, FixToFlyweightTranslator.java
  common/         OrderBook.java
```

## Objective

Refactor this single project into a **Gradle multi-module monorepo** with four bounded modules
and a launcher. The Algo and SOR components must be fully separated from the OMS core,
communicating only over Aeron IPC — not via direct Java method calls or shared classpath
beyond the codec module.

Do NOT convert this into microservices. All modules are co-deployed on the same host,
communicating via Aeron IPC channels configured in the launcher.

---

## Target Module Structure

```
market-oms/                          ← repo root
  settings.gradle
  build.gradle                       ← root conventions (Java 21, shared dependency versions)
  gradle/
    libs.versions.toml               ← version catalog

  oms-codec/                         ← Module 1
    src/main/java/com/sellside/oms/codec/
    src/main/java/com/sellside/oms/fix/
    build.gradle                     ← depends on Agrona only

  oms-core/                          ← Module 2
    src/main/java/com/sellside/oms/statemachine/
    src/main/java/com/sellside/oms/validation/
    src/main/java/com/sellside/oms/cluster/
    src/main/java/com/sellside/oms/common/
    build.gradle                     ← depends on oms-codec, Aeron Cluster, Agrona

  algo-sor/                          ← Module 3
    src/main/java/com/sellside/oms/algo/
    src/main/java/com/sellside/oms/algoagent/  ← NEW: AlgoSorAgent.java
    build.gradle                     ← depends on oms-codec, Aeron, Agrona

  oms-launcher/                      ← Module 4
    src/main/java/com/sellside/oms/launcher/  ← NEW: OmsLauncher.java
    build.gradle                     ← depends on all above modules
```

---

## Step-by-Step Instructions

### Step 1 — Explore First (Plan Mode)

Before writing any code, read and understand:
- Every existing Java file in full
- The existing build file(s) at the repo root
- The dependency tree (especially Agrona and Aeron version constraints)

Produce a written plan listing:
1. Every file move (source path → destination path)
2. Every import statement that will need updating (old package → new package if any)
3. Every new file to be created (with its purpose)
4. Any circular dependency risks

Do not edit any files until the plan is approved.

---

### Step 2 — Gradle Multi-Module Build Files

Create the following build files exactly as specified.

**`settings.gradle`** (repo root):
```groovy
rootProject.name = 'market-oms'
include 'oms-codec', 'oms-core', 'algo-sor', 'oms-launcher'
```

**`gradle/libs.versions.toml`** — version catalog with these entries (look up current stable
versions if uncertain):
- `agrona` (io.aeron:agrona)
- `aeron` (io.aeron:aeron-all)
- `aeron-cluster` (already bundled in aeron-all — do NOT add a separate dep)
- `junit-jupiter` for tests
- `java` version = `21`

**`build.gradle`** (root, conventions only):
- Apply `java` plugin to all subprojects
- Set `sourceCompatibility` and `targetCompatibility` to Java 21
- Configure `test { useJUnitPlatform() }` for all subprojects
- Do NOT declare any module-specific dependencies here

**`oms-codec/build.gradle`**:
- `implementation` dependency on Agrona only
- No Aeron dependency (codec is transport-agnostic)

**`oms-core/build.gradle`**:
- `implementation project(':oms-codec')`
- `implementation` Agrona + Aeron

**`algo-sor/build.gradle`**:
- `implementation project(':oms-codec')`
- `implementation` Agrona + Aeron
- NO dependency on `:oms-core` — this boundary is strictly enforced

**`oms-launcher/build.gradle`**:
- `implementation project(':oms-codec')`
- `implementation project(':oms-core')`
- `implementation project(':algo-sor')`
- `implementation` Agrona + Aeron

---

### Step 3 — Move Existing Source Files

Move (do not rewrite) each existing file to its new module path:

| Existing path | Destination module | New path |
|---|---|---|
| `codec/OrderFlyweight.java` | `oms-codec` | `oms-codec/src/main/java/com/sellside/oms/codec/` |
| `codec/OrderFields.java` | `oms-codec` | `oms-codec/src/main/java/com/sellside/oms/codec/` |
| `fix/FixFields.java` | `oms-codec` | `oms-codec/src/main/java/com/sellside/oms/fix/` |
| `fix/FixToFlyweightTranslator.java` | `oms-codec` | `oms-codec/src/main/java/com/sellside/oms/fix/` |
| `statemachine/OrderState.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/statemachine/` |
| `statemachine/OrderStateMachine.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/statemachine/` |
| `validation/OrderValidationEngine.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/validation/` |
| `validation/SymbolUniverse.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/validation/` |
| `validation/ValidationResult.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/validation/` |
| `cluster/OmsClusteredService.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/cluster/` |
| `cluster/ClusterMessageType.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/cluster/` |
| `cluster/AeronEgressPublisher.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/cluster/` |
| `cluster/SequenceTracker.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/cluster/` |
| `common/OrderBook.java` | `oms-core` | `oms-core/src/main/java/com/sellside/oms/common/` |
| `algo/AlgoExecutionEngine.java` | `algo-sor` | `algo-sor/src/main/java/com/sellside/oms/algo/` |
| `algo/IcebergAlgoEngine.java` | `algo-sor` | `algo-sor/src/main/java/com/sellside/oms/algo/` |
| `algo/TwapAlgoEngine.java` | `algo-sor` | `algo-sor/src/main/java/com/sellside/oms/algo/` |
| `algo/SmartOrderRouter.java` | `algo-sor` | `algo-sor/src/main/java/com/sellside/oms/algo/` |

**Package names do NOT change.** All files keep `package com.sellside.oms.*`. Only their
filesystem location changes. Update no import statements unless the compiler reports an error.

---

### Step 4 — Create AlgoSorAgent (new file in algo-sor)

Create `algo-sor/src/main/java/com/sellside/oms/algoagent/AlgoSorAgent.java`.

This class bridges the `algo-sor` module to the Aeron IPC transport without depending on
`oms-core`. It must satisfy these constraints:

**Zero-allocation hot path contract** — same as the rest of the codebase:
- No object allocation in `doWork()`
- Pre-allocate all flyweights, buffers, and engine instances in the constructor
- Use `Thread.onSpinWait()` for back-pressure, not `Thread.sleep()`

**Class signature:**
```java
package com.sellside.oms.algoagent;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import com.sellside.oms.algo.*;
import com.sellside.oms.codec.*;

import java.nio.ByteBuffer;

public final class AlgoSorAgent implements Agent, FragmentHandler { ... }
```

**Constructor parameters:**
```java
public AlgoSorAgent(
    Subscription parentOrderSub,      // Aeron sub: accepted parent orders from oms-core
    ExclusivePublication childOrderPub, // Aeron pub: child orders back to oms-core/FIX bridge
    IcebergAlgoEngine icebergEngine,
    TwapAlgoEngine    twapEngine,
    SmartOrderRouter  sor
)
```

**`doWork()` implementation:**
- Polls `parentOrderSub` with a fragment limit of 10
- Returns the number of fragments processed (standard Aeron Agent duty cycle pattern)
- No allocation

**`onFragment()` implementation:**
- Reads message type byte at offset 0 of the inbound buffer
- If `ClusterMessageType.NEW_ORDER`: wraps the pre-allocated parent flyweight over the
  buffer at `ClusterMessageType.OFFSET_PAYLOAD`, calls `sor.route()`, dispatches each
  child via `childOrderPub`
- No allocation — reuse the pre-allocated `OrderFlyweight` and `UnsafeBuffer` from the
  constructor

**`roleName()`**: return `"algo-sor-agent"`

---

### Step 5 — Create OmsLauncher (new file in oms-launcher)

Create `oms-launcher/src/main/java/com/sellside/oms/launcher/OmsLauncher.java`.

This is the process entry point. It wires all modules together and starts the `AgentRunner`
threads pinned to specific CPU cores using Agrona's `AffinityThreadFactory` if available,
otherwise `new ThreadFactory()`.

**Responsibilities:**
1. Start an embedded Aeron `MediaDriver` (low-latency configuration)
2. Create an `Aeron` client instance
3. Create the Aeron IPC channels connecting `oms-core` ↔ `algo-sor`:
   - Channel: `"aeron:ipc"`, Stream ID 10 — parent order publication (oms-core → algo-sor)
   - Channel: `"aeron:ipc"`, Stream ID 11 — child order publication (algo-sor → oms-core)
4. Instantiate `AlgoSorAgent` with pre-allocated engines and the IPC pub/sub
5. Create an `AgentRunner` for `AlgoSorAgent` using `BusySpinIdleStrategy`
6. Start the `AgentRunner` on a dedicated thread
7. Add a JVM shutdown hook that closes the `AgentRunner`, `Aeron` client, and `MediaDriver`
   in reverse order

**Use these Aeron MediaDriver context settings for low-latency:**
```java
new MediaDriver.Context()
    .threadingMode(ThreadingMode.DEDICATED)
    .conductorIdleStrategy(new BusySpinIdleStrategy())
    .senderIdleStrategy(new BusySpinIdleStrategy())
    .receiverIdleStrategy(new BusySpinIdleStrategy())
    .dirDeleteOnStart(true)
    .dirDeleteOnShutdown(true)
```

**Class must have a `main(String[] args)` method.**

---

### Step 6 — Verify the Build

After all files are created and moved, run:

```bash
./gradlew clean build --info 2>&1 | tail -60
```

Fix any compilation errors. Common issues to watch for:
- Missing import for `ClusterMessageType` in `AlgoSorAgent` (it lives in `oms-core` which
  `algo-sor` must NOT depend on) — if this occurs, move `ClusterMessageType` to `oms-codec`
  and update the dependency accordingly
- Agrona version mismatch between modules — all modules must use identical versions

Re-run build after each fix. Do not stop until `BUILD SUCCESSFUL`.

---

### Step 7 — Verify the Architectural Boundary

Run this check to confirm `algo-sor` has no compile-time dependency on `oms-core`:

```bash
./gradlew :algo-sor:dependencies --configuration compileClasspath 2>&1 | grep "oms-core"
```

Expected output: no lines. If any lines appear, find the import causing the coupling and
resolve it by either moving the offending class to `oms-codec` or removing the dependency.

---

## Hard Constraints (never violate these)

1. **No object allocation on the hot path.** Every `doWork()` / `onFragment()` method must
   be free of `new` expressions. All buffers, flyweights, and engine instances are
   pre-allocated in constructors.

2. **`algo-sor` must NOT depend on `oms-core`.** The only shared module is `oms-codec`.
   Communication between them is exclusively via Aeron IPC channels at runtime.

3. **Package names are unchanged.** All classes keep their existing `com.sellside.oms.*`
   packages. Only filesystem and Gradle module boundaries change.

4. **No Spring, no CDI, no reflection frameworks.** This is a mechanical-sympathy system.
   All wiring is explicit constructor injection in `OmsLauncher`.

5. **Java 21 only.** Use `sealed` interfaces or `records` where they naturally improve
   clarity — but only if they introduce no allocation on the hot path.

6. **Run the full build and the boundary check before declaring done.** Do not stop at
   "code looks correct" — verify with the compiler and Gradle dependency report.

---

## Verification Criteria

The task is complete when ALL of the following are true:

- [ ] `./gradlew clean build` exits with `BUILD SUCCESSFUL` and zero warnings about
      cross-module dependencies
- [ ] `./gradlew :algo-sor:dependencies --configuration compileClasspath | grep oms-core`
      returns no output
- [ ] `./gradlew :oms-codec:dependencies --configuration compileClasspath | grep aeron-cluster`
      returns no output (codec must not pull in Aeron Cluster)
- [ ] `AlgoSorAgent.doWork()` and `onFragment()` contain no `new` expressions
- [ ] `OmsLauncher.main()` starts without throwing and logs the Aeron MediaDriver version
