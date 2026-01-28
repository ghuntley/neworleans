# Orleans Rust Port - Implementation Plan

Based on comprehensive technical specifications in `specs/`.

---

## Phase 1: Core Runtime Foundation

### 1.1 Grain Identity System
- Implement `GrainId` composite identifier (GrainType + IdSpan)
- Implement `GrainType` for type identification
- Implement `IdSpan` supporting Guid/long/string/compound keys
- Implement `ActivationId` for live grain instances
- Implement `GrainAddress` (SiloAddress + GrainId + ActivationId)

### 1.2 Binary Serialization Protocol
- Implement wire protocol with field headers (1-byte metadata)
- Implement WireType encoding (VarInt/Fixed/LengthDelimited/Reference)
- Implement VarInt encoding (7-bit continuation)
- Implement `IFieldCodec` trait for type serializers
- Implement `IDeepCopier` trait for deep copying
- Implement primitive codecs (integers, floats, bool, etc.)
- Implement string codec (UTF-8)
- Implement collection codecs (Vec, HashMap, etc.)
- Implement reference tracking for cyclic object graphs
- Implement `Writer` and `Reader` buffer abstractions
- Implement `PooledBuffer` for memory reuse
- Implement serializer session with session pool
- Implement version tolerance and delta encoding

### 1.3 Async Task Scheduling
- Implement `WorkItemGroup` per-activation scheduler
- Implement `ActivationTaskScheduler` for single-threaded execution
- Implement `IWorkItem` trait abstraction
- Implement closure work items (async/sync wrappers)
- ✅ Implement runtime context (thread-local grain context) - `orleans-runtime/src/runtime_context.rs`
- ✅ Implement `RequestContext` for async-local storage - `orleans-filters/src/request_context.rs`
- Implement message loop for activation processing
- Implement timeout and deadline handling

### 1.4 Grain Lifecycle
- Implement activation states (Invalid/Creating/Valid/Deactivating)
- Implement state transition machine
- Implement `OnActivateAsync`/`OnDeactivateAsync` hooks
- Implement deactivation reasons enum
- Implement activation collection (garbage collection)
- Implement collection age limits configuration
- Implement keep-alive mechanism

---

## Phase 2: Clustering & Distribution

### 2.1 Membership Protocol
- Implement `SiloAddress` (IP:Port + Epoch)
- Implement `SiloStatus` enum (Created/Joining/Active/Dead/etc.)
- Implement `MembershipEntry` with metadata
- Implement `MembershipTable` interface
- Implement `MembershipTableManager`
- Implement `TableVersion` for global versioning
- Implement `MembershipTableData` for cluster snapshots
- Implement `MembershipSnapshot` point-in-time views
- Implement suspect vote management
- Implement `MembershipAgent` for join/leave protocol

### 2.2 Failure Detection
- Implement `SiloHealthMonitor` for remote tracking
- Implement `LocalSiloHealthMonitor` for local tracking
- Implement `ProbeResult` health check responses
- Implement gossip protocol for state dissemination

### 2.3 Grain Directory
- Implement distributed grain directory
- Implement `GrainDirectoryPartition` per-silo segments
- Implement consistent hashing for partition assignment
- Implement `VirtualBucketsRingProvider` hash ring
- Implement `RingRange` for hash intervals
- Implement directory handoff on membership changes
- Implement `HandoffManager` for partition transfers
- Implement cache invalidation (piggyback mechanism)

### 2.4 Placement Strategies
- Implement `IPlacementDirector` trait
- Implement `RandomPlacementDirector`
- Implement `HashBasedPlacementDirector`
- Implement `PreferLocalPlacementDirector`
- Implement `ResourceOptimizedPlacementDirector`
- Implement version-aware placement

### 2.5 Messaging Infrastructure
- Implement core message structure (request/response)
- Implement `MessageFactory`
- Implement `MessageCenter` dispatcher
- Implement `ConnectionManager` for lifecycle
- Implement network connections (TCP)
- Implement message serialization for wire format
- Implement message routing
- Implement backpressure and flow control

---

## Phase 3: Persistence & Transactions

### 3.1 State Storage
- Implement `IGrainStorage` provider interface
- Implement grain state wrapper with metadata
- Implement `IStorage` high-level interface
- Implement state storage bridge for lifecycle
- Implement ETag model for versioning
- Implement optimistic concurrency control
- Implement `InconsistentStateException`
- Implement memory storage provider (for testing)
- Implement grain migration support
- Implement silo lifecycle integration

### 3.2 Production Storage Providers
- Implement PostgreSQL storage provider
- Implement AWS S3 storage provider
- Implement DynamoDB storage provider (optional)
- Implement ADO.NET-style SQL provider (optional)

### 3.3 ACID Transactions
- Implement two-phase commit (2PC) protocol
- Implement `TransactionAgent` (per-grain coordinator)
- Implement `TransactionManager` (global coordinator)
- Implement TM election mechanism
- Implement transactional state wrapper
- Implement `TransactionInfo` metadata
- Implement read-only transactions (1-phase optimization)
- Implement read-write transactions (full 2-phase)
- Implement lock groups for serializable batching
- Implement conflict detection
- Implement copy-on-write semantics
- Implement causal clock for ordering
- Implement transaction status codes
- Implement exception hierarchy
- Implement storage batch processing

---

## Phase 4: Advanced Features

### 4.1 Streaming
- Implement `StreamId` identity
- Implement `StreamSequenceToken` for checkpointing
- Implement `IAsyncStream` read/write interface
- Implement `IAsyncObserver` event handler
- Implement `IAsyncBatchObserver` for batches
- Implement `StreamImpl`
- Implement subscription handles
- Implement pub/sub system
- Implement `IStreamProvider` interface
- Implement memory stream provider
- Implement persistent stream provider
- Implement `IQueueAdapter` for external queues
- Implement `IBatchContainer`
- Implement explicit pub/sub
- Implement implicit pub/sub (attribute-based)
- Implement pulling agent
- Implement flow control and backpressure
- Implement queue cache
- Implement broadcast channels

### 4.2 Timers & Reminders
- Implement `IGrainTimer` interface
- Implement timer characteristics (non-persistent, activation-scoped)
- Implement `RegisterTimer` API
- Implement timer internal mechanics
- Implement `IRemindable` interface
- Implement `IReminderRegistry`
- Implement `ReminderEntry` data structure
- Implement `IReminderTable` interface
- Implement reminder service execution engine
- Implement `RegisterOrUpdateReminder` API
- Implement memory reminder table (for testing)

### 4.3 Call Filters & Interceptors
- Implement `IIncomingGrainCallFilter` (server-side)
- Implement `IOutgoingGrainCallFilter` (client-side)
- Implement `IGrainCallContext`
- Implement filter pipeline architecture
- Implement incoming call pipeline
- Implement outgoing call pipeline
- Implement filter registration
- Implement `RequestContext` static access
- Implement context propagation across calls
- Implement response handling
- Implement activity propagation filter (tracing)
- Implement per-grain filters
- Implement common patterns (logging, auth, exceptions)

### 4.4 Versioning & Compatibility
- Implement version attribute system
- Implement version storage
- Implement `GrainVersionManifest`
- Implement strict version compatible strategy
- Implement backward compatible strategy
- Implement all versions compatible strategy
- Implement minimum version selector
- Implement latest version selector
- Implement all compatible versions selector
- Implement `CachedVersionSelectorManager`
- Implement rolling upgrade workflow support

### 4.5 Stateless Workers
- Implement `StatelessWorker` attribute
- Implement stateless worker placement strategy
- Implement `StatelessWorkerGrainContext`
- Implement message routing algorithm for workers
- Implement load balancing
- Implement `StatelessWorkerDirector`
- Implement per-silo worker load balancing
- Implement configuration options
- Implement idle worker removal (adaptive pooling)
- Implement PID controller for scaling

### 4.6 Observers & Callbacks
- Implement `IGrainObserver` interface
- Implement `CreateObjectReference` pattern
- Implement `ObserverGrainId` structure
- Implement observer storage (weak references)
- Implement `LocalObjectData`
- Implement `InvokableObjectManager`
- Implement `ObserverManager<T>` helper
- Implement copy-on-write snapshots
- Implement notification patterns (async/sync)
- Implement one-way calls (fire-and-forget)
- Implement `InvokeMethodOptions`
- Implement `[OneWay]` attribute
- Implement observer deletion
- Implement `ClientObserversPlacementDirector`
- Implement message dispatch flow

---

## Phase 5: Testing & Tooling

### 5.1 Test Infrastructure
- Implement `TestCluster` framework
- Implement test silo configuration
- Implement test grain interfaces
- Implement mock storage providers
- Implement test utilities

### 5.2 Test Suites
- Implement serialization tests (spec 18)
- Implement grain lifecycle tests (spec 19)
- Implement clustering tests (spec 20)
- Implement streaming tests (spec 21)
- Implement transaction tests (spec 22)
- Implement placement tests (spec 23)
- Implement timer/reminder tests (spec 24)
- Implement observer/callback tests (spec 25)
- Implement persistence storage tests (spec 26)
- Implement codegen/analyzer tests (spec 27)
- Implement security/TLS tests (spec 28)
- Implement extension provider tests (spec 29)
- Implement DI tests (spec 30)
- Implement distributed chaos tests (spec 31)
- Implement journaling tests (spec 32)
- Implement scheduler tests (spec 33)
- Implement directory tests (spec 34)
- Implement membership tests (spec 35)
- Implement caching tests (spec 36)
- Implement integration tests (spec 37)
- Implement default cluster tests (spec 38)
- Implement codec tests (spec 39)
- Implement buffer tests (spec 40)
- Implement internal test utilities (spec 41)
- Implement benchmark tests (spec 43)

### 5.3 Code Generation
- Implement proc macro crate structure
- Implement `#[derive(GrainSerialize)]` macro
- Implement `#[grain]` attribute macro
- Implement codec generation
- Implement copier generation
- Implement proxy generation (client stubs)
- Implement invoker generation (server dispatchers)
- Implement activator generation (grain factories)
- Implement metadata generation

### 5.4 Client/Silo Architecture
- Implement `IClusterClient` interface
- Implement `ISiloHost` interface
- Implement `ClusterClient`
- Implement `Silo`
- Implement `OutsideRuntimeClient` (external client)
- Implement `InsideRuntimeClient` (silo-side)
- Implement lifecycle management
- Implement lifecycle stages
- Implement default services registration
- Implement client services DI
- Implement silo services DI
- Implement configuration options
- Implement gateway
- Implement startup/shutdown sequences
- Implement message flow (client-to-grain routing)

---

## Phase 6: Production Hardening

### 6.1 Security
- Implement TLS for network connections
- Implement authentication mechanisms
- Implement authorization filters

### 6.2 Observability
- Implement distributed tracing integration
- Implement metrics collection
- Implement structured logging

### 6.3 Performance
- Run benchmarks against .NET Orleans
- Profile and optimize hot paths
- Tune memory allocations
- Optimize serialization

### 6.4 Documentation
- Write API documentation
- Write migration guide from .NET Orleans
- Write architecture overview
- Create examples and tutorials

---

## Crate Structure

```
orleans-rs/
├── orleans-core/           # Core types and traits
├── orleans-runtime/        # Runtime implementation
├── orleans-serialization/  # Serialization framework
├── orleans-persistence/    # Storage providers
├── orleans-transactions/   # Transaction support
├── orleans-streaming/      # Streaming framework
├── orleans-reminders/      # Reminder service
├── orleans-testing/        # Test infrastructure
└── orleans-codegen/        # Proc macros
```

---

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `serde` | Serialization traits |
| `bytes` | Buffer management |
| `dashmap` | Concurrent hash maps |
| `parking_lot` | Synchronization primitives |
| `crossbeam` | Lock-free data structures |
| `uuid` | UUID generation |
| `tracing` | Logging and diagnostics |
| `async-trait` | Async trait support |

---

## Success Criteria

- All test suites passing
- Feature parity with .NET Orleans core functionality
- Performance within 20% of .NET Orleans
- Clean API that feels native to Rust
- Comprehensive documentation
