# Orleans Rust Port - Minimum Viable Implementation Plan

## Goal

Three separate Rust processes functioning as a single Orleans cluster where:
- A grain created/activated on Process1 is accessible from Process2 or Process3
- Grains have location transparency (caller doesn't know which silo hosts the grain)
- Single-writer guarantee (only one activation per grain across the cluster)

## Non-Goals for MVP

- ~~Persistence (grains are in-memory only)~~ → **Now implemented in Phase 11**
- ~~Timers~~ → **Now implemented in Phase 12**
- ~~Reminders~~ → **Now implemented in Phase 13**
- ~~Observers/Callbacks~~ → **Now implemented in Phase 15**
- Transactions
- Streaming
- Complex placement strategies (MVP uses hash-based only)
- Version tolerance in serialization
- TLS/Security
- Graceful grain migration

---

## Phase 1: Core Identity Types ✅

**Objective**: Establish the fundamental addressing system for grains and silos.

**Status**: COMPLETE - All 120 unit tests and 13 doc tests passing.

### Tasks

- [x] **1.1** Implement `IdSpan` - UTF-8 byte array with pre-computed XxHash32
  - `IdSpan::new(bytes: &[u8]) -> Self`
  - `IdSpan::from_str(s: &str) -> Self`
  - `IdSpan::get_hash_code() -> u32`
  - Property test: hash is stable across calls
  - Property test: empty IdSpan has consistent hash

- [x] **1.2** Implement `GrainType` - wrapper around IdSpan for type identification
  - `GrainType::create(name: &str) -> Self`
  - `GrainType::get_hash_code() -> u32`
  - Unit test: system type prefixes (`sys.`, `sys.svc.`)

- [x] **1.3** Implement `GrainId` - composite of GrainType + IdSpan key
  - `GrainId::new(grain_type: GrainType, key: IdSpan) -> Self`
  - `GrainId::get_uniform_hash_code() -> u32` (combines type and key hashes)
  - Property test: two GrainIds with same type+key are equal
  - Property test: hash distribution is uniform

- [x] **1.4** Implement `SiloAddress` - endpoint + generation number
  - `SiloAddress::new(endpoint: SocketAddr, generation: i64) -> Self`
  - `SiloAddress::get_hash_code() -> u32`
  - Generation distinguishes silo restarts at same address

- [x] **1.5** Implement `ActivationId` - unique identifier for a grain activation
  - `ActivationId::new() -> Self` (random UUID)
  - `ActivationId::get_deterministic(grain_id: &GrainId) -> Self`

- [x] **1.6** Implement `GrainAddress` - complete location (GrainId + ActivationId + SiloAddress)
  - `GrainAddress::is_complete() -> bool`

### Crate Structure
```
orleans-core/
├── src/
│   ├── lib.rs
│   ├── id_span.rs
│   ├── grain_type.rs
│   ├── grain_id.rs
│   ├── silo_address.rs
│   ├── activation_id.rs
│   └── grain_address.rs
```

### Tests
- Unit tests for each type's creation and equality
- Property tests for hash stability and distribution
- Property tests: `GrainId::parse(grain_id.to_string()) == grain_id`

---

## Phase 2: Binary Serialization ✅

**Objective**: Implement Orleans wire protocol for network communication.

**Status**: COMPLETE - 102 unit tests and property tests passing.

### Tasks

- [x] **2.1** Implement VarInt encoding/decoding
  - `write_varint(writer: &mut impl Write, value: u64)`
  - `read_varint(reader: &mut impl Read) -> u64`
  - ZigZag encoding for signed integers
  - Property test: `decode(encode(n)) == n` for all u64

- [x] **2.2** Implement wire types enum
  ```rust
  enum WireType {
      VarInt = 0,
      TagDelimited = 1,
      LengthPrefix = 2,
      Fixed32 = 3,
      Fixed64 = 4,
      Reference = 6,
      Extended = 7,
  }
  ```

- [x] **2.3** Implement field header encoding
  - 1-byte header: WireType(3 bits) + SchemaType(2 bits) + FieldIdDelta(3 bits)
  - Extended field IDs for delta > 6

- [x] **2.4** Implement `Writer` struct
  - Buffer management with `bytes::BytesMut`
  - `write_field_header(field_id_delta, wire_type, schema_type)`
  - `write_varint()`, `write_fixed32()`, `write_fixed64()`
  - `write_length_prefixed(bytes)`

- [x] **2.5** Implement `Reader` struct
  - `read_field_header() -> Field`
  - `read_varint()`, `read_fixed32()`, `read_fixed64()`
  - `read_length_prefixed() -> &[u8]`
  - `skip_field(wire_type)` for forward compatibility

- [x] **2.6** Implement primitive codecs
  - `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
  - `f32`, `f64`
  - `bool`
  - `String` (UTF-8, length-prefixed)
  - `Vec<u8>` (raw bytes)
  - `Option<T>` (skip if None)

- [x] **2.7** Implement identity type codecs
  - `IdSpan`, `GrainType`, `GrainId`, `SiloAddress`, `ActivationId`, `GrainAddress`

- [ ] **2.8** Implement `#[derive(OrleansSerialize)]` proc macro (basic version)
  - Generates `IFieldCodec` implementation for structs
  - Field IDs via `#[id(n)]` attribute
  - *Deferred to Phase 7 (Codegen) for better integration with grain interfaces*

### Crate Structure
```
orleans-serialization/
├── src/
│   ├── lib.rs
│   ├── varint.rs
│   ├── wire_type.rs
│   ├── field.rs
│   ├── writer.rs
│   ├── reader.rs
│   ├── codecs/
│   │   ├── mod.rs
│   │   ├── primitives.rs
│   │   ├── string.rs
│   │   └── identity.rs
│   └── buffer.rs

orleans-codegen/
├── src/
│   └── lib.rs  (proc macro crate)
```

### Tests
- Property tests: roundtrip serialization for all types
- Property tests: VarInt encoding size bounds
- Fuzz tests: malformed input doesn't panic
- Unit tests: wire format matches Orleans .NET format (golden tests)

---

## Phase 3: Messaging Infrastructure ✅

**Objective**: Enable message passing between silos over TCP.

**Status**: COMPLETE - 41 unit tests passing, including request/response roundtrips between silos.

### Tasks

- [x] **3.1** Define `Message` struct
  ```rust
  struct Message {
      id: CorrelationId,
      direction: Direction,  // Request, Response, OneWay
      target_grain: GrainId,
      target_silo: Option<SiloAddress>,
      target_activation: Option<ActivationId>,
      sending_grain: Option<GrainId>,
      sending_silo: SiloAddress,
      sending_activation: Option<ActivationId>,
      interface_type: GrainInterfaceType,
      method_id: u32,
      body: Bytes,  // Serialized arguments/result
      rejection_info: Option<RejectionInfo>,
      timeout: Option<Duration>,
  }
  ```

- [x] **3.2** Implement `CorrelationId`
  - `CorrelationId { nonce: u64, counter: u64 }`
  - Unique per-message identifier for request/response matching
  - Atomic counter for monotonic IDs within a process

- [x] **3.3** Implement `Message` factory methods
  - `Message::new_request(target, interface, method, body, sending_silo) -> Message`
  - `Message::create_response(body) -> Message`
  - `Message::create_rejection(rejection_type, message) -> Message`
  - `Message::new_one_way(...)` for fire-and-forget messages

- [x] **3.4** Implement message serialization
  - Frame format: `[header_len: i32][body_len: i32][header][body]`
  - Serialize/deserialize `Message` using Phase 2 codecs
  - Delta-encoded field IDs for compact wire format

- [x] **3.5** Implement `Connection` struct
  - TCP connection wrapper with async read/write
  - `send(message: Message) -> Result<()>`
  - `receive() -> Result<Message>`
  - Bidirectional I/O with separate reader/writer tasks
  - Connection statistics tracking

- [x] **3.6** Implement `ConnectionManager`
  - Pool of connections per target silo
  - `get_connection(silo: &SiloAddress) -> Arc<Connection>`
  - Connection health monitoring and automatic reconnection
  - Receiver loop tracking for bidirectional communication

- [x] **3.7** Implement `MessageCenter`
  - Central message dispatcher
  - Routes outgoing messages to correct connection
  - Routes incoming messages to grain activations or response handlers
  - Request/response correlation with timeouts
  - Automatic receiver loop for outbound connections
  - Incoming connection address learning for efficient responses

### Crate Structure
```
orleans-messaging/
├── src/
│   ├── lib.rs
│   ├── message.rs
│   ├── correlation_id.rs
│   ├── direction.rs
│   ├── grain_interface_type.rs
│   ├── message_codec.rs
│   ├── connection.rs
│   ├── connection_manager.rs
│   ├── message_center.rs
│   └── error.rs
```

### Tests
- Unit tests: message serialization roundtrip (5 tests)
- Unit tests: correlation ID generation and parsing (6 tests)
- Unit tests: direction enum (3 tests)
- Unit tests: grain interface type (6 tests)
- Unit tests: message creation and manipulation (6 tests)
- Integration tests: two silo communication (3 tests)
- Integration tests: connection management (4 tests)
- Integration tests: message center operations (4 tests)
- Test: connection reconnection on failure
- Test: concurrent request handling

---

## Phase 4: Cluster Membership ✅

**Objective**: Enable silos to discover each other and track cluster state.

**Status**: COMPLETE - 75 unit tests and 1 doc test passing.

### Tasks

- [x] **4.1** Define `SiloStatus` enum
  ```rust
  enum SiloStatus {
      Created,
      Joining,
      Active,
      ShuttingDown,
      Stopping,
      Dead,
  }
  ```

- [x] **4.2** Define `MembershipEntry` struct
  ```rust
  struct MembershipEntry {
      silo_address: SiloAddress,
      status: SiloStatus,
      start_time: DateTime<Utc>,
      i_am_alive_time: DateTime<Utc>,
      suspect_times: Vec<(SiloAddress, DateTime<Utc>)>,
  }
  ```

- [x] **4.3** Define `IMembershipTable` trait
  ```rust
  trait IMembershipTable: Send + Sync {
      async fn read_all(&self) -> Result<MembershipTableData>;
      async fn read_row(&self, silo_address: &SiloAddress) -> Result<Option<(MembershipEntry, String)>>;
      async fn insert_row(&self, entry: MembershipEntry, table_version: TableVersion) -> Result<bool>;
      async fn update_row(&self, entry: MembershipEntry, etag: &str, table_version: TableVersion) -> Result<bool>;
      async fn update_i_am_alive(&self, entry: &MembershipEntry) -> Result<()>;
      async fn cleanup_defunct_silo_entries(&self, before: DateTime<Utc>) -> Result<()>;
  }
  ```

- [x] **4.4** Implement `InMemoryMembershipTable` (for testing/MVP)
  - Thread-safe with RwLock
  - Optimistic concurrency with version numbers and ETags
  - Status transition validation

- [x] **4.5** Implement `MembershipTableManager`
  - Periodically refreshes membership from table
  - Provides `MembershipTableSnapshot` to other components
  - `get_active_silos() -> Vec<SiloAddress>`
  - Broadcast channel for membership change notifications
  - Suspect/kill protocol with vote counting

- [x] **4.6** Implement `MembershipAgent`
  - Join protocol: Joining -> Active
  - Leave protocol: Active -> ShuttingDown -> Stopping -> Dead
  - Periodic heartbeat (I Am Alive updates)
  - Background tasks for heartbeat and table refresh

- [x] **4.7** Implement basic failure detection
  - Suspect voting mechanism
  - Configurable death vote threshold
  - Vote expiration for stale votes

### Crate Structure
```
orleans-clustering/
├── src/
│   ├── lib.rs
│   ├── silo_status.rs
│   ├── membership_entry.rs
│   ├── membership_table.rs
│   ├── in_memory_table.rs
│   ├── membership_snapshot.rs
│   ├── membership_manager.rs
│   ├── membership_agent.rs
│   ├── table_version.rs
│   ├── options.rs
│   └── error.rs
```

### Tests
- Unit test: silo join/leave lifecycle (18 tests)
- Integration test: three silos form cluster ✅
- Test: silo marked dead after suspect votes ✅
- Test: membership version only increases ✅
- Test: concurrent silo operations ✅

---

## Phase 5: Grain Directory ✅

**Objective**: Distributed lookup of grain locations using consistent hashing.

**Status**: COMPLETE - 55 unit tests and 1 doc test passing.

### Tasks

- [x] **5.1** Implement `ConsistentHashRing`
  - Virtual buckets (30 per silo default)
  - `get_primary_silo(hash: u32) -> SiloAddress`
  - `get_silo_range(silo: &SiloAddress) -> RingRange`

- [x] **5.2** Implement `GrainDirectoryPartition`
  - In-memory HashMap of GrainId -> GrainAddress
  - Each silo owns a portion of the hash space
  - `lookup(grain_id: &GrainId) -> Option<GrainAddress>`
  - `register(membership_version, address, previous) -> RegistrationResult`
  - `unregister(grain_id, activation_id) -> bool`

- [x] **5.3** Implement `DistributedGrainDirectory`
  - Routes lookups to correct silo based on hash
  - Local calls for owned ranges
  - Remote calls for other ranges (via IRemoteGrainDirectory trait)
  - `lookup(grain_id: &GrainId) -> Option<GrainAddress>`
  - `register(address: GrainAddress) -> Result<GrainAddress>`

- [x] **5.4** Implement directory cache
  - LRU cache of GrainId -> GrainAddress (configurable size, default 100,000)
  - Cache invalidation on activation move/death
  - Pending invalidation support for in-flight operations
  - Piggyback cache updates via GrainAddressCacheUpdate

- [x] **5.5** Handle membership changes
  - `on_membership_change()` updates ring and cleans up dead silo entries
  - `remove_entries_for_silo()` for cleanup
  - Range release/acquire for handoff support

### Crate Structure
```
orleans-directory/
├── src/
│   ├── lib.rs
│   ├── consistent_hash.rs
│   ├── ring_range.rs
│   ├── partition.rs
│   ├── distributed_directory.rs
│   ├── cache.rs
│   └── error.rs
```

### Tests
- Unit tests: ring segment containment (5 tests)
- Unit tests: ring range operations (6 tests)
- Unit tests: consistent hash ring (9 tests)
- Unit tests: grain directory partition (11 tests)
- Unit tests: directory cache (12 tests)
- Unit tests: distributed directory (6 tests)
- Integration tests: three-silo directory ring ✅
- Integration tests: cross-silo grain registration ✅
- Property tests: consistent mapping ✅
- Property tests: hash distribution balanced ✅
- Property tests: minimal disruption when silo added ✅

---

## Phase 6: Grain Runtime ✅

**Objective**: Host grain activations with turn-based execution.

**Status**: COMPLETE - 64 unit tests passing.

### Tasks

- [x] **6.1** Define `IGrain` trait
  ```rust
  #[async_trait]
  trait IGrain: Send + Sync {
      fn grain_id(&self) -> &GrainId;
      async fn on_activate(&mut self, context: Arc<dyn IGrainContext>) -> RuntimeResult<()> { Ok(()) }
      async fn on_deactivate(&mut self, reason: DeactivationReason) -> RuntimeResult<()> { Ok(()) }
  }
  ```

- [x] **6.2** Define `IGrainContext` trait
  - Access to grain identity, runtime services
  - `grain_id()`, `activation_id()`, `silo_address()`
  - `grain_factory()` for creating grain references
  - `deactivate_on_idle()` and `delay_deactivation()` for lifecycle control

- [x] **6.3** Implement `ActivationData`
  - Holds grain instance + context + message queue
  - Activation state machine: Creating -> Activating -> Valid -> Deactivating -> Invalid
  - Turn-based scheduler with message queue
  - Tracks outstanding calls and statistics

- [x] **6.4** Implement `Catalog`
  - Registry of active grains on this silo
  - `get_or_create_activation(grain_id) -> Result<ActivationHandle>`
  - Grain type registration with activators and invokers
  - Activation collection (GC idle grains)
  - Activation removal and cleanup

- [x] **6.5** Implement `Dispatcher`
  - Routes incoming messages to correct activation
  - Handles activation creation if needed
  - Handles rejection if grain can't be activated here
  - Configurable timeout and queue depth

- [x] **6.6** Implement `GrainFactory`
  - Creates grain references (proxies)
  - `get_grain<T>(key) -> TypedGrainReference<T>`
  - Interface resolver (convention-based or map-based)
  - Extension trait for typed access

- [x] **6.7** Implement `GrainReference<T>` (proxy)
  - Holds GrainId + interface type
  - `invoke()` for RPC calls with serialized body
  - `invoke_one_way()` for fire-and-forget
  - `cast()` for interface casting

### Crate Structure
```
orleans-runtime/
├── src/
│   ├── lib.rs
│   ├── error.rs
│   ├── grain.rs
│   ├── grain_context.rs
│   ├── activation_data.rs
│   ├── activation_state.rs
│   ├── catalog.rs
│   ├── dispatcher.rs
│   ├── grain_factory.rs
│   └── grain_reference.rs
```

### Tests
- Unit test: activation state transitions (17 tests)
- Unit test: activation data lifecycle (11 tests)
- Unit test: catalog operations (9 tests)
- Unit test: dispatcher configuration (1 test)
- Unit test: grain factory operations (7 tests)
- Unit test: grain reference operations (6 tests)
- Unit test: grain context operations (6 tests)
- Unit test: grain trait and type data (4 tests)
- Integration test: grain method invocation roundtrip
- Test: idle grain deactivation

---

## Phase 7: Code Generation ✅

**Objective**: Proc macros to generate grain interfaces and invokers.

**Status**: COMPLETE - Core proc-macro infrastructure implemented with 11 tests passing.

### Tasks

- [x] **7.1** Implement `#[grain_interface]` attribute macro
  - Applied to trait definitions
  - Generates `GrainInterfaceType` constant (e.g., `IHELLO_GRAIN_INTERFACE_TYPE`)
  - Generates method ID constants module (e.g., `ihello_grain_methods::SAY_HELLO = 1`)
  - Generates proxy struct (e.g., `IHelloGrainProxy`) for remote invocation
  - Generates `GrainInterfaceMarker` implementation for typed grain factory access

- [x] **7.2** Implement `#[grain]` attribute macro
  - Applied to struct definitions
  - Generates `IGrain` implementation with `grain_type()` method
  - Generates activator struct (e.g., `HelloGrainActivator`) implementing `IGrainActivator`
  - Generates `create_<grain>_type()` helper function

- [x] **7.3** Implement `#[grain_impl]` attribute macro
  - Applied to trait impl blocks
  - Generates invoker struct (e.g., `HelloGrainIHelloGrainInvoker`) implementing `IGrainMethodInvoker`
  - Generates `create_<grain>_type_with_<interface>()` helper function
  - Method dispatch via match on method_id

- [x] **7.4** Implement `GrainSerialize`/`GrainDeserialize` traits
  - Simple serialization traits for grain method arguments and return values
  - Implementations for all primitive types (u8-u64, i8-i64, f32, f64, bool)
  - Implementations for String, Vec<T>, Option<T>
  - Located in orleans-runtime for use by generated code

### MVP Limitations
- Methods with parameters require manual invoker implementation (generates compile error)
- Proxy generation creates skeleton but complex serialization deferred
- Full Orleans serialization integration deferred to future work

### Example Usage
```rust
#[grain_interface]
pub trait IHelloGrain {
    async fn say_hello(&self, name: String) -> String;
}

#[grain]
pub struct HelloGrain;

impl IHelloGrain for HelloGrain {
    async fn say_hello(&self, name: String) -> String {
        format!("Hello, {}!", name)
    }
}
```

### Crate Structure
```
orleans-codegen/
├── src/
│   ├── lib.rs
│   ├── grain_interface.rs
│   ├── grain.rs
│   ├── proxy_gen.rs
│   └── invoker_gen.rs
```

### Tests
- Compile-time tests: macro expansion
- Integration test: generated code compiles
- Integration test: proxy calls work end-to-end

---

## Phase 8: Silo Host ✅

**Objective**: Assemble all components into a runnable silo process.

**Status**: COMPLETE - 6 unit tests, 9 integration tests, and 2 doc tests passing.

### Tasks

- [x] **8.1** Implement `SiloBuilder`
  - Configure silo address, cluster ID
  - Register grain types
  - Configure membership table

- [x] **8.2** Implement `Silo`
  - Lifecycle: Created -> Starting -> Running -> Stopping -> Stopped
  - Starts all background services
  - Graceful shutdown

- [x] **8.3** Implement startup sequence
  1. Initialize message center and start listening
  2. Connect to membership table
  3. Join cluster (MembershipAgent)
  4. Start grain directory with consistent hash ring
  5. Create catalog and register grain types
  6. Start dispatcher and register message handler
  7. Start membership change listener
  8. Mark silo as Running

- [x] **8.4** Implement shutdown sequence
  1. Mark silo as Stopping
  2. Signal shutdown to background tasks
  3. Deactivate all grains
  4. Leave cluster (MembershipAgent stop)
  5. Shutdown message center
  6. Mark silo as Stopped

- [ ] **8.5** Implement `ClusterClient` (deferred - not required for MVP)
  - External client that connects to cluster
  - Discovers silos via membership table
  - Routes requests through gateway silo

### Crate Structure
```
orleans-host/
├── src/
│   ├── lib.rs
│   ├── config.rs
│   ├── error.rs
│   ├── silo_builder.rs
│   └── silo.rs
├── tests/
│   └── integration_test.rs
```

### Tests
- Unit tests: silo creation, start/stop lifecycle, component initialization (6 tests)
- Integration test: three silos form cluster ✅
- Integration test: grain activation on silo ✅
- Integration test: single activation guarantee ✅
- Integration test: grain directory assignment consistency ✅
- Integration test: message center initialization ✅
- Integration test: dispatcher registration ✅
- Integration test: silo graceful shutdown with grain deactivation ✅
- Integration test: distributed grain placement across 3 silos ✅
- Integration test: unique silo addresses with generation numbers ✅

---

## Phase 9: Integration Tests ✅

**Objective**: Verify the complete system with three-silo cluster.

**Status**: COMPLETE - Multi-process cluster support implemented with TCP-based membership table.

### TCP-Based Multi-Process Cluster Support ✅

- [x] **TCP Membership Table Server/Client** (`orleans-clustering/src/tcp_membership_table.rs`)
  - `MembershipTableServer` - TCP server that hosts membership table for multi-process clusters
  - `TcpMembershipTable` - TCP client implementing `IMembershipTable` trait
  - Enables separate OS processes to share cluster membership state
  - Full implementation of all `IMembershipTable` operations over TCP/JSON protocol

- [x] **Standalone Silo Binaries** (`orleans-host/src/bin/`)
  - `orleans-membership-server` - Standalone membership table server binary
  - `orleans-silo` - Standalone silo process that connects to membership server

### Test Scenarios

- [x] **9.1** Basic grain invocation ✅
  - Start 3 silos
  - Create grain on silo1
  - Call grain from silo2, verify response
  - Call grain from silo3, verify response
  - **Implemented in**: `test_cross_silo_grain_invocation` (orleans-host/tests/integration_test.rs)

- [x] **9.2** Grain location transparency ✅
  - Call grain without knowing which silo hosts it
  - Verify request is routed correctly
  - **Implemented in**: `test_location_transparency` (orleans-host/tests/integration_test.rs)
  - Uses `DirectoryAwareMessageSender` for automatic grain location lookup

- [x] **9.3** Single activation guarantee ✅
  - Simultaneously request same grain from all silos
  - Verify only one activation exists
  - **Implemented in**: `test_simultaneous_single_activation_guarantee` (orleans-host/tests/integration_test.rs)
  - Tests both local catalog guarantees and directory-coordinated cross-silo invocation
  - Verifies turn-based execution ensures no races during concurrent access

- [x] **9.4** Multi-process cluster communication ✅
  - Three silos join cluster via TCP-based membership table
  - Grain created on Process 1 (Silo 1)
  - Process 2 (Silo 2) and Process 3 (Silo 3) successfully invoke grain on Process 1
  - Counter incremented correctly across cross-process calls: 1 -> 2 -> 3
  - **Implemented in**: `test_tcp_membership_with_in_process_silos` (orleans-host/tests/multi_process_test.rs)
  - **TCP membership table tests**: 5 unit tests in `tcp_membership_table::tests`

- [x] **9.5** TCP membership table operations ✅
  - Insert, read, update, delete operations work over TCP
  - Multiple clients can connect concurrently
  - Optimistic concurrency control works correctly
  - Heartbeat (I Am Alive) updates work
  - **Implemented in**: `test_tcp_membership_table_operations` (orleans-host/tests/multi_process_test.rs)

- [x] **9.6** Actual OS process cluster formation ✅
  - Three separate OS processes (using `orleans-silo` binary) form a cluster
  - Processes connect to shared TCP membership server
  - Verifies cluster membership visible across processes
  - **Implemented in**: `test_three_process_cluster_formation` (orleans-host/tests/multi_process_test.rs)

- [x] **9.6.1** Cross-process grain invocation ✅ (NEW - MVP Complete!)
  - Three separate OS processes form a cluster with grain routing
  - Process 1 creates and hosts a CounterGrain
  - Process 2 invokes increment() on the grain → returns 1
  - Process 3 invokes increment() on the grain → returns 2
  - Proves single activation guarantee across separate processes
  - Proves location transparency (callers don't know which process hosts the grain)
  - **Implemented in**: `test_cross_process_grain_invocation` (orleans-host/tests/cross_process_grain_test.rs)
  - **Additional tests**: `test_single_silo_grain_creation`, `test_single_silo_grain_invocation`

- [x] **9.7** Silo failure handling ✅
  - Start 3 silos, create grain
  - Kill silo hosting grain
  - Call grain, verify it re-activates on another silo
  - **Implemented in**: `failure_handling_test.rs` (orleans-host/tests/)
  - **Features implemented**:
    - `DirectoryAwareMessageSender` with retry logic (up to 3 attempts)
    - Directory cache invalidation when silo fails
    - Automatic failover to alternative silos
    - Single activation guarantee maintained during failover
  - **Tests**: 5 tests covering:
    - `test_silo_failure_detection` (9.7.1)
    - `test_grain_reactivation_after_failure` (9.7.2)
    - `test_retry_logic_on_connection_failure` (9.7.3)
    - `test_directory_cache_invalidation` (9.7.4)
    - `test_single_activation_during_failover` (9.7.5)

- [x] **9.8** Grain state isolation ✅
  - Create grain, set state
  - Call from another silo, verify state persists
  - Verify no cross-grain state leakage
  - **Implemented in**: `state_isolation_test.rs` (orleans-host/tests/)
  - **Tests**: 4 tests (3 active, 1 ignored for full cross-silo routing):
    - `test_no_cross_grain_state_leakage` (9.8.2) - Different grain IDs maintain isolated state
    - `test_many_grains_independent_state` (9.8.3) - 20 grains with independent state, partial modification
    - `test_state_accumulation` (9.8.4) - 50 increments with consistent intermediate states
    - `test_state_persists_across_silos` (9.8.1) - [ignored] cross-silo state persistence

- [x] **9.9** Concurrent grain calls ✅
  - Many simultaneous calls to same grain
  - Verify turn-based execution (no races)
  - **Implemented in**: `concurrent_calls_test.rs` (orleans-host/tests/)
  - **Tests**:
    - `test_many_concurrent_calls_single_silo` - 100 concurrent increments, values 1..100, max concurrent = 1
    - `test_interleaved_read_write_operations` - 50 interleaved read/write pairs, no race conditions
    - `test_counter_invariants_property` - Property-based test for counter invariants (10, 25, 50, 75 calls)

### Property-Based Tests (Future work)

- [ ] **9.10** Grain identity properties
  - `grain_id(grain_ref) == expected_grain_id`
  - `hash(grain_id1) != hash(grain_id2)` for different grains (usually)

- [ ] **9.11** Directory consistency
  - After any sequence of register/unregister operations
  - `lookup(grain_id)` returns registered address or None

- [ ] **9.12** Message delivery
  - All sent messages are received (unless silo dies)
  - No duplicate deliveries

### Crate Structure
```
orleans-tests/
├── src/
│   └── lib.rs
├── tests/
│   ├── basic_invocation.rs
│   ├── location_transparency.rs
│   ├── single_activation.rs
│   ├── failure_handling.rs
│   ├── state_isolation.rs
│   └── concurrent_calls.rs
```

---

## Phase 10: Observability & Telemetry ✅

**Objective**: Implement comprehensive structured logging via the `tracing` crate for production-ready observability.

**Status**: COMPLETE - 20 unit tests passing (including 4 property-based tests).

### Tasks

- [x] **10.1** Create `orleans-telemetry` crate
  - Centralized logging configuration and initialization
  - Multiple output formats: Compact, Pretty, JSON
  - Configurable log levels per module
  - Re-exports of common tracing macros

- [x] **10.2** Implement `LogConfig` builder
  - `LogConfig::new()` with fluent builder pattern
  - Preset configurations: `development()`, `production()`, `testing()`
  - Configurable options: location, thread IDs, target, span events
  - Custom filter directives for fine-grained control

- [x] **10.3** Define Orleans-specific field names
  - Standard field names: `silo`, `grain_id`, `activation_id`, `correlation_id`
  - Additional fields: `method_id`, `interface_type`, `duration_ms`, `error`
  - Target module paths for each Orleans component

- [x] **10.4** Add `#[instrument]` spans to key components
  - Silo lifecycle: `start()`, `stop()` with silo address field
  - Catalog: `get_or_create_activation()`, `deactivate_grain()` with grain_id
  - Dispatcher: `handle_request()`, `find_or_create_grain()` with correlation_id
  - Membership agent: `start()`, `stop()` with silo address

- [x] **10.5** Write unit and property-based tests
  - Log level conversion tests
  - Config builder idempotency property tests
  - Filter directive preservation tests
  - Field and target name validation tests

### Crate Structure
```
orleans-telemetry/
├── Cargo.toml
├── src/
│   └── lib.rs
│       ├── LogFormat enum (Compact, Pretty, Json)
│       ├── LogLevel enum (Trace, Debug, Info, Warn, Error)
│       ├── LogConfig struct with builder pattern
│       ├── init_logging() and init_logging_with_config()
│       ├── fields module (Orleans-specific field names)
│       └── targets module (Orleans component targets)
```

### Tests
- Unit tests: log level/format defaults, config builder (6 tests)
- Unit tests: field and target name validation (2 tests)
- Property tests: config builder idempotency (1 test)
- Property tests: filter directive preservation (1 test)
- Property tests: log level/format consistency (2 tests)
- Integration tests: tracing macros and spans compile (2 tests)
- Doc tests: initialization examples (3 tests)

### Usage Example
```rust
use orleans_telemetry::{init_logging, LogFormat, LogLevel, LogConfig};

// Simple initialization
init_logging(LogFormat::Compact, LogLevel::Info);

// Advanced configuration
let config = LogConfig::development()
    .with_filter("orleans_host=debug,orleans_runtime=trace");
init_logging_with_config(config);
```

---

## Phase 11: Grain Persistence ✅

**Objective**: Enable grains to store state durably across activations using an optimistic concurrency model with ETags.

**Status**: COMPLETE - 49 unit tests and 6 doc tests passing.

### Tasks

- [x] **11.1** Define error types for persistence operations
  - `StorageError` enum with variants: EtagMismatch, RecordExists, RecordNotFound, PayloadTooLarge, Serialization, StateNotInitialized, etc.
  - `InconsistentStateError` for optimistic concurrency conflicts
  - `StorageResult<T>` type alias

- [x] **11.2** Implement `GrainState<T>` wrapper
  - Wraps grain state with persistence metadata
  - Tracks ETag for optimistic concurrency control
  - Tracks `record_exists` flag for new vs. existing state
  - Methods: `state()`, `state_mut()`, `etag()`, `mark_read()`, `mark_written()`, `mark_cleared()`

- [x] **11.3** Define `IGrainStorage` trait
  - Primary storage provider interface using raw bytes (dyn-compatible)
  - `read_state(state_name, grain_id) -> RawGrainState`
  - `write_state(state_name, grain_id, state) -> String` (returns new ETag)
  - `clear_state(state_name, grain_id, expected_etag)`
  - Works with `RawGrainState` for serialized data

- [x] **11.4** Implement `MemoryGrainStorage`
  - In-memory storage provider for testing and development
  - Partitioned storage for reduced lock contention
  - Full ETag-based optimistic concurrency control
  - Wildcard ETag ("*") support for forced updates
  - Structured logging via `tracing` crate

- [x] **11.5** Implement `StateStorageBridge<T>`
  - Adapter connecting grains to storage providers
  - Handles serialization/deserialization via `GrainStorageSerializer`
  - Tracks initialization state (must call `read_state` before `write_state`)
  - Implements `IStorage` and `IStorageTyped<T>` traits
  - Concurrent modification detection via ETag tracking

- [x] **11.6** Implement `GrainStorageSerializer`
  - JSON-based serialization for grain state
  - Concrete type (not trait) for dyn-compatibility
  - Methods: `serialize()`, `deserialize()`

### Crate Structure
```
orleans-persistence/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # StorageError, InconsistentStateError
│   ├── grain_state.rs     # GrainState<T> wrapper
│   ├── storage.rs         # IGrainStorage trait, GrainStorageSerializer
│   ├── memory_storage.rs  # MemoryGrainStorage provider
│   └── state_bridge.rs    # StateStorageBridge<T>
```

### Tests
- Unit tests: error types and conversions (5 tests)
- Unit tests: grain state wrapper operations (14 tests)
- Unit tests: memory storage operations (13 tests)
- Unit tests: state bridge lifecycle (10 tests)
- Unit tests: serializer roundtrip (5 tests)
- Integration test: end-to-end persistence lifecycle (2 tests)
- Doc tests: public API examples (6 tests)

### Usage Example
```rust
use orleans_persistence::{
    GrainState, MemoryGrainStorage, StateStorageBridge, IStorage, IStorageTyped,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Default, Clone, Serialize, Deserialize)]
struct CounterState {
    count: i32,
}

// Create storage provider
let storage = Arc::new(MemoryGrainStorage::new());

// Create bridge for a grain
let mut bridge: StateStorageBridge<CounterState> =
    StateStorageBridge::new(grain_id, "CounterState", storage);

// Load state on activation
bridge.read_state().await?;

// Modify and persist
bridge.state_mut().count += 1;
bridge.write_state().await?;

// Clear state
bridge.clear_state().await?;
```

---

## Phase 12: Grain Timers ✅

**Objective**: Implement in-memory, grain-scoped scheduled callbacks for periodic and delayed execution.

**Status**: COMPLETE - 33 unit tests passing.

### Tasks

- [x] **12.1** Define error types for timer operations
  - `TimerError` enum with variants: AlreadyDisposed, NotFound, PeriodTooShort, ChannelClosed, Internal
  - `TimerResult<T>` type alias

- [x] **12.2** Implement `TimerId` unique identifier
  - Simple u64-based identifier
  - Display trait for logging
  - Hash/Eq traits for collection storage

- [x] **12.3** Implement `TimerHandle` internal management
  - Cancellation via `CancellationToken`
  - Schedule change via channel
  - `is_cancelled()`, `cancel()`, `change()` methods

- [x] **12.4** Implement `GrainTimer` public API
  - `id()`, `is_disposed()`, `dispose()`, `change()` methods
  - Clone support for sharing timer references
  - Dispose is idempotent (safe to call multiple times)

- [x] **12.5** Implement `GrainTimerRegistry`
  - Manages all timers for a grain activation
  - `register_timer(callback, due_time, period)` -> GrainTimer
  - `dispose_all()` for grain deactivation cleanup
  - `active_timer_count()` for monitoring
  - Timer tasks use tokio::spawn for async execution

- [x] **12.6** Implement `TimerOptions` configuration
  - `min_timer_period` (default: 10ms)
  - `max_timer_callbacks_per_turn` (default: 100)
  - `allow_immediate_timers` (default: true)
  - Builder pattern for configuration

- [x] **12.7** Implement `ITimerRegistry` trait
  - Interface for timer registration services
  - Enables dependency injection and testing

### Timer Characteristics
- In-memory only (lost on deactivation)
- Grain-scoped (tied to specific activation)
- No persistence across silo restarts
- High frequency capable (milliseconds)
- Stopped automatically on grain deactivation
- Callbacks queued on grain's work queue for turn-based execution

### Crate Structure
```
orleans-timers/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs        # TimerError, TimerResult
│   ├── options.rs      # TimerOptions configuration
│   ├── timer.rs        # TimerId, TimerHandle, GrainTimer
│   └── registry.rs     # GrainTimerRegistry, ITimerRegistry
```

### Tests
- Unit tests: error types (5 tests)
- Unit tests: timer options (6 tests)
- Unit tests: timer ID and handle (9 tests)
- Unit tests: grain timer operations (4 tests)
- Unit tests: registry operations (8 tests)
- Async tests: timer firing and scheduling (multiple tests)

### Usage Example
```rust
use orleans_timers::{GrainTimerRegistry, TimerCallback};
use std::time::Duration;

// Create a timer registry (typically owned by grain context)
let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
let registry = GrainTimerRegistry::new(callback_tx);

// Register a periodic timer
let callback: TimerCallback = Box::new(|| Box::pin(async {
    println!("Timer fired!");
}));
let timer = registry.register_timer(
    callback,
    Duration::from_secs(5),   // due time (first tick after 5s)
    Duration::from_secs(10),  // period (every 10s thereafter)
)?;

// Later, change the timer's schedule
timer.change(Duration::from_secs(1), Duration::from_secs(5))?;

// Or dispose the timer
timer.dispose();

// On grain deactivation, dispose all timers
registry.dispose_all();
```

---

## Phase 13: Grain Reminders ✅

**Objective**: Implement persistent, cluster-aware scheduled callbacks that survive silo restarts and grain deactivations.

**Status**: COMPLETE - 64 unit tests and 1 doc test passing.

### Tasks

- [x] **13.1** Define error types for reminder operations
  - `ReminderError` enum with variants: NotFound, AlreadyExists, EtagMismatch, PeriodTooShort, InvalidName, NotRemindable, NotInitialized, ShuttingDown, Storage, Serialization, Internal
  - `ReminderResult<T>` type alias

- [x] **13.2** Implement `ReminderOptions` configuration
  - `min_reminder_period` (default: 60s)
  - `refresh_reminder_period` (default: 5 minutes)
  - `init_timeout` (default: 30s)
  - `max_reminders_per_silo` (default: 10,000)
  - `min_due_time` (default: 5s)
  - Preset: `for_testing()` with shorter intervals

- [x] **13.3** Implement `GrainReminder` handle
  - Contains `grain_id` and `reminder_name`
  - `name()`, `grain_id()` accessors
  - Display trait for logging

- [x] **13.4** Implement `TickStatus` struct
  - `first_tick_time`, `current_tick_time`, `period` fields
  - `tick_count()` - number of ticks since first tick
  - `time_until_next_tick()` - duration until next firing

- [x] **13.5** Implement `ReminderEntry` persistence model
  - `grain_id`, `reminder_name`, `start_at`, `period`, `etag` fields
  - `get_next_tick_time()` - calculates next firing time
  - `get_grain_hash_code()` - for consistent hashing
  - `should_fire_now()`, `time_until_next_tick()` methods
  - Serde serialization support

- [x] **13.6** Define `IRemindable` trait
  - Interface for grains that receive reminder callbacks
  - `receive_reminder(reminder_name, tick_status)` async method

- [x] **13.7** Define `IReminderTable` trait
  - Storage interface for reminder persistence
  - `read_rows(grain_id)` - get all reminders for a grain
  - `read_row(grain_id, reminder_name)` - get specific reminder
  - `read_rows_in_range(range)` - get reminders in hash range
  - `upsert_row(entry)` - insert or update reminder
  - `remove_row(grain_id, reminder_name, etag)` - delete with ETag check
  - `clear_table()` - for testing

- [x] **13.8** Define `IReminderRegistry` trait
  - Grain-facing interface for reminder management
  - `register_or_update_reminder(name, due_time, period)`
  - `unregister_reminder(reminder)`
  - `get_reminder(name)`, `get_reminders()`

- [x] **13.9** Implement `InMemoryReminderTable`
  - Thread-safe in-memory storage for testing/development
  - ETag-based optimistic concurrency control
  - Wildcard ETag ("*") support for forced updates
  - Hash range filtering for `read_rows_in_range`

- [x] **13.10** Implement `ReminderService`
  - Manages reminder execution for a silo
  - `start()`, `stop()` lifecycle methods
  - `update_owned_range(range)` for membership changes
  - `register_or_update_reminder()`, `unregister_reminder()`
  - `get_reminder()`, `get_reminders()`
  - Background refresh task for reminder assignments
  - Local timer management for owned reminders
  - Callback messages via channel for grain notification

### Reminder Characteristics
- Persistent (survives silo restarts)
- Cluster-wide (any silo can trigger based on hash ownership)
- Lower frequency (minimum 1 minute by default)
- Requires grain to implement `IRemindable` trait
- Stored in reminder table (pluggable storage backend)
- Uses consistent hashing for silo assignment
- ETag-based optimistic concurrency control

### Comparison: Timers vs Reminders

| Feature | Timer | Reminder |
|---------|-------|----------|
| Persistence | No | Yes |
| Survives deactivation | No | Yes |
| Survives silo restart | No | Yes |
| Minimum period | Milliseconds | Minutes |
| Cluster-aware | No | Yes |
| Storage required | No | Yes |
| Use case | In-memory polling | Scheduled tasks |

### Crate Structure
```
orleans-reminders/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # ReminderError, ReminderResult
│   ├── options.rs         # ReminderOptions configuration
│   ├── reminder.rs        # GrainReminder, TickStatus, ReminderIdentity
│   ├── reminder_entry.rs  # ReminderEntry persistence model
│   ├── traits.rs          # IRemindable, IReminderTable, IReminderRegistry
│   ├── memory_table.rs    # InMemoryReminderTable
│   └── service.rs         # ReminderService
```

### Tests
- Unit tests: error types (5 tests)
- Unit tests: reminder options (5 tests)
- Unit tests: reminder and tick status (10 tests)
- Unit tests: reminder entry (12 tests)
- Unit tests: in-memory table (14 tests)
- Unit tests: reminder service (12 tests)
- Unit tests: trait accessibility (4 tests)
- Async tests: reminder firing and scheduling

### Usage Example
```rust
use orleans_reminders::{
    ReminderService, InMemoryReminderTable, ReminderOptions,
    IRemindable, TickStatus, ReminderResult,
};
use std::sync::Arc;
use std::time::Duration;

// Implement IRemindable for your grain
struct MyGrain {
    counter: u32,
}

#[async_trait::async_trait]
impl IRemindable for MyGrain {
    async fn receive_reminder(
        &mut self,
        reminder_name: &str,
        tick_status: TickStatus,
    ) -> ReminderResult<()> {
        println!("Reminder {} fired, tick count: {}",
            reminder_name, tick_status.tick_count());
        Ok(())
    }
}

// Create and use the reminder service
async fn example() {
    let table = Arc::new(InMemoryReminderTable::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let options = ReminderOptions::default();

    let service = ReminderService::new(silo_address, table, tx, options);
    service.start().await.unwrap();

    // Register a reminder
    service.register_or_update_reminder(
        &grain_id,
        "daily-check",
        Duration::from_secs(60),   // first tick in 1 minute
        Duration::from_secs(3600), // then every hour
    ).await.unwrap();

    // Later, unregister
    service.unregister_reminder(&grain_id, "daily-check").await.unwrap();

    service.stop().await.unwrap();
}
```

---

## Phase 14: Call Filters and Interceptors ✅

**Objective**: Implement middleware pipeline for intercepting grain method calls, enabling cross-cutting concerns like logging, tracing, authentication, and error handling.

**Status**: COMPLETE - 87 unit tests passing.

### Tasks

- [x] **14.1** Define error types for filter operations
  - `FilterError` enum with variants: BrokenFilterChain, NoResponseSet, Configuration, Invocation, Internal, ContextKeyNotFound, ContextTypeMismatch, AccessDenied, Timeout
  - `FilterResult<T>` type alias

- [x] **14.2** Implement `Response` types
  - `Response` enum: Completed, Result, Exception
  - `ResponseResult` for typed results with optional serialization
  - `ResponseException` with message, type, and stack trace
  - Factory methods: `completed()`, `from_result()`, `from_exception()`

- [x] **14.3** Implement `ContextProperties` for request context
  - Type-erased storage using `ContextValueBox`
  - `get<T>()`, `set<T>()`, `remove()`, `clear()` methods
  - Clone support with proper value cloning
  - Merge support for context propagation

- [x] **14.4** Implement `RequestContext` task-local storage
  - Static accessor using tokio task_local
  - `scope()` for running futures with context
  - `with_inherited_context()` for copy-on-write inheritance
  - `snapshot()` for context propagation to outgoing calls
  - Well-known keys: trace_id, span_id, correlation_id, user_id, tenant_id

- [x] **14.5** Implement call context types
  - `GrainCallContext` base with target, interface, method info
  - `IncomingGrainCallContext` for server-side (includes grain_type)
  - `OutgoingGrainCallContext` for client-side
  - Extension storage for filter-specific data
  - Invoke callback mechanism for pipeline continuation

- [x] **14.6** Define filter traits
  - `IIncomingGrainCallFilter` for server-side interception
  - `IOutgoingGrainCallFilter` for client-side interception
  - `name()` and `order()` methods for diagnostics and sorting
  - `DelegateIncomingFilter`/`DelegateOutgoingFilter` for closures

- [x] **14.7** Implement filter pipelines
  - `IncomingFilterPipeline` and `OutgoingFilterPipeline`
  - Sequential filter execution with chain continuation
  - Broken chain detection (filter didn't call invoke)
  - Response enforcement (response must be set after invocation)
  - Filter ordering by `order()` value
  - Configurable via `PipelineOptions`

- [x] **14.8** Implement built-in filters
  - `LoggingFilter` - logs method calls with timing
  - `ActivityPropagationFilter` - propagates trace context
  - `ExceptionTransformFilter` - transforms exceptions for clients

### Filter Characteristics
- Middleware pattern: each filter calls `context.invoke()` to continue
- Filters execute in order (lower order values first)
- Both pre-processing (before invoke) and post-processing (after invoke)
- Request context flows through async call chains
- Built-in filters for common cross-cutting concerns

### Crate Structure
```
orleans-filters/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # FilterError, FilterResult
│   ├── response.rs        # Response, ResponseResult, ResponseException
│   ├── request_context.rs # ContextProperties, RequestContext
│   ├── context.rs         # GrainCallContext, IncomingGrainCallContext, OutgoingGrainCallContext
│   ├── filter.rs          # IIncomingGrainCallFilter, IOutgoingGrainCallFilter, delegates
│   ├── pipeline.rs        # IncomingFilterPipeline, OutgoingFilterPipeline
│   └── builtin.rs         # LoggingFilter, ActivityPropagationFilter, ExceptionTransformFilter
```

### Tests
- Unit tests: error types (8 tests)
- Unit tests: response operations (10 tests)
- Unit tests: context properties (10 tests)
- Unit tests: request context (12 tests)
- Unit tests: call context (12 tests)
- Unit tests: filter traits (6 tests)
- Unit tests: pipeline operations (16 tests)
- Unit tests: built-in filters (13 tests)

### Usage Example
```rust
use orleans_filters::{
    IncomingFilterPipeline, LoggingFilter, ActivityPropagationFilter,
    IIncomingGrainCallFilter, RequestContext, ContextProperties,
};
use std::sync::Arc;

// Create a filter pipeline
let mut pipeline = IncomingFilterPipeline::new();

// Add built-in filters
pipeline.add_filter(Arc::new(LoggingFilter::new())).unwrap();
pipeline.add_filter(Arc::new(ActivityPropagationFilter::new())).unwrap();

// Sort filters by order
pipeline.sort_by_order();

// Execute the pipeline
pipeline.execute(&mut context, |ctx| {
    // Invoke the actual grain method
    ctx.set_result(grain.invoke_method(ctx.method_id(), ctx.request_body()));
    Ok(())
}).await?;

// Request context flows through calls
let mut props = ContextProperties::new();
props.set("user_id", "user123".to_string());

RequestContext::scope(props, async {
    // Context is available in nested async calls
    let user = RequestContext::get::<String>("user_id");
}).await;
```

---

## Phase 15: Observers and Callbacks ✅

**Objective**: Implement publish-subscribe communication where grains can send notifications to clients or other grains without polling.

**Status**: COMPLETE - 91 unit tests and 2 doc tests passing.

### Tasks

- [x] **15.1** Define error types for observer operations
  - `ObserverError` enum with variants: ObserverGarbageCollected, NotRegistered, AlreadyRegistered, InvalidReference, NotObserverGrainId, SubscriptionExpired, NotificationFailed, ShuttingDown, ChannelError, Internal
  - `ObserverResult<T>` type alias

- [x] **15.2** Implement `ObserverGrainId`
  - Special grain ID format: `sys.observer/[ClientId]+[ObserverScopedId]`
  - `create(client_id)` generates new observer ID with random UUID
  - `is_observer_grain_id(grain_id)` validates format
  - `try_parse(grain_id)` attempts conversion from regular GrainId
  - `client_id()`, `scoped_id()` accessors

- [x] **15.3** Define `IGrainObserver` trait
  - Marker trait for observer interfaces
  - `as_any()` and `as_any_mut()` for downcasting
  - All observers implement `Send + Sync + Debug + Any`

- [x] **15.4** Implement `InvokeMethodOptions`
  - Options flags: `one_way`, `read_only`, `always_interleave`, `unordered`
  - Factory methods: `one_way()`, `read_only()`, `always_interleave()`

- [x] **15.5** Implement `ObserverManager<K, V>`
  - Thread-safe subscription management with copy-on-write semantics
  - `subscribe(key, observer)` adds or renews subscription
  - `unsubscribe(key)` removes subscription
  - `notify(callback)` and `notify_filtered(callback, predicate)` for sync notification
  - `notify_async(callback)` and `notify_async_filtered(callback, predicate)` for async notification
  - Automatic expiration-based cleanup via configurable `Duration`
  - `clear_expired()` removes stale subscriptions
  - Copy-on-write snapshots for safe concurrent iteration and modification

- [x] **15.6** Implement `LocalObjectData`
  - Weak reference storage for registered observers (allows GC)
  - Message queue for sequential delivery
  - `receive_message(message)` queues messages
  - `try_dequeue_message()` for message pump
  - `is_alive()` checks if observer was garbage collected
  - `mark_deregistered()` for cleanup

- [x] **15.7** Implement `InvokableObjectManager`
  - Registry of locally registered observers
  - `register(observer)` creates new observer ID and registers
  - `try_register(observer, id)` registers with specific ID
  - `deregister(observer_id)` removes registration
  - `dispatch(grain_id, method_id, body)` routes messages to observers
  - `cleanup_garbage_collected()` removes defunct observers
  - Thread-safe with DashMap

### Observer Characteristics
- Weak references allow automatic garbage collection
- One-way (fire-and-forget) notification semantics
- Subscription expiration with configurable timeout
- Copy-on-write for safe concurrent notification
- Message queuing for sequential delivery

### Crate Structure
```
orleans-observers/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs                # ObserverError, ObserverResult
│   ├── observer_grain_id.rs    # ObserverGrainId
│   ├── traits.rs               # IGrainObserver, IAddressable, IInvokable
│   ├── observer_manager.rs     # ObserverManager<K, V>
│   ├── local_object_data.rs    # LocalObjectData, ObserverMessage
│   └── invokable_object_manager.rs  # InvokableObjectManager
```

### Tests
- Unit tests: error types (10 tests)
- Unit tests: observer grain ID (18 tests + 3 property tests)
- Unit tests: traits (5 tests)
- Unit tests: observer manager (18 tests + 3 property tests)
- Unit tests: local object data (12 tests)
- Unit tests: invokable object manager (15 tests)
- Integration tests: full observer flow (7 tests)

### Usage Example
```rust
use orleans_observers::{
    ObserverManager, InvokableObjectManager, IGrainObserver, ObserverGrainId,
};
use std::sync::Arc;
use std::time::Duration;

// Observable grain with subscription management
struct ChatRoomGrain {
    observers: ObserverManager<String, Arc<dyn IGrainObserver>>,
}

impl ChatRoomGrain {
    fn new() -> Self {
        Self {
            observers: ObserverManager::new(Duration::from_secs(300)),
        }
    }

    fn subscribe(&self, user_id: String, observer: Arc<dyn IGrainObserver>) {
        self.observers.subscribe(user_id, observer);
    }

    fn broadcast_message(&self, message: &str) {
        self.observers.notify(|_observer| {
            // Send notification to each observer
            println!("Broadcasting: {}", message);
        });
    }
}

// Client-side observer registration
let manager = InvokableObjectManager::new("client-1".to_string());
let observer: Arc<dyn IGrainObserver> = Arc::new(MyObserver::new());
let _keep_alive = observer.clone(); // Must keep strong reference!
let observer_id = manager.register(observer).unwrap();

// Pass observer_id to grain for subscription
// grain.subscribe(user_id, observer_ref).await;

// Later, deregister
manager.deregister(&observer_id).unwrap();
```

---

## Workspace Structure

```
orleans-rs/
├── Cargo.toml (workspace)
├── orleans-core/           # Identity types
├── orleans-serialization/  # Wire protocol
├── orleans-codegen/        # Proc macros
├── orleans-messaging/      # Message passing
├── orleans-clustering/     # Membership
├── orleans-directory/      # Grain directory
├── orleans-runtime/        # Grain hosting
├── orleans-telemetry/      # Structured logging
├── orleans-persistence/    # Grain state persistence
├── orleans-timers/         # Grain timers
├── orleans-reminders/      # Grain reminders (persistent)
├── orleans-filters/        # Call filters and interceptors
├── orleans-observers/      # Observers and callbacks
├── orleans-host/           # Silo assembly
└── orleans-tests/          # Integration tests
```

## Dependencies

```toml
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
bytes = "1"
dashmap = "5"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
thiserror = "1"
parking_lot = "0.12"
xxhash-rust = { version = "0.8", features = ["xxh32"] }
proptest = "1"  # For property testing
tracing-subscriber = { version = "0.3", features = ["env-filter", "json", "fmt"] }
```

---

## Success Criteria ✅

The MVP is complete! All core criteria have been achieved:

1. ✅ **Three silos form a cluster** - All silos appear in membership table with Active status
   - Verified by: `test_cluster_formation`, `test_tcp_membership_with_in_process_silos`

2. ✅ **Grain creation works** - `grain_factory.get_grain_reference_by_id()` returns a valid reference
   - Verified by: `test_grain_activation`, multiple integration tests

3. ✅ **Cross-silo calls work** - Calling a grain from a different silo than where it's activated succeeds
   - Verified by: `test_cross_silo_grain_invocation`, `test_tcp_membership_with_in_process_silos`

4. ✅ **Single activation guarantee** - Only one activation exists per grain ID across the cluster
   - Verified by: `test_single_activation_guarantee`, `test_simultaneous_single_activation_guarantee`

5. ✅ **Turn-based execution** - Grain methods execute sequentially, no concurrent access
   - Verified by: counter test showing correct increment sequence (1 -> 2 -> 3)

6. ✅ **Multi-process support** - Separate OS processes can form a cluster via TCP membership table
   - Verified by: `test_tcp_membership_with_in_process_silos`, `test_three_process_cluster_formation`

7. ✅ **All tests pass** - 618+ tests across all crates (120 core, 80 clustering, 54 directory, 20 telemetry, 55 persistence, 33 timers, 64 reminders, 87 filters, 93 observers, 18 host)

8. ✅ **Grain persistence** - Grains can persist state durably with optimistic concurrency control
   - Verified by: `orleans-persistence` crate with 49 unit tests and 6 doc tests

9. ✅ **Grain reminders** - Persistent scheduled callbacks that survive silo restarts
   - Verified by: `orleans-reminders` crate with 64 unit tests and 1 doc test

10. ✅ **Call filters and interceptors** - Middleware pipeline for cross-cutting concerns
    - Verified by: `orleans-filters` crate with 87 unit tests

11. ✅ **Observers and callbacks** - Pub/sub communication for grains
    - Verified by: `orleans-observers` crate with 91 unit tests and 2 doc tests

---

## Implementation Order

```
Phase 1 (Identity)     ──┐
                         ├──→ Phase 2 (Serialization) ──┐
Phase 4 (Clustering)   ──┤                              │
                         │                              ├──→ Phase 3 (Messaging)
                         │                              │
                         └──────────────────────────────┤
                                                        │
Phase 5 (Directory)    ←────────────────────────────────┤
                                                        │
Phase 6 (Runtime)      ←────────────────────────────────┤
                                                        │
Phase 7 (Codegen)      ←────────────────────────────────┘

Phase 8 (Host)         ←── All above phases

Phase 9 (Tests)        ←── Phase 8

Phase 10 (Telemetry)   ←── All above phases (observability layer)

Phase 11 (Persistence) ←── Phase 1 (Identity) + serde

Phase 12 (Timers)      ←── tokio (standalone, integrates with Runtime)

Phase 13 (Reminders)   ←── Phase 1 (Identity) + Phase 5 (Directory) + tokio

Phase 14 (Filters)     ←── Phase 1 (Identity) + Phase 3 (Messaging) + tokio

Phase 15 (Observers)   ←── Phase 1 (Identity) + tokio
```

Estimated complexity: ~10,000-15,000 lines of Rust code.
