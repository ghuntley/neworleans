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
- ~~Streaming~~ → **Now implemented in Phase 16**
- ~~Transactions~~ → **Now implemented in Phase 17**
- ~~Complex placement strategies (MVP uses hash-based only)~~ → **Now implemented in Phase 21**
- ~~Version tolerance in serialization~~ → **Now implemented in Phase 22**
- ~~TLS/Security~~ → **Now implemented in Phase 23**
- ~~Graceful grain migration~~ → **Now implemented in Phase 24**

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

- [x] **2.8** Implement `#[derive(OrleansSerialize, OrleansDeserialize)]` proc macros
  - Generates `FieldSerialize` and `FieldDeserialize` implementations for structs
  - Field IDs via `#[id(n)]` attribute (auto-assigned if not specified)
  - Supports nested structs, all primitive types, forward compatibility (unknown fields skipped)
  - 14 unit tests including 4 property-based tests

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

- [x] **8.5** Implement `ClusterClient` → **Implemented in Phase 18**
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

### Property-Based Tests ✅

- [x] **9.10** Grain identity properties
  - `grain_id(grain_ref) == expected_grain_id`
  - `hash(grain_id1) != hash(grain_id2)` for different grains (usually)
  - **Implemented in**: `property_tests.rs` (orleans-host/tests/)
  - **Tests**: 8 property tests covering equality, hash consistency, parse/display roundtrip, integer key roundtrip, compound key split, hash distribution, hash stability

- [x] **9.11** Directory consistency
  - After any sequence of register/unregister operations
  - `lookup(grain_id)` returns registered address or None
  - **Implemented in**: `property_tests.rs` (orleans-host/tests/)
  - **Tests**: 7 property tests covering register/lookup, unregister/lookup, duplicate registration, conflict detection, grain count consistency, dead silo entry removal, concurrent operations

- [x] **9.12** Message delivery
  - All sent messages are received (unless silo dies)
  - No duplicate deliveries
  - **Implemented in**: `property_tests.rs` (orleans-host/tests/)
  - **Tests**: 4 async tests covering local delivery, sequential delivery, data integrity, and no duplicate delivery
  - **Additional tests**: Hash distribution uniformity, hash collision rate verification

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

## Phase 16: Streaming ✅

**Objective**: Implement reactive pub/sub messaging for processing sequences of events with support for multiple stream providers.

**Status**: COMPLETE - 51 unit tests and 1 doc test passing.

### Tasks

- [x] **16.1** Define error types for streaming operations
  - `StreamError` enum with variants: SubscriptionNotFound, ProviderNotFound, StreamCompleted, StreamErrored, QueueAdapterError, Serialization, DeliveryFailed, FilterError, Timeout, ChannelClosed, ShuttingDown, CacheMiss, Internal
  - `StreamResult<T>` type alias
  - `DeliveryStatus` enum for tracking delivery outcomes

- [x] **16.2** Implement stream identity types
  - `StreamKey` enum: Guid, String, Integer
  - `StreamId` struct with namespace + key
  - `StreamSequenceToken` for checkpointing (sequence_number + event_index)
  - `QualifiedStreamId` with provider name

- [x] **16.3** Define core streaming traits
  - `IAsyncObserver<T>` for receiving individual events
  - `IAsyncBatchObserver<T>` for batch event handling
  - `IAsyncStream<T>` for producing and consuming events
  - `StreamFilter` for selective event delivery
  - `AnyStreamItem` trait for type-erased streaming

- [x] **16.4** Implement subscription management
  - `StreamSubscriptionHandle` for managing subscriptions
  - `SubscriptionState` for internal tracking
  - `SubscriptionMarker` for implicit vs explicit subscriptions
  - `PubSubSubscriptionState` for consumer tracking

- [x] **16.5** Define stream provider interfaces
  - `IStreamProvider` trait with lifecycle methods
  - `StreamHandle` for type-erased stream access
  - `StreamProviderDirection` enum (ReadOnly, WriteOnly, ReadWrite)
  - `StreamProviderRegistry` for managing multiple providers

- [x] **16.6** Implement memory stream provider
  - `MemoryStreamProvider` for testing and development
  - In-memory message queuing with sequence tokens
  - Immediate delivery to subscribers
  - Stream completion and error signaling

- [x] **16.7** Implement configuration options
  - `StreamPullingAgentOptions` for queue polling
  - `StreamLifecycleOptions` for timeouts
  - `StreamPubSubOptions` for subscription types
  - `StreamCacheEvictionOptions` for cache management

### Crate Structure
```
orleans-streaming/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # StreamError, StreamResult, DeliveryStatus
│   ├── stream_id.rs       # StreamId, StreamKey, StreamSequenceToken
│   ├── traits.rs          # IAsyncObserver, IAsyncStream, StreamFilter
│   ├── subscription.rs    # StreamSubscriptionHandle, SubscriptionState
│   ├── provider.rs        # IStreamProvider, StreamProviderRegistry
│   ├── memory_provider.rs # MemoryStreamProvider
│   └── options.rs         # Configuration options
```

### Tests
- Unit tests: error types (3 tests)
- Unit tests: stream identity (17 tests)
- Unit tests: traits (3 tests)
- Unit tests: subscription (6 tests)
- Unit tests: provider (3 tests)
- Unit tests: memory provider (9 tests)
- Unit tests: options (6 tests)
- Integration tests: end-to-end streaming (2 tests)
- Doc tests: public API examples (1 test)

### Usage Example
```rust
use orleans_streaming::{
    MemoryStreamProvider, StreamId, IAsyncObserver, StreamSequenceToken,
    StreamError, StreamResult, IStreamProvider, StreamHandle,
};
use std::sync::Arc;
use async_trait::async_trait;

#[derive(Debug)]
struct MyObserver;

#[async_trait]
impl IAsyncObserver<serde_json::Value> for MyObserver {
    async fn on_next(
        &self,
        item: serde_json::Value,
        token: Option<StreamSequenceToken>,
    ) -> StreamResult<()> {
        println!("Received: {:?}", item);
        Ok(())
    }

    async fn on_completed(&self) -> StreamResult<()> {
        println!("Stream completed");
        Ok(())
    }

    async fn on_error(&self, error: StreamError) -> StreamResult<()> {
        println!("Stream error: {}", error);
        Ok(())
    }
}

async fn example() -> StreamResult<()> {
    // Create provider
    let provider = Arc::new(MemoryStreamProvider::new("MyProvider"));
    provider.start().await?;

    // Get stream and subscribe
    let stream = provider.get_stream(StreamId::create("orders", "customer-123"));
    let handle = stream.subscribe_any(Arc::new(MyObserver)).await?;

    // Publish events
    stream.on_next_any(serde_json::json!({"order_id": 1})).await?;

    // Cleanup
    handle.unsubscribe().await?;
    provider.stop().await?;
    Ok(())
}
```

---

## Phase 17: Transactions ✅

**Objective**: Implement ACID transactions using an asymmetric Two-Phase Commit (2PC) protocol for distributed state consistency.

**Status**: COMPLETE - 100 unit tests passing.

### Tasks

- [x] **17.1** Define error types for transaction operations
  - `TransactionalStatus` enum with status codes (Ok, PrepareTimeout, CascadingAbort, BrokenLock, etc.)
  - `TransactionError` enum for error handling
  - `AbortedReason` enum for abort causes

- [x] **17.2** Implement `TransactionalStateOptions` and `TransactionAgentOptions`
  - Configurable timeouts: lock_timeout, prepare_timeout, lock_acquire_timeout
  - Max lock group size, cleanup intervals
  - Testing presets with shorter timeouts

- [x] **17.3** Implement `CausalClock`
  - Monotonically increasing timestamps for causal ordering
  - `utc_now()` returns unique timestamp > previous
  - `merge_utc_now(external)` for cross-node timestamp synchronization
  - Lock-free implementation using AtomicI64

- [x] **17.4** Implement transaction identity types
  - `TransactionId` with UUID backing
  - `ParticipantId` with grain ID and role capabilities
  - `AccessCounter` for read/write tracking
  - `TransactionInfo` for full transaction metadata
  - `Role` enum: Resource, Manager, PriorityManager

- [x] **17.5** Implement `ReaderWriterLock` with lock groups
  - `LockGroup<TState>` for concurrent non-conflicting transactions
  - Conflict detection: Read-Read (no conflict), Read-Write/Write-Write (conflict)
  - Priority-based conflict resolution (earlier timestamp wins)
  - Copy-on-write semantics for isolation
  - `TransactionRecord` for per-transaction state

- [x] **17.6** Define storage interfaces
  - `ITransactionalStateStorage` trait for persistence
  - `PendingTransactionState` for uncommitted changes
  - `TransactionalStateMetaData` with commit records
  - `StorageBatch` for batch operations
  - `InMemoryTransactionalStorage` for testing

- [x] **17.7** Implement `TransactionalState<TState>`
  - `ITransactionalState` trait with `perform_read()` and `perform_update()`
  - Integration with `ReaderWriterLock` for concurrency control
  - `prepare()`, `commit()`, `abort()`, `confirm()` lifecycle methods
  - Storage persistence for durability

- [x] **17.8** Implement `TransactionAgent`
  - Orchestrates 2PC protocol from client side
  - `start_transaction(read_only, timeout)` creates new transaction
  - `resolve(tx_id)` triggers commit protocol
  - Read-only transactions use 1-phase commit
  - Read-write transactions use 2-phase commit
  - `TransactionOverloadDetector` for backpressure

### Crate Structure
```
orleans-transactions/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs            # TransactionalStatus, TransactionError, AbortedReason
│   ├── options.rs          # TransactionalStateOptions, TransactionAgentOptions
│   ├── clock.rs            # CausalClock
│   ├── transaction_info.rs # TransactionId, ParticipantId, AccessCounter, TransactionInfo
│   ├── lock.rs             # ReaderWriterLock, LockGroup, TransactionRecord
│   ├── storage.rs          # ITransactionalStateStorage, InMemoryTransactionalStorage
│   ├── transactional_state.rs # TransactionalState, ITransactionalState
│   └── agent.rs            # TransactionAgent, TransactionOverloadDetector
```

### Tests
- Unit tests: error types (8 tests)
- Unit tests: options (6 tests)
- Unit tests: causal clock (11 tests)
- Unit tests: transaction info (16 tests)
- Unit tests: lock groups (13 tests)
- Unit tests: storage (10 tests)
- Unit tests: transactional state (10 tests)
- Unit tests: transaction agent (13 tests)
- Integration tests: full transaction flow (8 tests)
- Property tests: clock monotonicity, timestamp uniqueness

### Usage Example
```rust
use orleans_transactions::{
    TransactionAgent, TransactionalState, ITransactionalState,
    TransactionAgentOptions, TransactionalStateOptions,
    InMemoryTransactionalStorage,
};
use std::sync::Arc;

#[derive(Clone, Default, Serialize, Deserialize)]
struct BankAccount {
    balance: i64,
}

// Create transaction infrastructure
let agent = TransactionAgent::new(TransactionAgentOptions::default());
let storage = Arc::new(InMemoryTransactionalStorage::<BankAccount>::new());
let state = TransactionalState::new(grain_id, "balance", storage, options);

// Activate grain
state.on_activate().await?;

// Start transaction
let info = agent.start_transaction(false, None)?;

// Perform operations
state.perform_update(info.transaction_id, |s| {
    s.balance += 100;
}).await?;

// Commit
state.commit(info.transaction_id).await?;

// Or abort
state.abort(info.transaction_id, None).await?;
```

---

## Phase 18: ClusterClient ✅

**Objective**: Implement external client for connecting to Orleans clusters without being a silo.

**Status**: COMPLETE - 62 unit tests and 3 doc tests passing.

### Tasks

- [x] **18.1** Define error types for client operations
  - `ClientError` enum with variants: NotConnected, AlreadyConnected, Connecting, GatewayConnectionFailed, NoGatewaysAvailable, GatewayDisconnected, RequestTimeout, RequestRejected, Serialization, Deserialization, Network, Configuration, Internal, ShuttingDown, CallbackNotFound, Lifecycle, Messaging, Membership
  - `ClientResult<T>` type alias
  - `ClientStatus` enum: Created, Connecting, Connected, Disconnecting, Disconnected

- [x] **18.2** Implement `ClientOptions` configuration
  - `cluster_id`, `service_id` for cluster identification
  - `gateway_endpoints` for static gateway addresses
  - `response_timeout` (default: 30s)
  - `gateway_refresh_interval` (default: 60s)
  - `reconnect_delay` (default: 1s)
  - `max_pending_requests` (default: 10,000)
  - `max_retry_attempts` (default: 3)
  - `auto_reconnect` (default: true)
  - Builder pattern with validation

- [x] **18.3** Implement `GatewayOptions` and `GatewayStatus`
  - Gateway status: Healthy, Degraded, Unhealthy, Recovering
  - `GatewayInfo` for tracking gateway health and statistics
  - Configurable failure threshold and recovery period
  - Health check timeout settings

- [x] **18.4** Implement `CallbackDataManager`
  - Request/response correlation via `CorrelationId`
  - `CallbackData` with request, sender channel, timeout tracking
  - `add(request, timeout)` returns oneshot receiver
  - `try_complete(response)` matches and completes callbacks
  - `fail(correlation_id, error)` for error handling
  - `expire_timed_out()` for automatic cleanup
  - `fail_all(error)` for shutdown
  - Background expiration task with cancellation support

- [x] **18.5** Implement `GatewayManager`
  - Manages connections to gateway silos
  - Round-robin and preferred gateway selection
  - Gateway health tracking with failure/success recording
  - Automatic gateway discovery from membership table
  - Gateway recovery for unhealthy connections
  - Background refresh and recovery tasks

- [x] **18.6** Implement `ClusterClient`
  - Main client struct implementing grain factory
  - `connect()` and `disconnect()` lifecycle methods
  - `get_grain_reference()` and `get_grain<T>()` for grain access
  - Message receiver task for response handling
  - Integration with `GrainFactory` for type-safe grain access
  - Virtual client address for message routing
  - Structured logging via `tracing` crate

- [x] **18.7** Implement `ClientBuilder`
  - Fluent builder pattern for client configuration
  - `with_cluster_id()`, `with_service_id()`, `with_gateway()`
  - `with_response_timeout()`, `with_max_retry_attempts()`
  - `with_membership_table()` for auto-discovery
  - `build()` and `build_and_connect()` for client creation

### Crate Structure
```
orleans-client/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs       # ClientError, ClientResult, ClientStatus
│   ├── options.rs     # ClientOptions, GatewayOptions
│   ├── callback.rs    # CallbackData, CallbackDataManager
│   ├── gateway.rs     # GatewayManager, GatewayInfo, GatewayStatus
│   ├── client.rs      # ClusterClient
│   └── builder.rs     # ClientBuilder
```

### Tests
- Unit tests: error types (5 tests)
- Unit tests: client options (12 tests)
- Unit tests: gateway options (2 tests)
- Unit tests: callback data manager (9 tests)
- Unit tests: gateway manager (10 tests)
- Unit tests: cluster client (10 tests)
- Unit tests: client builder (14 tests)
- Async tests: callback response handling (3 tests)
- Doc tests: public API examples (3 tests)

### Usage Example
```rust
use orleans_client::{ClientBuilder, ClusterClient};
use std::time::Duration;

// Build and connect the client
let client = ClientBuilder::new()
    .with_cluster_id("my-cluster")
    .with_service_id("my-app")
    .with_gateway("10.0.0.1:30000".parse()?)
    .with_gateway("10.0.0.2:30000".parse()?)
    .with_response_timeout(Duration::from_secs(30))
    .build()?;

client.connect().await?;

// Get grain references and invoke methods
// let grain = client.get_grain::<IMyGrain>("my-key").await?;
// let result = grain.my_method("argument").await?;

// Disconnect when done
client.disconnect().await?;
```

---

## Phase 19: Interface Versioning ✅

**Objective**: Implement interface versioning system for heterogeneous cluster deployments with rolling upgrades.

**Status**: COMPLETE - 97 unit tests and 9 doc tests passing.

### Tasks

- [x] **19.1** Define error types for versioning operations
  - `VersionError` enum with variants: NoCompatibleVersion, InterfaceNotFound, GrainTypeNotFound, NoSilosForVersion, StaleManifest, InvalidVersion, UnknownStrategy, Configuration, Internal
  - `VersionResult<T>` type alias

- [x] **19.2** Implement `GrainVersioningOptions` configuration
  - `default_compatibility_strategy` (default: "BackwardCompatible")
  - `default_version_selector_strategy` (default: "AllCompatibleVersions")
  - `enabled` flag for version-aware placement
  - Preset configurations: `strict()`, `permissive()`, `conservative()`, `aggressive()`

- [x] **19.3** Implement compatibility strategies
  - `CompatibilityDirector` trait with `is_compatible(requested, current)` method
  - `BackwardCompatible` - newer versions handle older requests (default)
  - `StrictVersionCompatible` - only exact version matches
  - `AllVersionsCompatible` - all versions work together
  - Factory function `create_compatibility_director(name)`

- [x] **19.4** Implement version selectors
  - `VersionSelector` trait with `get_suitable_versions()` method
  - `MinimumVersionSelector` - select lowest compatible version
  - `LatestVersionSelector` - select highest compatible version
  - `AllCompatibleVersionsSelector` - return all compatible versions (default)
  - Factory function `create_version_selector(name)`

- [x] **19.5** Implement `GrainVersionManifest`
  - Thread-safe tracking of versions across the cluster
  - `register_version(interface, version, silo)` and `unregister_version()`
  - `get_available_versions(interface)` and `get_supported_silos(interface, version)`
  - `unregister_silo(silo)` for membership changes
  - Atomic manifest version for cache invalidation

- [x] **19.6** Implement manager components
  - `CompatibilityDirectorManager` - per-interface compatibility strategy configuration
  - `VersionSelectorManager` - per-interface version selector configuration
  - `CachedVersionSelectorManager` - caching layer with automatic invalidation

- [x] **19.7** Implement `PlacementTarget`
  - `grain_id`, `interface_type`, `interface_version` fields
  - `is_version_aware()` method (version > 0)
  - Request context data support for placement decisions

### Crate Structure
```
orleans-versioning/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs            # VersionError, VersionResult
│   ├── options.rs          # GrainVersioningOptions
│   ├── compatibility.rs    # CompatibilityDirector, strategies
│   ├── selector.rs         # VersionSelector, strategies
│   ├── manifest.rs         # GrainVersionManifest
│   ├── manager.rs          # Manager components with caching
│   └── placement_target.rs # PlacementTarget
```

### Tests
- Unit tests: error types (9 tests)
- Unit tests: options configuration (7 tests)
- Unit tests: compatibility strategies (12 tests)
- Unit tests: version selectors (14 tests)
- Unit tests: grain version manifest (15 tests)
- Unit tests: manager components (18 tests)
- Unit tests: placement target (14 tests)
- Property tests: compatibility transitivity, selector correctness (8 tests)
- Doc tests: public API examples (9 tests)

### Usage Example
```rust
use orleans_versioning::{
    GrainVersionManifest, CachedVersionSelectorManager,
    CompatibilityDirectorManager, VersionSelectorManager,
    GrainVersioningOptions, PlacementTarget,
    BackwardCompatible, LatestVersionSelector,
};
use std::sync::Arc;

// Configure versioning
let options = GrainVersioningOptions::aggressive();

// Set up managers
let manifest = Arc::new(GrainVersionManifest::new());
let compat_mgr = Arc::new(CompatibilityDirectorManager::new());
let selector_mgr = Arc::new(VersionSelectorManager::new());

// Configure latest version selection for an interface
selector_mgr.set_strategy(interface_type.clone(), "LatestVersion")?;

// Register silo versions
manifest.register_version(&interface_type, 1, silo_a.clone());
manifest.register_version(&interface_type, 2, silo_b.clone());

// Create cached selector for placement
let cached = CachedVersionSelectorManager::new(manifest, compat_mgr, selector_mgr);

// Get suitable silos for version-aware placement
let result = cached.get_suitable_silos(&grain_type, &interface_type, 1);
println!("Suitable silos: {:?}", result.suitable_silos);
```

---

## Phase 20: Stateless Workers ✅

**Objective**: Implement stateless workers - grains designed for high-throughput, parallelizable operations without state preservation between calls.

**Status**: COMPLETE - 81 unit tests passing.

### Tasks

- [x] **20.1** Define error types for stateless worker operations
  - `StatelessWorkerError` enum with variants: PoolAtCapacity, NoWorkersAvailable, WorkerCreationFailed, WorkerNotFound, InvalidConfiguration, ShuttingDown, MessageRoutingFailed, PidControllerError, ActivationFailed, DeactivationFailed, Internal
  - `StatelessWorkerResult<T>` type alias

- [x] **20.2** Implement `StatelessWorkerOptions` configuration
  - `remove_idle_workers` (default: true)
  - `idle_workers_inspection_period` (default: 500ms)
  - `min_idle_cycles_before_removal` (default: 1)
  - `default_max_local_workers` (default: CPU count)
  - `min_workers` (default: 1)
  - `activation_timeout` and `deactivation_timeout`
  - Preset: `for_testing()` with shorter intervals

- [x] **20.3** Implement `StatelessWorkerPlacement`
  - `max_local` - maximum workers per silo
  - `remove_idle_workers` - adaptive pool management
  - `is_using_grain_directory()` returns `false` (no directory lookup)

- [x] **20.4** Implement `PidController` for adaptive pool sizing
  - Tuned PID constants (Kp=0.433, Ki=0.468, Kd=0.480) via genetic algorithm
  - `compute(average_waiting_count)` returns control signal
  - `should_remove_worker(control_signal, min_idle_cycles)` for removal decision
  - `apply_anti_windup(remaining, previous)` prevents oscillation
  - Integral term clamping for stability

- [x] **20.5** Implement `WorkerState` for individual worker tracking
  - `activation_id`, `waiting_count`, `is_executing`, `last_activity`
  - `enqueue_message()`, `start_processing()`, `finish_processing()`
  - `is_inactive()` check (not executing AND waiting_count == 0)
  - `mark_deactivating()` for graceful shutdown

- [x] **20.6** Implement `WorkerPoolStats`
  - `total_workers`, `active_workers`, `inactive_workers`
  - `total_waiting`, `average_waiting`, `max_waiting`, `min_waiting`
  - `from_workers(workers)` factory for statistics computation

- [x] **20.7** Implement `StatelessWorkerDirector` for silo-level placement
  - `select_silo(local_silo, local_silo_terminating, compatible_silos)`
  - Prefers local silo for cache locality
  - Falls back to random selection from compatible silos
  - `uses_grain_directory()` returns `false`

- [x] **20.8** Implement `StatelessWorkerContext` coordinator
  - Manages pool of up to `max_local` workers per grain identity per silo
  - `route_message()` with priority: 1) reuse inactive, 2) create if capacity, 3) least loaded
  - `worker_start_processing()` and `worker_finish_processing()` lifecycle methods
  - `collect_idle_workers()` using PID controller for adaptive removal
  - `shutdown()` for graceful context cleanup
  - Background inspection timer for idle worker collection
  - Structured logging via `tracing` crate

### Key Differences from Regular Grains

| Aspect | Regular Grains | Stateless Workers |
|--------|----------------|-------------------|
| State preservation | Expected between requests | No expectation |
| Grain directory registration | Yes | No |
| Multiple activations | Not allowed (one per grain ID) | Multiple (up to MaxLocal per silo) |
| Location transparency | Via directory lookup | Direct placement decision |
| Message ordering | FIFO within grain | Unordered across workers |

### Crate Structure
```
orleans-stateless-workers/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # StatelessWorkerError, StatelessWorkerResult
│   ├── options.rs         # StatelessWorkerOptions, StatelessWorkerPlacement
│   ├── pid_controller.rs  # PidController with tuned constants
│   ├── worker_state.rs    # WorkerState, WorkerPoolStats
│   ├── director.rs        # StatelessWorkerDirector, PlacementContext
│   └── context.rs         # StatelessWorkerContext, WorkItem
```

### Tests
- Unit tests: error types (11 tests)
- Unit tests: options configuration (11 tests)
- Unit tests: PID controller (16 tests + 3 property tests)
- Unit tests: worker state (12 tests)
- Unit tests: placement director (10 tests + 1 distribution test)
- Unit tests: context operations (13 tests + 2 property tests)
- Integration tests: full stateless worker flow (2 tests)

### Usage Example
```rust
use orleans_stateless_workers::{
    StatelessWorkerContext, StatelessWorkerPlacement, StatelessWorkerOptions,
    StatelessWorkerDirector,
};

// Configure stateless worker placement
let placement = StatelessWorkerPlacement::with_max_local(8);
let options = StatelessWorkerOptions::default();

// Create context (typically managed by runtime)
let ctx = StatelessWorkerContext::new(
    grain_id,
    silo_address,
    placement,
    options,
);

// Start context (begins idle worker collection)
ctx.start()?;

// Route messages to workers
let worker_id = ctx.route_message()?;

// Track processing lifecycle
ctx.worker_start_processing(&worker_id);
// ... process message ...
ctx.worker_finish_processing(&worker_id);

// Graceful shutdown
ctx.shutdown().await?;
```

---

## Phase 21: Placement Strategies ✅

**Objective**: Implement advanced placement strategies for production-grade load balancing and resource optimization.

**Status**: COMPLETE - 118 unit tests and 1 doc test passing.

### Tasks

- [x] **21.1** Define error types for placement operations
  - `PlacementError` enum with variants: NoCompatibleSilos, NoSilosWithRole, AllSilosOverloaded, SiloUnavailable, LocalSiloTerminating, StatisticsUnavailable, InvalidConfiguration, StrategyNotFound, DirectorNotFound, Internal
  - `PlacementResult<T>` type alias
  - `is_retryable()` method to identify transient errors

- [x] **21.2** Implement `PlacementStrategy` trait
  - Common placement strategy interface
  - `strategy_type()` returns strategy identifier
  - `name()` for logging
  - Implementations: RandomPlacement, HashBasedPlacement, PreferLocalPlacement, ActivationCountBasedPlacement, ResourceOptimizedPlacement, SiloRoleBasedPlacement

- [x] **21.3** Implement `PlacementContext` trait
  - `get_compatible_silos(target)` returns silos that can host the grain
  - `get_local_silo()` returns the local silo address
  - `get_silo_status(silo)` returns silo status (Active, ShuttingDown, etc.)
  - `get_silo_statistics(silo)` returns runtime statistics
  - `SimplePlacementContext` implementation for testing

- [x] **21.4** Implement `SiloRuntimeStatistics`
  - CPU usage (0.0-1.0), memory available/max
  - Activation counts (active, recently used)
  - `is_overloaded()` check based on configurable thresholds
  - `normalized_available_memory()` for resource comparisons
  - `SiloStatisticsCache` with thread-safe DashMap storage

- [x] **21.5** Implement `PlacementDirector` trait and registry
  - `on_add_activation(strategy, target, context) -> SiloAddress`
  - `PlacementDirectorRegistry` for strategy-to-director mapping
  - Utility functions: `select_random()`, `select_by_hash()`, `filter_overloaded()`
  - `PlacementTarget` with grain identity, type, version, and request context

- [x] **21.6** Implement `RandomPlacementDirector`
  - Uniform random selection from compatible silos
  - Honors placement hints when silo is compatible

- [x] **21.7** Implement `HashBasedPlacementDirector`
  - Deterministic placement using grain ID hash
  - Same grain always maps to same silo (while cluster topology is stable)
  - Enables cache affinity for frequently accessed grains

- [x] **21.8** Implement `PreferLocalPlacementDirector`
  - Prefers local silo when compatible and not terminating
  - Falls back to random selection from compatible silos
  - Reduces network hops for grain activations

- [x] **21.9** Implement `ActivationCountPlacementDirector`
  - "Power of k choices" algorithm (default k=2)
  - Randomly samples k silos and picks one with lowest activation count
  - O(k) complexity vs O(n) for full scan
  - Configurable via `ActivationCountBasedPlacementOptions`

- [x] **21.10** Implement `ResourceOptimizedPlacementDirector`
  - Multi-dimensional resource scoring
  - Configurable weights: CPU (default 40%), memory (default 40%), activation count (default 20%)
  - Local silo preference margin for latency optimization
  - Filters overloaded silos before scoring
  - Presets: `balanced()`, `cpu_focused()`, `memory_focused()`

- [x] **21.11** Implement placement options
  - `ActivationCountBasedPlacementOptions` with `choose_out_of` (k value)
  - `ResourceOptimizedPlacementOptions` with weight configuration
  - `PlacementOptions` enum combining all strategy options
  - Builder pattern for ergonomic configuration
  - Serialization support with serde

### Crate Structure
```
orleans-placement/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API, create_default_registry()
│   ├── error.rs            # PlacementError, PlacementResult
│   ├── options.rs          # Strategy configuration options
│   ├── statistics.rs       # SiloRuntimeStatistics, SiloStatisticsCache
│   ├── strategy.rs         # PlacementStrategy trait, strategy types
│   ├── context.rs          # PlacementContext, PlacementTarget
│   ├── director.rs         # PlacementDirector trait, registry, utilities
│   └── directors/
│       ├── mod.rs
│       ├── random.rs
│       ├── hash_based.rs
│       ├── prefer_local.rs
│       ├── activation_count.rs
│       └── resource_optimized.rs
```

### Tests
- Unit tests: error types (10 tests)
- Unit tests: placement options (12 tests)
- Unit tests: silo statistics and cache (15 tests)
- Unit tests: placement strategies (12 tests)
- Unit tests: placement director utilities (8 tests)
- Unit tests: random placement director (6 tests)
- Unit tests: hash-based placement director (6 tests)
- Unit tests: prefer-local placement director (8 tests)
- Unit tests: activation-count placement director (10 tests)
- Unit tests: resource-optimized placement director (8 tests)
- Integration tests (6 tests)
- Property-based tests: placement invariants (4 tests)
- Doc tests: usage examples (1 test)

### Usage Example
```rust
use orleans_placement::{
    create_default_registry, PlacementDirector, SimplePlacementContext,
    PlacementTarget, SiloRuntimeStatistics, ActivationCountBasedPlacement,
};
use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};

// Create silos
let silo1 = SiloAddress::new(addr1, 1);
let silo2 = SiloAddress::new(addr2, 1);

// Create context with statistics
let context = SimplePlacementContext::new(silo1.clone(), vec![silo1.clone(), silo2.clone()])
    .with_statistics(silo1.clone(), SiloRuntimeStatistics::new(silo1.clone())
        .with_activation_count(100)
        .with_cpu_usage(0.4));

// Create placement target
let grain_type = GrainType::create("my.grain");
let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
let target = PlacementTarget::new(grain_id, grain_type);

// Use registry to get director and select silo
let registry = create_default_registry();
let director = registry.get("ActivationCountBasedPlacement").unwrap();
let strategy = ActivationCountBasedPlacement::default();
let selected = director.on_add_activation(&strategy, &target, &context).await?;
```

---

## Phase 22: Version Tolerance in Serialization ✅

**Objective**: Comprehensive testing and validation of serialization version tolerance for forward and backward compatibility during rolling upgrades.

**Status**: COMPLETE - 15 new version tolerance tests (11 unit tests + 4 property-based tests) passing.

### Overview

Version tolerance enables safe schema evolution in distributed systems:
- **Forward compatibility**: Older code can read data from newer code (unknown fields are skipped)
- **Backward compatibility**: Newer code can read data from older code (missing fields get defaults)

### Implementation Details

The version tolerance mechanism was already implemented in Phase 2 (Binary Serialization):
- `Reader::skip_field()` handles skipping unknown fields based on wire type
- `OrleansDeserialize` derive macro generates `_ => reader.skip_field(&field)?` for unrecognized field IDs
- Fields are initialized with `Default::default()` so missing fields get appropriate defaults

### Test Coverage

- [x] **22.1** Forward compatibility - V2 data deserializes to V1 (unknown fields skipped)
  - `test_forward_compatibility_skips_unknown_fields`
  - `test_forward_compatibility_skips_multiple_unknown_fields`
  - `prop_forward_compatibility_preserves_common_fields`

- [x] **22.2** Backward compatibility - V1 data deserializes to V2 (missing fields get defaults)
  - `test_backward_compatibility_missing_fields_get_defaults`
  - `prop_backward_compatibility_preserves_common_fields`

- [x] **22.3** Sparse field IDs with gaps
  - `test_forward_compatibility_with_sparse_field_ids`
  - `test_backward_compatibility_with_sparse_field_ids`

- [x] **22.4** Nested struct version tolerance
  - `test_nested_forward_compatibility`
  - `test_nested_backward_compatibility`
  - `prop_nested_version_tolerance`

- [x] **22.5** Field reordering (same IDs, different declaration order)
  - `test_field_reordering_same_ids`
  - `prop_field_id_determines_identity`

- [x] **22.6** Empty to non-empty struct evolution
  - `test_empty_struct_forward_compatibility`
  - `test_empty_struct_backward_compatibility`

- [x] **22.7** Different wire types for unknown fields
  - `test_skip_different_wire_types` (VarInt, LengthPrefixed, TagDelimited)

### Tests Location
```
orleans-codegen/tests/derive_serialize_tests.rs
```

### Key Behaviors Validated

| Scenario | Sender Version | Receiver Version | Result |
|----------|---------------|------------------|--------|
| Forward compat | V2 (newer) | V1 (older) | Unknown fields skipped, common fields preserved |
| Backward compat | V1 (older) | V2 (newer) | Missing fields get defaults, common fields preserved |
| Field reorder | Any | Any | Field IDs determine identity, not declaration order |
| Nested structs | V2 inner | V1 inner | Nested unknown fields skipped correctly |
| Sparse IDs | New fields in gaps | Old | Fields in gaps skipped correctly |

---

## Phase 23: TLS/Security ✅

**Objective**: Implement TLS encryption for Orleans cluster communication, including mTLS support, certificate management, and secure connection handling.

**Status**: COMPLETE - 51 unit tests and 2 doc tests passing.

### Tasks

- [x] **23.1** Define error types for security operations
  - `SecurityError` enum with variants: HandshakeFailed, HandshakeTimeout, CertificateNotFound, InvalidCertificate, PrivateKeyNotFound, InvalidPrivateKey, CertificateKeyMismatch, CertificateValidationFailed, CertificateExpired, RemoteCertificateRequired, etc.
  - `SecurityResult<T>` type alias
  - `is_retryable()`, `is_certificate_error()`, `is_configuration_error()` helper methods

- [x] **23.2** Implement TLS options and configuration
  - `RemoteCertificateMode` enum: NoCertificate, AllowCertificate, RequireCertificate
  - `TlsProtocol` enum: Tls12, Tls13
  - `CertificateSource` enum: PemFile, Pkcs12, InMemory, SelfSigned
  - `TlsOptions` struct with builder pattern
  - Presets: `for_testing()`, `production_mtls()`, `production_server_only()`

- [x] **23.3** Implement certificate loading and generation
  - `LoadedCertificate` struct with chain, private key, and metadata
  - `load_certificate(source)` - loads from any CertificateSource
  - `load_pem_certificate(cert_path, key_path)` - PEM file loading
  - `load_in_memory_certificate(cert_pem, key_pem)` - in-memory PEM
  - `generate_self_signed_certificate(cn, sans, days)` - self-signed for testing
  - `load_ca_certificates(path)` - CA certificate loading from file or directory
  - Certificate metadata extraction: common name, SANs, EKU (server/client auth)
  - Validity checking: `is_valid()`, `time_until_expiration()`, `expires_within()`

- [x] **23.4** Implement TLS configuration builders
  - `build_server_config(options)` - creates rustls ServerConfig
  - `build_client_config(options)` - creates rustls ClientConfig
  - Client certificate verification based on RemoteCertificateMode
  - Root certificate store with system roots and custom CAs
  - Insecure verifiers for testing (with warnings)
  - ALPN protocol support (Orleans1)

- [x] **23.5** Implement secure stream wrappers
  - `TlsStream` enum wrapping client/server TLS streams
  - Implements `AsyncRead` and `AsyncWrite` for transparent I/O
  - `peer_addr()`, `local_addr()`, `alpn_protocol()`, `protocol_version()`, `negotiated_cipher_suite()`
  - `SecureAcceptor` for server-side TLS handshake with timeout
  - `SecureConnector` for client-side TLS handshake with SNI support
  - `TlsConnectionInfo` for extracting connection details

### TLS Features

| Feature | Support |
|---------|---------|
| TLS 1.2 | ✓ |
| TLS 1.3 | ✓ (default) |
| Server Authentication | ✓ |
| Client Authentication (mTLS) | ✓ |
| ALPN Negotiation | ✓ ("orleans1") |
| SNI (Server Name Indication) | ✓ |
| Custom CA Certificates | ✓ |
| System Root Certificates | ✓ |
| Self-Signed Certificates | ✓ (testing only) |
| Certificate Expiration Check | ✓ |
| Handshake Timeout | ✓ (default: 10s) |

### Crate Structure
```
orleans-security/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Public API and integration tests
│   ├── error.rs         # SecurityError, SecurityResult
│   ├── options.rs       # TlsOptions, RemoteCertificateMode, CertificateSource
│   ├── certificate.rs   # LoadedCertificate, certificate loading/generation
│   ├── config.rs        # TLS configuration builders
│   └── stream.rs        # TlsStream, SecureAcceptor, SecureConnector
```

### Tests
- Unit tests: error types (5 tests)
- Unit tests: options configuration (10 tests)
- Unit tests: certificate operations (10 tests)
- Unit tests: TLS configuration (9 tests)
- Unit tests: stream operations (8 tests)
- Integration tests: TLS echo roundtrip (4 tests)
- Integration tests: ALPN negotiation (1 test)
- Doc tests: public API examples (2 tests)

### Usage Example
```rust
use orleans_security::{TlsOptions, SecureAcceptor, SecureConnector, CertificateSource};
use std::path::PathBuf;

// For testing with self-signed certificates
let options = TlsOptions::for_testing();
let acceptor = SecureAcceptor::new(&options)?;
let connector = SecureConnector::new(&options)?;

// For production with real certificates
let options = TlsOptions::production_mtls()
    .with_certificate(CertificateSource::PemFile {
        cert_path: PathBuf::from("/etc/orleans/server.crt"),
        key_path: PathBuf::from("/etc/orleans/server.key"),
    })
    .with_custom_ca(PathBuf::from("/etc/orleans/ca.crt"));

// Accept TLS connection (server side)
let tls_stream = acceptor.accept(tcp_stream).await?;

// Connect with TLS (client side)
let tls_stream = connector.connect(tcp_stream, "silo.example.com").await?;
```

---

## Phase 24: Graceful Grain Migration ✅

**Objective**: Implement graceful grain migration between silos without losing state, enabling silo shutdown, cluster rebalancing, and rolling upgrades.

**Status**: COMPLETE - 72 tests passing (68 unit tests + 4 doc tests).

### Tasks

- [x] **24.1** Define error types for migration operations
  - `MigrationError` enum with variants: GrainImmovable, GrainBusy, ActivationNotFound, TargetSiloUnavailable, MigrationRejected, DehydrationFailed, RehydrationFailed, ContextKeyNotFound, ContextTypeMismatch, StateTransferFailed, Timeout, Cancelled, ShuttingDown, AlreadyMigrating, DirectoryUpdateFailed, Serialization, Deserialization, Internal
  - `MigrationResult<T>` type alias
  - `MigrationReason` enum: SiloShutdown, Rebalancing, Manual, ResourceOptimization, VersionUpgrade
  - `is_retryable()`, `is_permanent()`, `is_serialization_error()` helper methods

- [x] **24.2** Implement `MigrationOptions` configuration
  - `migration_timeout` (default: 60s)
  - `dehydration_timeout`, `rehydration_timeout` (default: 10s)
  - `state_transfer_timeout` (default: 30s)
  - `max_concurrent_migrations` (default: 10)
  - `max_retry_attempts` (default: 3)
  - `allow_migration_with_pending_requests` (default: false)
  - `max_context_size` (default: 10MB)
  - `enable_message_forwarding` and `max_forward_count`
  - Presets: `for_testing()`, `for_shutdown()`

- [x] **24.3** Implement `GrainMigrationConfig`
  - `is_migratable`, `persist_before_migration`, `custom_timeout`, `migration_priority`
  - Factory methods: `migratable()`, `immovable()`
  - Builder pattern for configuration

- [x] **24.4** Implement `MigrationContext`
  - Type-safe key-value storage for migration state
  - `try_add_value<T>()` for dehydration (serialization)
  - `try_get_value<T>()` for rehydration (deserialization)
  - `add_bytes()`, `try_get_bytes()` for raw data
  - `to_bytes()`, `from_bytes()` for network transfer
  - Size tracking and max size enforcement
  - `merge()` for combining contexts
  - `SharedMigrationContext` for thread-safe access

- [x] **24.5** Implement `IGrainMigrationParticipant` trait
  - `on_dehydrate(&self, context)` - save state before migration
  - `on_rehydrate(&mut self, context)` - restore state after migration
  - `migration_key_prefix()` - namespace for context keys
  - `has_migration_state()` - check if participant has state to migrate

- [x] **24.6** Implement `IMigratable` trait
  - `can_migrate()` - check if grain can be migrated now
  - `on_migration_start()` - called before dehydration
  - `on_migration_complete()` - called after rehydration

- [x] **24.7** Implement `MigrationParticipantRegistry`
  - Register participants with name and priority
  - `dehydrate_all()` - call participants in priority order
  - `rehydrate_all()` - call participants in reverse priority order
  - `has_migration_state()` - check if any participant has state

- [x] **24.8** Implement `ActivationMigrationManager`
  - `migrate_activation(grain_id, target_silo, reason)` async method
  - `can_migrate(grain_id)` - check migration eligibility
  - `is_migrating(grain_id)` - check if migration in progress
  - `cancel_migration(grain_id)` - cancel pending migration
  - `mark_immovable(grain_id)` / `unmark_immovable(grain_id)`
  - `begin_shutdown()` for graceful silo shutdown
  - `get_statistics()` for migration metrics
  - Concurrent migration limiting via semaphore
  - Migration phase tracking: Preparing, Dehydrating, Transferring, Rehydrating, UpdatingDirectory, Completed, Failed

- [x] **24.9** Implement `MigrationStatistics`
  - `total_migrations`, `successful_migrations`, `failed_migrations`, `cancelled_migrations`
  - `total_bytes_transferred`, `average_duration_ms`

### Migration Flow

```
Source Silo                          Target Silo
    │                                    │
    │ 1. Prepare (drain requests)        │
    ▼                                    │
    │ 2. Dehydrate (serialize state)     │
    ▼                                    │
    │ ──── State Transfer ────────────▶  │
    │                                    ▼
    │                    3. Rehydrate (deserialize state)
    │                                    ▼
    │                    4. Update Directory
    │                                    ▼
    │ 5. Deactivate                      │ Active
```

### Crate Structure
```
orleans-migration/
├── Cargo.toml
├── src/
│   ├── lib.rs           # Public API and integration tests
│   ├── error.rs         # MigrationError, MigrationReason, MigrationResult
│   ├── options.rs       # MigrationOptions, GrainMigrationConfig
│   ├── context.rs       # MigrationContext, SharedMigrationContext
│   ├── participant.rs   # IGrainMigrationParticipant, MigrationParticipantRegistry
│   └── manager.rs       # ActivationMigrationManager, MigrationStatistics
```

### Tests
- Unit tests: error types (7 tests)
- Unit tests: options configuration (10 tests)
- Unit tests: migration context (18 tests)
- Unit tests: participant registry (10 tests)
- Unit tests: migration manager (13 tests)
- Integration tests: full migration flow (6 tests)
- Doc tests: public API examples (4 tests)

### Usage Example
```rust
use orleans_migration::{
    ActivationMigrationManager, MigrationOptions, MigrationReason,
    IGrainMigrationParticipant, MigrationContext,
};

// Configure migration manager
let options = MigrationOptions::default();
let manager = ActivationMigrationManager::new(local_silo, options);

// Migrate a grain
manager.migrate_activation(&grain_id, &target_silo, MigrationReason::Manual).await?;

// Implement migration participant for grain state
#[derive(Debug)]
struct MyGrainState {
    counter: i32,
}

impl IGrainMigrationParticipant for MyGrainState {
    fn on_dehydrate(&self, context: &mut MigrationContext) {
        context.try_add_value("counter", &self.counter);
    }

    fn on_rehydrate(&mut self, context: &MigrationContext) {
        if let Some(value) = context.try_get_value::<i32>("counter") {
            self.counter = value;
        }
    }
}
```

---

## Workspace Structure

```
orleans-rs/
├── Cargo.toml (workspace)
├── orleans-core/              # Identity types
├── orleans-serialization/     # Wire protocol
├── orleans-codegen/           # Proc macros
├── orleans-messaging/         # Message passing
├── orleans-clustering/        # Membership
├── orleans-directory/         # Grain directory
├── orleans-runtime/           # Grain hosting
├── orleans-telemetry/         # Structured logging
├── orleans-persistence/       # Grain state persistence
├── orleans-timers/            # Grain timers
├── orleans-reminders/         # Grain reminders (persistent)
├── orleans-filters/           # Call filters and interceptors
├── orleans-observers/         # Observers and callbacks
├── orleans-streaming/         # Reactive pub/sub streaming
├── orleans-transactions/      # ACID transactions with 2PC
├── orleans-versioning/        # Interface versioning for rolling upgrades
├── orleans-client/            # ClusterClient for external applications
├── orleans-stateless-workers/ # High-throughput parallelizable grains
├── orleans-placement/         # Advanced placement strategies
├── orleans-security/          # TLS/Security support
├── orleans-migration/         # Graceful grain migration
├── orleans-postgres/          # PostgreSQL storage providers
├── orleans-persistence-s3/    # AWS S3 storage providers
├── orleans-event-sourcing/    # Event sourcing for grains
├── orleans-chaos/             # Chaos engineering framework
├── orleans-host/              # Silo assembly
└── orleans-tests/             # Integration tests
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

7. ✅ **All tests pass** - 1577+ tests across all crates (120 core, 80 clustering, 54 directory, 20 telemetry, 55 persistence, 33 timers, 64 reminders, 87 filters, 93 observers, 51 streaming, 100 transactions, 106 versioning, 62 client, 81 stateless-workers, 119 placement, 51 security, 72 migration, 20 postgres, 61 persistence-s3, 60 event-sourcing, 117 chaos, 40 host, 39 codegen including 34 property tests)

8. ✅ **Grain persistence** - Grains can persist state durably with optimistic concurrency control
   - Verified by: `orleans-persistence` crate with 49 unit tests and 6 doc tests

9. ✅ **Grain reminders** - Persistent scheduled callbacks that survive silo restarts
   - Verified by: `orleans-reminders` crate with 64 unit tests and 1 doc test

10. ✅ **Call filters and interceptors** - Middleware pipeline for cross-cutting concerns
    - Verified by: `orleans-filters` crate with 87 unit tests

11. ✅ **Observers and callbacks** - Pub/sub communication for grains
    - Verified by: `orleans-observers` crate with 91 unit tests and 2 doc tests

12. ✅ **Streaming** - Reactive pub/sub messaging for event processing
    - Verified by: `orleans-streaming` crate with 51 unit tests and 1 doc test

13. ✅ **Property-based tests** - Comprehensive invariant verification for distributed system correctness
    - Verified by: 22 property tests in `property_tests.rs` covering:
      - Grain identity properties (equality, hashing, parse/display roundtrip)
      - Directory consistency (register/lookup/unregister invariants)
      - Message delivery guarantees (no loss, no duplicates, data integrity)

14. ✅ **ACID Transactions** - Two-phase commit protocol for distributed state consistency
    - Verified by: `orleans-transactions` crate with 100 unit tests
    - Features: CausalClock, ReaderWriterLock, TransactionAgent, TransactionalState

15. ✅ **Interface Versioning** - Rolling upgrades with heterogeneous cluster deployments
    - Verified by: `orleans-versioning` crate with 97 unit tests and 9 doc tests
    - Features: CompatibilityDirector, VersionSelector, GrainVersionManifest, CachedVersionSelectorManager, PlacementTarget

16. ✅ **Stateless Workers** - High-throughput parallelizable grains without state preservation
    - Verified by: `orleans-stateless-workers` crate with 81 unit tests
    - Features: PID controller for adaptive pool sizing, worker state tracking, placement director, context coordinator
    - Multiple activations per grain identity for parallel processing

17. ✅ **Advanced Placement Strategies** - Production-grade load balancing and resource optimization
    - Verified by: `orleans-placement` crate with 118 unit tests and 1 doc test
    - Features: RandomPlacement, HashBasedPlacement, PreferLocalPlacement, ActivationCountBasedPlacement, ResourceOptimizedPlacement
    - Power-of-k-choices algorithm for efficient load balancing
    - Multi-dimensional resource scoring with configurable weights

18. ✅ **Version Tolerance in Serialization** - Forward and backward compatibility for rolling upgrades
    - Verified by: 15 version tolerance tests (11 unit tests + 4 property-based tests) in `orleans-codegen`
    - Features: Unknown field skipping, default values for missing fields, nested struct version tolerance
    - Enables safe schema evolution without breaking existing deployments

19. ✅ **TLS/Security** - Encrypted cluster communication with mutual TLS support
    - Verified by: `orleans-security` crate with 51 unit tests and 2 doc tests
    - Features: TLS 1.2/1.3, mTLS, certificate loading, self-signed cert generation, ALPN negotiation
    - Secure acceptor/connector for server and client handshakes

20. ✅ **Graceful Grain Migration** - Move grains between silos without losing state
    - Verified by: `orleans-migration` crate with 68 unit tests and 4 doc tests
    - Features: MigrationContext for state transfer, IGrainMigrationParticipant trait, ActivationMigrationManager
    - Dehydration/rehydration pattern with priority-ordered participants
    - Support for silo shutdown, cluster rebalancing, and rolling upgrades

21. ✅ **Event Sourcing** - Audit trails, temporal queries, and state reconstruction
    - Verified by: `orleans-event-sourcing` crate with 60 unit tests
    - Features: JournaledGrain, EventApplier, LogViewAdaptor, InMemoryEventStorage, InMemorySnapshotStorage
    - Temporal queries: get_state_at_version, get_events_since, get_events_in_range
    - Automatic snapshots with configurable intervals for fast recovery

22. ✅ **Chaos Testing** - Chaos engineering framework for cluster resilience validation
    - Verified by: `orleans-chaos` crate with 117 unit tests
    - Features: ChaosController, NetworkFaultInjector, ProcessFaultInjector, StorageFaultInjector, ChaosReporter
    - Network faults: delay, packet loss, partition, bandwidth throttling
    - Process faults: kill, pause, resume, memory pressure, CPU throttling
    - Storage faults: read/write failure, latency, corruption
    - Comprehensive reporting with timeline events, cluster snapshots, and recovery metrics

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

Phase 16 (Streaming)   ←── Phase 1 (Identity) + Phase 15 (Observers) + tokio

Phase 17 (Transactions) ←── Phase 1 (Identity) + Phase 11 (Persistence) + tokio

Phase 18 (ClusterClient) ←── Phase 1 (Identity) + Phase 3 (Messaging) + Phase 4 (Clustering) + Phase 6 (Runtime)

Phase 19 (Versioning)   ←── Phase 1 (Identity) + Phase 5 (Directory)

Phase 20 (Stateless Workers) ←── Phase 1 (Identity) + Phase 6 (Runtime) + tokio

Phase 21 (Placement)   ←── Phase 1 (Identity) + Phase 4 (Clustering) + rand

Phase 22 (Version Tolerance) ←── Phase 2 (Serialization) + Phase 7 (Codegen)

Phase 23 (Security)    ←── Phase 1 (Identity) + tokio-rustls + rustls

Phase 24 (Migration)   ←── Phase 1 (Identity) + Phase 6 (Runtime) + tokio

Phase 25 (PostgreSQL)  ←── Phase 4 (Clustering) + Phase 11 (Persistence) + Phase 13 (Reminders) + sqlx

Phase 26 (S3 Storage)  ←── Phase 9 (Streaming) + aws-sdk-s3

Phase 27 (Network Tests) ←── All above phases

Phase 28 (Event Sourcing) ←── Phase 11 (Persistence) + Phase 17 (Transactions)

Phase 29 (Chaos Testing) ←── Phase 27 (Network Tests)

Phase 30 (Benchmarks)  ←── All above phases + criterion

Phase 31 (Queue Adapters) ←── Phase 16 (Streaming) + rdkafka/lapin
```

Estimated complexity: ~35,000-45,000 lines of Rust code.

---

# Post-MVP Phases

The following phases extend Orleans-RS beyond the MVP with production-grade storage backends,
comprehensive testing infrastructure, and advanced features.

---

## Phase 25: PostgreSQL Storage Provider ✅

**Objective**: Implement PostgreSQL as a production-grade storage backend for membership tables, grain state persistence, and reminder storage.

**Status**: COMPLETE - 20 unit tests passing.

### Tasks

- [x] **25.1** Define database schema for Orleans tables
  - `membership` table with silo address, status, heartbeat, suspect votes
  - `membership_version` table for optimistic concurrency control
  - `grain_state` table with grain_type, grain_key, state_name, state_data (BYTEA), etag
  - `reminders` table with grain_type, grain_key, reminder_name, start_at, period, etag
  - Indexes for efficient lookups by hash ranges and grain IDs

- [x] **25.2** Implement `PostgresMembershipTable`
  - Implements `IMembershipTable` trait
  - Connection pooling with `sqlx::PgPool`
  - Optimistic concurrency with ETags and table versions
  - `read_all()`, `read_row()`, `insert_row()`, `update_row()`, `update_i_am_alive()`
  - `delete_membership_table_entries()`, `cleanup_defunct_silo_entries()`, `initialize_membership_table()`
  - Structured logging via `tracing` crate

- [x] **25.3** Implement `PostgresGrainStorage`
  - Implements `IGrainStorage` trait
  - `read_state()`, `write_state()`, `clear_state()`
  - State stored as BYTEA for efficient binary storage
  - ETag-based optimistic concurrency control
  - Wildcard ETag ("*") support for unconditional upserts
  - Structured logging via `tracing` crate

- [x] **25.4** Implement `PostgresReminderTable`
  - Implements `IReminderTable` trait
  - `read_rows()`, `read_row()`, `read_rows_in_range()`, `upsert_row()`, `remove_row()`, `clear_table()`
  - Hash range queries for silo ownership using grain_hash column
  - ETag-based optimistic concurrency with atomic counter
  - Structured logging via `tracing` crate

- [x] **25.5** Implement connection management
  - `PostgresOptions` configuration (connection string, pool size, timeouts)
  - Configurable min/max connections, connect timeout, idle timeout, max lifetime
  - Schema and cluster_id configuration
  - `for_testing()` preset with shorter timeouts
  - Validation of configuration options

- [x] **25.6** Implement schema migrations
  - Automatic schema creation (CREATE SCHEMA IF NOT EXISTS)
  - Automatic table creation with appropriate indexes
  - `run_migrations` option to control migration execution
  - Index creation for performance optimization

### Crate Structure
```
orleans-postgres/
├── Cargo.toml
├── src/
│   ├── lib.rs             # Public API and re-exports
│   ├── error.rs           # PostgresError, PostgresResult
│   ├── options.rs         # PostgresOptions configuration
│   ├── membership.rs      # PostgresMembershipTable
│   ├── storage.rs         # PostgresGrainStorage
│   └── reminder.rs        # PostgresReminderTable
```

### Tests
- Unit tests: error types (4 tests)
- Unit tests: options configuration (5 tests)
- Unit tests: membership operations (2 tests)
- Unit tests: grain storage operations (2 tests)
- Unit tests: reminder table operations (3 tests)
- Unit tests: library-level integration (4 tests)

### Usage Example
```rust
use orleans_postgres::{PostgresOptions, PostgresMembershipTable, PostgresGrainStorage, PostgresReminderTable};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL connection
    let options = PostgresOptions::new("postgres://user:pass@localhost/orleans")
        .with_schema("my_cluster")
        .with_max_connections(20);

    // Create storage providers
    let membership = PostgresMembershipTable::new(&options).await?;
    let storage = PostgresGrainStorage::new(&options).await?;
    let reminders = PostgresReminderTable::new(&options).await?;

    // Use with Orleans silo/client...
    Ok(())
}
```

### Dependencies
```toml
sqlx = { version = "0.8", features = ["postgres", "runtime-tokio", "chrono", "uuid", "json"] }
```

---

## Phase 26: S3 Storage Provider ✅

**Objective**: Implement AWS S3-compatible object storage for large grain state, event logs, and stream checkpoints.

**Status**: COMPLETE - 61 unit tests passing.

### Tasks

- [x] **26.1** Implement `S3GrainStorage`
  - Implements `IGrainStorage` trait
  - Object key format: `{prefix}grains/{grain_type}/{grain_key}/{state_name}.bin`
  - ETag-based optimistic concurrency via S3 conditional requests
  - Optional compression (gzip, zstd)
  - Server-side encryption (SSE-S3, SSE-KMS)
  - Structured logging via `tracing` crate

- [x] **26.2** Implement `S3StreamCheckpointStorage`
  - Checkpoint persistence for stream consumers
  - Object key format: `{prefix}checkpoints/{namespace}/{stream_key}/{consumer_id}.json`
  - ETag-based optimistic concurrency for atomic updates
  - List checkpoints and streams
  - Structured logging via `tracing` crate

- [x] **26.3** Implement `S3EventLogStorage`
  - Append-only event log for event sourcing
  - Object key format: `{prefix}events/{grain_id}/{sequence:020}.json`
  - Batch writes for efficiency with configurable batch size
  - Range reads for event replay
  - Zero-padded sequence numbers for lexicographic ordering
  - Structured logging via `tracing` crate

- [x] **26.4** Implement S3 client configuration
  - `S3Options` with bucket, region, credentials, endpoint
  - Support for S3-compatible services (MinIO, LocalStack)
  - Presets: `for_testing()`, `for_minio()`
  - Key prefix support for multi-tenant deployments
  - Configurable timeouts and retry settings

- [x] **26.5** Implement retry and resilience
  - Exponential backoff via AWS SDK retry configuration
  - Configurable retry policies (max_retry_attempts, retry_base_delay, retry_max_delay)
  - Compression support (gzip, zstd) with configurable levels
  - Request and connection timeout handling
  - Checksum validation (CRC32C)

### Crate Structure
```
orleans-persistence-s3/
├── Cargo.toml
├── src/
│   ├── lib.rs             # Public API and integration tests
│   ├── error.rs           # S3Error, S3Result
│   ├── options.rs         # S3Options, CompressionType configuration
│   ├── client.rs          # S3Client wrapper with retry, compression
│   ├── grain_storage.rs   # S3GrainStorage implementing IGrainStorage
│   ├── checkpoint.rs      # S3StreamCheckpointStorage, StreamCheckpoint
│   └── event_log.rs       # S3EventLogStorage, EventEntry, EventBatch
```

### Tests
- Unit tests: error types (9 tests)
- Unit tests: options configuration (15 tests)
- Unit tests: S3 client operations (5 tests)
- Unit tests: grain storage key generation (5 tests)
- Unit tests: checkpoint operations (6 tests)
- Unit tests: event log operations (9 tests)
- Unit tests: library integration (12 tests)

### Usage Example
```rust
use orleans_persistence_s3::{S3Options, S3GrainStorage, CompressionType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure S3 connection
    let options = S3Options::new("my-orleans-bucket")
        .with_region("us-east-1")
        .with_key_prefix("orleans/")
        .with_compression(CompressionType::Gzip)
        .with_sse();

    // Create grain storage provider
    let storage = S3GrainStorage::new(options).await?;

    // For testing with LocalStack
    let test_options = S3Options::for_testing("test-bucket");
    let test_storage = S3GrainStorage::new(test_options).await?;

    Ok(())
}
```

### Dependencies
```toml
aws-sdk-s3 = "1.77"
aws-config = { version = "1.6", features = ["behavior-version-latest"] }
flate2 = "1.0"
zstd = "0.13"
```

---

## Phase 27: Real Network Integration Testing ✅

**Objective**: Implement comprehensive integration tests with real network communication between separate processes, validating cluster behavior under realistic conditions.

**Status**: COMPLETE - 18 unit tests passing.

### Tasks

- [x] **27.1** Implement `TestClusterBuilder`
  - Spawn multiple silo processes
  - Configurable number of silos (default: 3)
  - Process lifecycle management (start, stop, kill)
  - Port allocation and management
  - Shared configuration via MembershipTableServer

- [x] **27.2** Implement process-based silo launcher
  - `SiloProcess` struct wrapping `tokio::process::Child`
  - Stdout/stderr capture for debugging
  - JSON event parsing for process coordination
  - Graceful shutdown with timeout
  - Force kill on test failure
  - `try_kill_sync()` for Drop cleanup

- [x] **27.3** Implement cluster formation tests
  - Three silos join and form cluster
  - Verify all silos see each other as Active
  - Verify consistent membership table state
  - Test join/leave/rejoin scenarios
  - Test cluster restart

- [x] **27.4** Implement grain communication tests
  - Create grain on Silo1, call from Silo2
  - Verify single activation guarantee across processes
  - Test grain migration during silo shutdown
  - Test grain state persistence across calls

- [x] **27.5** Implement failure scenario tests
  - Silo crash detection and recovery
  - Grain reactivation after silo failure
  - Directory consistency after recovery
  - Multiple silo failure tolerance
  - Graceful shutdown with pending requests

- [x] **27.6** Implement performance tests
  - Cross-silo call latency baseline measurement
  - Cluster startup time measurement
  - Memory usage baseline (placeholder)
  - Connection efficiency tests
  - Shutdown time measurement
  - `PerformanceMetrics` struct for tracking

### Crate Structure
```
orleans-tests-integration/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── cluster_builder.rs  # TestClusterBuilder
│   ├── silo_process.rs     # SiloProcess management
│   ├── client_harness.rs   # Test client utilities
│   └── assertions.rs       # Cluster state assertions
├── tests/
│   ├── cluster_formation.rs
│   ├── grain_communication.rs
│   ├── failure_scenarios.rs
│   ├── persistence_integration.rs
│   └── performance_baseline.rs
```

### Tests
- Cluster formation: 3-silo cluster forms correctly
- Cluster formation: silo join after initial formation
- Cluster formation: silo graceful leave
- Grain communication: cross-silo grain invocation
- Grain communication: grain state persistence
- Grain communication: reminder firing across silos
- Failure: silo crash detected within timeout
- Failure: grain reactivates on healthy silo
- Failure: directory consistency after recovery
- Performance: baseline latency measurements

---

## Phase 28: Event Sourcing ✅

**Objective**: Implement event sourcing infrastructure for grains that need audit trails, temporal queries, or complex state reconstruction.

**Status**: COMPLETE - 60 unit tests passing.

### Tasks

- [x] **28.1** Define event sourcing core types
  - `ILogConsistentGrain` trait marker
  - `ILogViewAdaptor` trait for event log access
  - `EventEntry<E>` with sequence, timestamp, payload
  - `LogViewState<S, E>` combining state and events
  - `EventMetadata` for correlation ID, user ID, custom data
  - `EventApplier<S, E>` trait for state transitions

- [x] **28.2** Implement `LogViewAdaptorFactory`
  - Creates adaptors for different storage backends
  - In-memory adaptor for testing
  - Pluggable storage provider interface
  - `LogViewAdaptorOptions` for snapshot interval, max snapshots, auto-snapshot

- [x] **28.3** Implement `JournaledGrain<S, E>` base
  - State type `S`, Event type `E`, Applier type `A`
  - `raise_event(event)` for appending new events
  - `raise_event_with_metadata(event, metadata)` for events with metadata
  - `confirm_events()` to persist pending events
  - `abort_pending_events()` to discard uncommitted changes
  - `confirmed_version()` and `tentative_version()` for version tracking
  - `JournaledGrainBuilder` for fluent configuration
  - Structured logging via `tracing` crate

- [x] **28.4** Implement event persistence
  - `IEventStorage` trait for event persistence
  - `ISnapshotStorage` trait for snapshot persistence
  - `InMemoryEventStorage` implementation for testing
  - `InMemorySnapshotStorage` implementation for testing
  - `InMemoryLogStorage` combined storage
  - Version conflict detection with optimistic concurrency
  - Sequence validation for event ordering

- [x] **28.5** Implement snapshot support
  - Periodic state snapshots for fast recovery
  - Configurable snapshot interval
  - Snapshot + events replay for state reconstruction
  - `SnapshotConfig` and `SnapshotMetadata` types
  - `SnapshotState` for tracking snapshot history
  - Automatic old snapshot cleanup

- [x] **28.6** Implement temporal queries
  - `get_state_at_version(version)` - state at specific version
  - `get_events_since(version)` - events after specific version
  - `get_events_in_range(from, to)` - events in version range

### Crate Structure
```
orleans-event-sourcing/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs           # EventSourcingError, EventSourcingResult
│   ├── traits.rs          # ILogConsistentGrain, ILogViewAdaptor, IEventStorage, ISnapshotStorage
│   ├── event_entry.rs     # EventEntry, EventMetadata, LogViewState
│   ├── adaptor.rs         # LogViewAdaptor, LogViewAdaptorFactory, LogViewAdaptorOptions
│   ├── journaled_grain.rs # JournaledGrain, JournaledGrainBuilder
│   ├── snapshot.rs        # SnapshotConfig, SnapshotMetadata, SnapshotState
│   └── storage/
│       ├── mod.rs
│       └── memory.rs      # InMemoryEventStorage, InMemorySnapshotStorage, InMemoryLogStorage
```

### Tests
- Unit tests: error types (5 tests)
- Unit tests: event entry and metadata (10 tests)
- Unit tests: log view state operations (10 tests)
- Unit tests: traits and event applier (3 tests)
- Unit tests: in-memory event storage (6 tests)
- Unit tests: in-memory snapshot storage (4 tests)
- Unit tests: log view adaptor (7 tests)
- Unit tests: journaled grain (7 tests)
- Unit tests: snapshot config and state (8 tests)
- Integration tests: full event sourcing flow (4 tests)
- Integration tests: temporal queries (1 test)

### Usage Example
```rust
use orleans_event_sourcing::{
    JournaledGrain, EventApplier, InMemoryEventStorage,
    InMemorySnapshotStorage, EventMetadata,
};
use std::sync::Arc;

#[derive(Clone, Default, Serialize, Deserialize)]
struct BankAccountState {
    balance: i64,
}

#[derive(Clone, Serialize, Deserialize)]
enum BankAccountEvent {
    Deposited(i64),
    Withdrawn(i64),
}

struct BankAccountApplier;

impl EventApplier<BankAccountState, BankAccountEvent> for BankAccountApplier {
    fn apply(state: &mut BankAccountState, event: &BankAccountEvent) {
        match event {
            BankAccountEvent::Deposited(amount) => state.balance += amount,
            BankAccountEvent::Withdrawn(amount) => state.balance -= amount,
        }
    }
}

async fn example() -> EventSourcingResult<()> {
    let event_storage = Arc::new(InMemoryEventStorage::new());
    let snapshot_storage = Arc::new(InMemorySnapshotStorage::new());

    let mut grain = JournaledGrain::<_, _, BankAccountApplier>::with_snapshots(
        grain_id,
        event_storage,
        snapshot_storage,
    );

    grain.on_activate().await?;

    // Raise events (applied to tentative state)
    grain.raise_event(BankAccountEvent::Deposited(100));
    grain.raise_event(BankAccountEvent::Withdrawn(30));

    // Confirm (persists to storage)
    grain.confirm_events().await?;

    // Query state at previous version
    let old_state = grain.get_state_at_version(1).await?;

    grain.on_deactivate().await?;
    Ok(())
}
```

---

## Phase 29: Chaos Testing ✅

**Objective**: Implement chaos engineering framework for validating cluster resilience under adverse conditions.

**Status**: COMPLETE - 117 unit tests passing.

### Tasks

- [x] **29.1** Implement fault injection framework
  - `ChaosController` for orchestrating faults
  - `FaultInjector` trait for different fault types
  - Configurable fault schedules and probabilities
  - `FaultDescriptor`, `FaultState`, `FaultSchedule`, `FaultId` types
  - Support for immediate, delayed, and scheduled faults
  - Probability-based fault activation

- [x] **29.2** Implement network fault injection
  - `NetworkFaultInjector` with `NetworkFaultConfig`
  - Packet delay injection (simulate latency)
  - Packet loss injection (simulate unreliable network)
  - Partition injection (isolate nodes)
  - Bandwidth throttling
  - Connection-specific and all-silos targeting

- [x] **29.3** Implement process fault injection
  - `ProcessFaultInjector` with `ProcessFaultConfig`
  - Silo process kill (SIGKILL)
  - Silo process pause (SIGSTOP/SIGCONT)
  - Process resume (SIGCONT)
  - Memory pressure simulation
  - CPU throttling
  - Silo-to-PID mapping for easy targeting

- [x] **29.4** Implement storage fault injection
  - `StorageFaultInjector` with `StorageFaultConfig`
  - Storage read failures with configurable probability
  - Storage write failures with configurable probability
  - Storage latency injection
  - Storage corruption simulation (bit flipping)
  - Grain-specific and wildcard targeting

- [x] **29.5** Implement chaos test scenarios
  - Random silo selection for fault injection
  - Network partition and heal lifecycle
  - Storage unavailability and recovery
  - Multiple concurrent fault support
  - Maximum concurrent fault limits

- [x] **29.6** Implement chaos test reporting
  - `ChaosReporter` for comprehensive test reporting
  - Fault timeline logging with `TimelineEvent`
  - Cluster state snapshots with `ClusterSnapshot`
  - Recovery time measurement with `RecoveryMetrics`
  - Data consistency verification tracking
  - JSON report generation
  - Test run summary with pass/fail statistics

### Crate Structure
```
orleans-chaos/
├── Cargo.toml
├── src/
│   ├── lib.rs             # Public API and integration tests
│   ├── error.rs           # ChaosError, ChaosResult
│   ├── controller.rs      # ChaosController, ChaosControllerConfig
│   ├── injector.rs        # FaultInjector trait, FaultDescriptor, FaultState
│   ├── network.rs         # NetworkFaultInjector, NetworkFaultConfig
│   ├── process.rs         # ProcessFaultInjector, ProcessFaultConfig
│   ├── storage.rs         # StorageFaultInjector, StorageFaultConfig
│   └── reporting.rs       # ChaosReporter, TimelineEvent, ClusterSnapshot
```

### Tests
- Unit tests: error types (10 tests)
- Unit tests: fault injector types and scheduling (15 tests)
- Unit tests: network fault injection (15 tests)
- Unit tests: process fault injection (18 tests)
- Unit tests: storage fault injection (17 tests)
- Unit tests: chaos controller (15 tests)
- Unit tests: chaos reporting (21 tests)
- Integration tests: full chaos workflow (6 tests)

### Usage Example
```rust
use orleans_chaos::{
    ChaosController, ChaosReporter, FaultDescriptor, FaultType,
    FaultTarget, FaultSchedule, FaultParameters,
};
use std::sync::Arc;
use std::time::Duration;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    // Create reporter and controller
    let reporter = Arc::new(ChaosReporter::new("resilience-test"));
    let controller = ChaosController::for_testing()
        .with_reporter(reporter.clone());
    controller.start().await?;

    // Inject network partition
    let partition = FaultDescriptor::new(
        "partition-silo1-silo2",
        FaultType::NetworkPartition,
        FaultTarget::Connection {
            source: "silo-1".to_string(),
            destination: "silo-2".to_string(),
        },
        FaultSchedule::immediate(Some(Duration::from_secs(30))),
        FaultParameters::new(),
    );
    let fault_id = controller.schedule_fault(partition).await?;

    // ... run tests while partition is active ...

    // Heal the partition
    controller.heal_fault(&fault_id).await?;

    // Generate report
    let report = reporter.generate_json_report()?;
    println!("{}", report);

    controller.stop().await?;
    Ok(())
}
```

---

## Phase 30: Performance Benchmarks ✅

**Objective**: Establish comprehensive performance benchmarks for measuring and tracking Orleans-RS performance characteristics.

**Status**: COMPLETE - 69 unit tests and 2 doc tests passing.

### Tasks

- [x] **30.1** Implement micro-benchmarks
  - Serialization/deserialization throughput (VarInt encode/decode, Writer operations)
  - Message encoding/decoding latency (Message creation, serialize, deserialize, roundtrip)
  - Grain activation/deactivation cost (ActivationId, GrainType, GrainAddress operations)
  - Directory lookup performance (RingRange, GrainDirectoryPartition, hash distribution)

- [x] **30.2** Implement macro-benchmarks
  - End-to-end grain call latency (request-response cycle with different payload sizes)
  - Grain invocation overhead (simple method calls, method with args deserialization)
  - Async overhead (tokio spawn, oneshot channel roundtrip)
  - Simulated grain call with lookup

- [x] **30.3** Implement concurrent operation benchmarks
  - Concurrent directory lookups (10 and 100 concurrent requests)
  - DashMap vs HashMap comparison
  - Batch message serialization/deserialization (100 messages)

- [x] **30.4** Implement benchmark infrastructure
  - BenchmarkConfig with performance targets
  - LatencyTimer with HDRHistogram integration
  - BenchmarkStats with statistical analysis (mean, stddev, percentiles)
  - TestDataGenerator for consistent test data
  - BenchmarkRunner with structured logging via tracing
  - BenchmarkReporter with JSON result storage and baseline comparison

### Implementation Details

**Crate**: `orleans-bench`

**Benchmark files**:
- `benches/serialization.rs` - VarInt, Writer, identity type benchmarks
- `benches/messaging.rs` - CorrelationId, Message, GrainInterfaceType benchmarks
- `benches/activation.rs` - ActivationId, GrainType, GrainId lookup, Arc, synchronization benchmarks
- `benches/directory.rs` - RingRange, GrainDirectoryPartition, concurrent directory benchmarks
- `benches/e2e_latency.rs` - Full request-response cycle, async overhead, concurrent requests

**Library modules**:
- `src/lib.rs` - BenchmarkConfig, PerformanceTargets
- `src/harness.rs` - LatencyTimer, BenchmarkStats, TestDataGenerator, BenchmarkRunner
- `src/reporting.rs` - BenchmarkResult, BenchmarkBaseline, ComparisonResult, BenchmarkStorage, BenchmarkReporter
- `src/continuous.rs` - ContinuousConfig, ContinuousRunner, ContinuousStorage, BenchmarkHistory, TrendAnalysis, AnalysisReport
- `src/visualization.rs` - AsciiChart, CsvExporter, HtmlReportGenerator, VisualizationConfig
- `src/ci.rs` - CiRunner, ExitCode, JUnitTestSuite, GitHubActionsOutput

- [x] **30.5** Implement continuous benchmarking
  - Benchmark result storage (BenchmarkHistory, ContinuousStorage with JSON persistence)
  - Regression detection (TrendAnalysis with z-score and moving average analysis)
  - Performance trend visualization (AsciiChart, CsvExporter, HtmlReportGenerator with Chart.js)
  - CI integration for benchmark runs (JUnit XML output, GitHub Actions annotations, exit codes)

### Crate Structure
```
orleans-bench/
├── Cargo.toml
├── benches/
│   ├── serialization.rs   # Serialization benchmarks
│   ├── messaging.rs       # Message throughput benchmarks
│   ├── activation.rs      # Grain lifecycle benchmarks
│   ├── directory.rs       # Directory lookup benchmarks
│   ├── e2e_latency.rs     # End-to-end latency benchmarks
│   └── scalability.rs     # Scalability benchmarks
├── src/
│   ├── lib.rs
│   ├── harness.rs         # Benchmark harness utilities
│   ├── reporting.rs       # Result collection and reporting
│   ├── continuous.rs      # Continuous benchmarking infrastructure
│   ├── visualization.rs   # Trend visualization (ASCII, CSV, HTML)
│   └── ci.rs              # CI/CD integration (JUnit, GitHub Actions)
```

### Benchmark Targets
- Serialization: >1M messages/sec for small messages
- Grain call: <1ms p95 latency for local calls
- Cross-silo call: <5ms p95 latency
- Activation rate: >10K activations/sec
- Directory lookup: <100µs p95

### Dependencies
```toml
criterion = { version = "0.5", features = ["html_reports"] }
```

---

## Phase 31: Queue Adapters for Streaming ⏳

**Objective**: Implement queue adapters for integrating Orleans streaming with external message brokers (Kafka, RabbitMQ, etc.).

**Status**: PLANNED

### Tasks

- [ ] **31.1** Implement Kafka adapter
  - `KafkaQueueAdapter` implementing `IQueueAdapter`
  - Producer for publishing to Kafka topics
  - Consumer for reading from Kafka topics
  - Offset management for checkpointing
  - Consumer group support

- [ ] **31.2** Implement RabbitMQ adapter
  - `RabbitMQQueueAdapter` implementing `IQueueAdapter`
  - Publisher for exchanges/queues
  - Consumer with acknowledgment
  - Dead letter queue support

- [ ] **31.3** Implement Azure Service Bus adapter (future)
  - Queue and topic support
  - Session support for ordering
  - Scheduled message delivery

- [ ] **31.4** Implement adapter configuration
  - `QueueAdapterOptions` base trait
  - Per-adapter configuration (brokers, auth, etc.)
  - Connection pool management
  - Retry policies

- [ ] **31.5** Implement batch processing
  - Batch message retrieval
  - Batch acknowledgment
  - Configurable batch sizes
  - Backpressure handling

### Crate Structure
```
orleans-streaming-kafka/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── adapter.rs         # KafkaQueueAdapter
│   ├── producer.rs        # Kafka producer
│   ├── consumer.rs        # Kafka consumer
│   └── options.rs         # KafkaAdapterOptions

orleans-streaming-rabbitmq/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── adapter.rs         # RabbitMQQueueAdapter
│   ├── publisher.rs       # RabbitMQ publisher
│   ├── consumer.rs        # RabbitMQ consumer
│   └── options.rs         # RabbitMQAdapterOptions
```

### Tests
- Integration tests: Kafka produce/consume roundtrip
- Integration tests: Kafka consumer group behavior
- Integration tests: RabbitMQ publish/subscribe
- Integration tests: Dead letter queue handling
- Integration tests: Backpressure behavior

### Dependencies
```toml
# For Kafka
rdkafka = { version = "0.36", features = ["tokio"] }

# For RabbitMQ
lapin = "2.3"
```

---

## Additional Non-MVP Considerations

The following areas are identified for future development beyond the planned phases:

### Observability & Operations
- **Dashboard UI**: Web-based cluster monitoring and management
- **Prometheus Metrics**: Export metrics in Prometheus format
- **OpenTelemetry Integration**: Distributed tracing with OTLP export
- **Health Check Endpoints**: HTTP endpoints for load balancer health checks

### Security Enhancements
- **Authorization Filters**: Role-based access control for grain methods
- **Audit Logging**: Comprehensive audit trail for grain operations
- **Secrets Management**: Integration with HashiCorp Vault or AWS Secrets Manager

### Cloud-Native Features
- **Kubernetes Operator**: CRD-based Orleans cluster management
- **Auto-Scaling**: Scale silos based on load metrics
- **Service Mesh Integration**: Istio/Linkerd compatibility

### Developer Experience
- **CLI Tool**: Command-line tool for cluster management
- **Hot Reload**: Development-time grain code hot reload
