# Orleans Rust Port - Minimum Viable Implementation Plan

## Goal

Three separate Rust processes functioning as a single Orleans cluster where:
- A grain created/activated on Process1 is accessible from Process2 or Process3
- Grains have location transparency (caller doesn't know which silo hosts the grain)
- Single-writer guarantee (only one activation per grain across the cluster)

## Non-Goals for MVP

- Persistence (grains are in-memory only)
- Transactions
- Streaming
- Timers and Reminders
- Observers/Callbacks
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

- [ ] **9.7** Silo failure handling (Future work)
  - Start 3 silos, create grain
  - Kill silo hosting grain
  - Call grain, verify it re-activates on another silo

- [ ] **9.8** Grain state isolation (Future work)
  - Create grain, set state
  - Call from another silo, verify state persists
  - Verify no cross-grain state leakage

- [ ] **9.9** Concurrent grain calls (Future work)
  - Many simultaneous calls to same grain
  - Verify turn-based execution (no races)

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

7. ✅ **All tests pass** - 275+ tests across all crates (120 core, 80 clustering, 54 directory, 18 host)

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
```

Estimated complexity: ~8,000-12,000 lines of Rust code for MVP.
