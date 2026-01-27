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

## Phase 2: Binary Serialization

**Objective**: Implement Orleans wire protocol for network communication.

### Tasks

- [ ] **2.1** Implement VarInt encoding/decoding
  - `write_varint(writer: &mut impl Write, value: u64)`
  - `read_varint(reader: &mut impl Read) -> u64`
  - ZigZag encoding for signed integers
  - Property test: `decode(encode(n)) == n` for all u64

- [ ] **2.2** Implement wire types enum
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

- [ ] **2.3** Implement field header encoding
  - 1-byte header: WireType(3 bits) + SchemaType(2 bits) + FieldIdDelta(3 bits)
  - Extended field IDs for delta > 6

- [ ] **2.4** Implement `Writer` struct
  - Buffer management with segment pooling
  - `write_field_header(field_id_delta, wire_type, schema_type)`
  - `write_varint()`, `write_fixed32()`, `write_fixed64()`
  - `write_length_prefixed(bytes)`

- [ ] **2.5** Implement `Reader` struct
  - `read_field_header() -> Field`
  - `read_varint()`, `read_fixed32()`, `read_fixed64()`
  - `read_length_prefixed() -> &[u8]`
  - `skip_field(wire_type)` for forward compatibility

- [ ] **2.6** Implement primitive codecs
  - `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
  - `f32`, `f64`
  - `bool`
  - `String` (UTF-8, length-prefixed)
  - `Vec<u8>` (raw bytes)

- [ ] **2.7** Implement identity type codecs
  - `IdSpan`, `GrainType`, `GrainId`, `SiloAddress`, `ActivationId`, `GrainAddress`

- [ ] **2.8** Implement `#[derive(OrleansSerialize)]` proc macro (basic version)
  - Generates `IFieldCodec` implementation for structs
  - Field IDs via `#[id(n)]` attribute

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

## Phase 3: Messaging Infrastructure

**Objective**: Enable message passing between silos over TCP.

### Tasks

- [ ] **3.1** Define `Message` struct
  ```rust
  struct Message {
      id: CorrelationId,
      direction: Direction,  // Request, Response, OneWay
      target_grain: GrainId,
      target_silo: Option<SiloAddress>,
      target_activation: Option<ActivationId>,
      sending_grain: GrainId,
      sending_silo: SiloAddress,
      interface_type: GrainInterfaceType,
      method_id: u32,
      body: Vec<u8>,  // Serialized arguments/result
  }
  ```

- [ ] **3.2** Implement `CorrelationId`
  - `CorrelationId { nonce: u64, counter: u64 }`
  - Unique per-message identifier for request/response matching

- [ ] **3.3** Implement `MessageFactory`
  - `create_request(target, interface, method, args) -> Message`
  - `create_response(request, result) -> Message`
  - `create_rejection(request, reason) -> Message`

- [ ] **3.4** Implement message serialization
  - Frame format: `[header_len: i32][body_len: i32][header][body]`
  - Serialize/deserialize `Message` using Phase 2 codecs

- [ ] **3.5** Implement `Connection` struct
  - TCP connection wrapper with async read/write
  - `send(message: Message) -> Result<()>`
  - `receive() -> Result<Message>`
  - Outgoing message queue with backpressure

- [ ] **3.6** Implement `ConnectionManager`
  - Pool of connections per target silo
  - `get_connection(silo: &SiloAddress) -> Arc<Connection>`
  - Connection health monitoring and reconnection

- [ ] **3.7** Implement `MessageCenter`
  - Central message dispatcher
  - Routes outgoing messages to correct connection
  - Routes incoming messages to grain activations or response handlers
  - `send(message: Message) -> Result<()>`
  - `register_response_handler(correlation_id, handler)`

### Crate Structure
```
orleans-messaging/
├── src/
│   ├── lib.rs
│   ├── message.rs
│   ├── correlation_id.rs
│   ├── message_factory.rs
│   ├── connection.rs
│   ├── connection_manager.rs
│   └── message_center.rs
```

### Tests
- Unit tests: message serialization roundtrip
- Integration test: send message between two processes
- Property test: concurrent sends don't corrupt messages
- Test: connection reconnection on failure

---

## Phase 4: Cluster Membership

**Objective**: Enable silos to discover each other and track cluster state.

### Tasks

- [ ] **4.1** Define `SiloStatus` enum
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

- [ ] **4.2** Define `MembershipEntry` struct
  ```rust
  struct MembershipEntry {
      silo_address: SiloAddress,
      status: SiloStatus,
      start_time: DateTime<Utc>,
      i_am_alive_time: DateTime<Utc>,
  }
  ```

- [ ] **4.3** Define `IMembershipTable` trait
  ```rust
  trait IMembershipTable: Send + Sync {
      async fn read_all(&self) -> Result<MembershipTableData>;
      async fn insert_row(&self, entry: MembershipEntry) -> Result<bool>;
      async fn update_row(&self, entry: MembershipEntry, etag: &str) -> Result<bool>;
      async fn update_i_am_alive(&self, entry: &MembershipEntry) -> Result<()>;
  }
  ```

- [ ] **4.4** Implement `InMemoryMembershipTable` (for testing/MVP)
  - Shared via file or simple TCP protocol between processes
  - Optimistic concurrency with version numbers

- [ ] **4.5** Implement `MembershipTableManager`
  - Periodically refreshes membership from table
  - Provides `MembershipSnapshot` to other components
  - `get_active_silos() -> Vec<SiloAddress>`

- [ ] **4.6** Implement `MembershipAgent`
  - Join protocol: Joining -> Active
  - Leave protocol: Active -> ShuttingDown -> Dead
  - Periodic heartbeat (I Am Alive updates)

- [ ] **4.7** Implement basic failure detection
  - Track last heartbeat time per silo
  - Mark silo as Dead if heartbeat exceeds threshold

### Crate Structure
```
orleans-clustering/
├── src/
│   ├── lib.rs
│   ├── silo_status.rs
│   ├── membership_entry.rs
│   ├── membership_table.rs
│   ├── in_memory_table.rs
│   ├── membership_manager.rs
│   └── membership_agent.rs
```

### Tests
- Unit test: silo join/leave lifecycle
- Integration test: three silos form cluster
- Test: silo marked dead after missed heartbeats
- Property test: membership version only increases

---

## Phase 5: Grain Directory

**Objective**: Distributed lookup of grain locations using consistent hashing.

### Tasks

- [ ] **5.1** Implement `ConsistentHashRing`
  - Virtual buckets (30 per silo default)
  - `get_primary_silo(hash: u32) -> SiloAddress`
  - `get_my_range(silo: &SiloAddress) -> RingRange`

- [ ] **5.2** Implement `GrainDirectoryPartition`
  - In-memory HashMap of GrainId -> GrainAddress
  - Each silo owns a portion of the hash space
  - `lookup(grain_id: &GrainId) -> Option<GrainAddress>`
  - `register(grain_id: GrainId, address: GrainAddress) -> Result<GrainAddress>`
  - `unregister(grain_id: &GrainId, address: &GrainAddress)`

- [ ] **5.3** Implement `DistributedGrainDirectory`
  - Routes lookups to correct silo based on hash
  - Local calls for owned ranges
  - Remote calls for other ranges
  - `lookup(grain_id: &GrainId) -> Option<GrainAddress>`
  - `register(address: GrainAddress) -> Result<GrainAddress>`

- [ ] **5.4** Implement directory cache
  - LRU cache of GrainId -> GrainAddress
  - Cache invalidation on activation move/death

- [ ] **5.5** Handle membership changes
  - When silo joins: transfer owned entries to new silo
  - When silo leaves: re-register orphaned grains

### Crate Structure
```
orleans-directory/
├── src/
│   ├── lib.rs
│   ├── consistent_hash.rs
│   ├── ring_range.rs
│   ├── partition.rs
│   ├── distributed_directory.rs
│   └── cache.rs
```

### Tests
- Property test: all grains map to exactly one silo
- Property test: hash distribution is balanced
- Integration test: lookup returns correct silo
- Test: directory handoff when silo joins
- Test: directory recovery when silo leaves

---

## Phase 6: Grain Runtime

**Objective**: Host grain activations with turn-based execution.

### Tasks

- [ ] **6.1** Define `IGrain` trait
  ```rust
  #[async_trait]
  trait IGrain: Send + Sync {
      fn grain_id(&self) -> &GrainId;
      async fn on_activate(&mut self) -> Result<()> { Ok(()) }
      async fn on_deactivate(&mut self) -> Result<()> { Ok(()) }
  }
  ```

- [ ] **6.2** Define `IGrainContext` trait
  - Access to grain identity, runtime services
  - `grain_id()`, `activation_id()`, `silo_address()`
  - `grain_factory()` for creating grain references

- [ ] **6.3** Implement `ActivationData`
  - Holds grain instance + context + message queue
  - Activation state machine: Creating -> Activating -> Valid -> Deactivating -> Invalid
  - Turn-based scheduler (one message at a time)

- [ ] **6.4** Implement `Catalog`
  - Registry of active grains on this silo
  - `get_or_create_activation(grain_id) -> ActivationData`
  - Lock striping for concurrent activation creation
  - Activation collection (GC idle grains)

- [ ] **6.5** Implement `Dispatcher`
  - Routes incoming messages to correct activation
  - Handles activation creation if needed
  - Handles rejection if grain can't be activated here

- [ ] **6.6** Implement `GrainFactory`
  - Creates grain references (proxies)
  - `get_grain<T>(key) -> GrainReference<T>`

- [ ] **6.7** Implement `GrainReference<T>` (proxy)
  - Holds GrainId + interface type
  - Method calls serialize args and send message
  - Awaits response and deserializes result

### Crate Structure
```
orleans-runtime/
├── src/
│   ├── lib.rs
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
- Unit test: activation state transitions
- Unit test: turn-based execution (no concurrent calls)
- Integration test: grain method invocation roundtrip
- Test: idle grain deactivation
- Property test: single activation per grain across cluster

---

## Phase 7: Code Generation

**Objective**: Proc macros to generate grain interfaces and invokers.

### Tasks

- [ ] **7.1** Implement `#[grain_interface]` attribute macro
  - Applied to trait definitions
  - Generates `GrainInterfaceType` constant
  - Generates method ID constants

- [ ] **7.2** Implement `#[grain]` attribute macro
  - Applied to struct implementations
  - Generates `GrainType` registration
  - Generates invoker (dispatches method calls)

- [ ] **7.3** Generate proxy implementation
  - For each grain interface method:
    - Serialize arguments
    - Create request message
    - Send via message center
    - Await response
    - Deserialize result

- [ ] **7.4** Generate invoker implementation
  - For each grain interface method:
    - Deserialize arguments from message
    - Call grain method
    - Serialize result
    - Create response message

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

## Phase 8: Silo Host

**Objective**: Assemble all components into a runnable silo process.

### Tasks

- [ ] **8.1** Implement `SiloBuilder`
  - Configure silo address, cluster ID
  - Register grain types
  - Configure membership table

- [ ] **8.2** Implement `Silo`
  - Lifecycle: Start -> Running -> Stopping -> Stopped
  - Starts all background services
  - Graceful shutdown

- [ ] **8.3** Implement startup sequence
  1. Initialize message center and start listening
  2. Connect to membership table
  3. Join cluster (MembershipAgent)
  4. Start grain directory
  5. Start dispatcher
  6. Mark silo as Active

- [ ] **8.4** Implement shutdown sequence
  1. Mark silo as ShuttingDown
  2. Stop accepting new activations
  3. Deactivate all grains
  4. Mark silo as Dead
  5. Close connections

- [ ] **8.5** Implement `ClusterClient`
  - External client that connects to cluster
  - Discovers silos via membership table
  - Routes requests through gateway silo

### Crate Structure
```
orleans-host/
├── src/
│   ├── lib.rs
│   ├── silo_builder.rs
│   ├── silo.rs
│   ├── lifecycle.rs
│   └── cluster_client.rs
```

### Tests
- Integration test: silo starts and joins cluster
- Integration test: silo graceful shutdown
- Integration test: client connects and calls grain

---

## Phase 9: Integration Tests

**Objective**: Verify the complete system with three-silo cluster.

### Test Scenarios

- [ ] **9.1** Basic grain invocation
  - Start 3 silos
  - Create grain on silo1
  - Call grain from silo2, verify response
  - Call grain from silo3, verify response

- [ ] **9.2** Grain location transparency
  - Call grain without knowing which silo hosts it
  - Verify request is routed correctly

- [ ] **9.3** Single activation guarantee
  - Simultaneously request same grain from all silos
  - Verify only one activation exists

- [ ] **9.4** Silo failure handling
  - Start 3 silos, create grain
  - Kill silo hosting grain
  - Call grain, verify it re-activates on another silo

- [ ] **9.5** Grain state isolation
  - Create grain, set state
  - Call from another silo, verify state persists
  - Verify no cross-grain state leakage

- [ ] **9.6** Concurrent grain calls
  - Many simultaneous calls to same grain
  - Verify turn-based execution (no races)

### Property-Based Tests

- [ ] **9.7** Grain identity properties
  - `grain_id(grain_ref) == expected_grain_id`
  - `hash(grain_id1) != hash(grain_id2)` for different grains (usually)

- [ ] **9.8** Directory consistency
  - After any sequence of register/unregister operations
  - `lookup(grain_id)` returns registered address or None

- [ ] **9.9** Message delivery
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

## Success Criteria

The MVP is complete when:

1. **Three silos form a cluster** - All silos appear in membership table with Active status

2. **Grain creation works** - `grain_factory.get_grain::<IHelloGrain>("key1")` returns a valid reference

3. **Cross-silo calls work** - Calling a grain from a different silo than where it's activated succeeds

4. **Single activation guarantee** - Only one activation exists per grain ID across the cluster

5. **Turn-based execution** - Grain methods execute sequentially, no concurrent access

6. **All tests pass** - Unit, integration, and property tests verify correct behavior

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
