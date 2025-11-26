# iceberg-rust and DataFusion Integration

## Overview

This document explains the relationship between iceberg-rust and Apache DataFusion, the architectural tensions in the current implementation, and the community discussions around improving the integration. Understanding this context is valuable for anyone working with iceberg-rust or considering contributions to the ecosystem.

## What is Apache DataFusion?

[Apache DataFusion](https://datafusion.apache.org/) is a fast, extensible query engine written in Rust that serves as the foundation for data-centric systems.

### Core Capabilities

**Execution Engine:**
- Vectorized, multithreaded, streaming execution
- Uses Apache Arrow in-memory columnar format
- Async I/O from cloud storage (AWS S3, Azure, GCP)

**Query Processing:**
- SQL and DataFrame APIs
- Sophisticated query optimizer with:
  - Expression coercion and simplification
  - Projection and filter pushdown
  - Sort and distribution-aware optimizations
  - Automatic join reordering

**Data Format Support:**
- Native Parquet, CSV, JSON, Avro support
- Extensible via `TableProvider` trait for custom formats
- Highly optimized Parquet reader with:
  - Row group filtering
  - Page-level filtering
  - Parallel decompression
  - Adaptive I/O strategies

### Ecosystem Position

DataFusion is used as the query foundation by **40+ active projects**, including:
- **Distributed databases**: Ballista, CnosDB
- **Analytics engines**: InfluxDB IOx
- **Data lakehouse tools**: Delta Lake (delta-rs), Iceberg (iceberg-rust)
- **Specialized systems**: Various domain-specific query engines

DataFusion eliminates the need to rebuild query engine fundamentals, allowing projects to focus on domain-specific features rather than reimplementing expression evaluation, optimization passes, execution planning, and file format handling.

## iceberg-datafusion Integration

### What It Provides

The `iceberg-datafusion` crate (part of the iceberg-rust workspace) connects Apache Iceberg tables to DataFusion's query engine.

**Location in iceberg-rust workspace:**
```
crates/
├── iceberg/                 # Core Iceberg implementation
├── catalog/
│   ├── rest/               # REST catalog
│   ├── glue/               # AWS Glue catalog
│   └── hms/                # Hive Metastore
└── integrations/
    └── datafusion/         # DataFusion integration
```

**Capabilities:**
- Register Iceberg tables as DataFusion data sources
- Execute SQL queries over Iceberg tables
- Leverage predicate and projection pushdown
- Access DataFusion's optimized execution engine

### Implementation Timeline

- **Issue #242** (opened early in the project) - Initial discussion of DataFusion integration
- **Issue #357** - Tracking issue for DataFusion integration features
- **v0.3.0** (2024) - Working read path with scan API
- **v0.7.0** (October 2024) - Current release with DataFusion write support via `IcebergWriteExec` and `IcebergCommitExec`

## Architectural Tensions

### The Core Problem

The current iceberg-rust architecture creates **duplication and friction** with DataFusion. This is a key theme in [Issue #1797 - "Reduce the need for iceberg-rust forks"](https://github.com/apache/iceberg-rust/issues/1797).

### Specific Issues Identified

**1. Duplication of DataFusion Functionality**

iceberg-rust reimplements capabilities that DataFusion already provides:
- Expression evaluation and filtering
- File-level pruning logic
- Parquet scanning and reading
- Predicate handling

This duplication means:
- **Maintenance burden** - two implementations to maintain
- **Performance gaps** - custom implementations lag behind DataFusion's optimizations
- **Feature lag** - DataFusion improvements don't automatically benefit Iceberg users

**2. Not Leveraging DataFusion's Advanced Features**

The integration doesn't fully utilize DataFusion's capabilities:
- Doesn't use `ParquetExec` (DataFusion's optimized Parquet execution operator)
- Misses advanced Parquet optimizations (page-level filtering, adaptive I/O)
- Can't benefit from DataFusion's ongoing performance improvements
- Limited use of DataFusion's expression optimization passes

**3. Read Path Performance Issues**

Community members have identified specific performance problems:
- **Deadlocks** in the read path (PR #1486 mentioned as unreviewed)
- **Single-threaded limitations** due to output partitioning constraints
- **Suboptimal I/O patterns** compared to native DataFusion table providers

**4. Design Philosophy Mismatch**

The architecture feels **Java-influenced** rather than idiomatic Rust:
- Tight coupling between serialization, handling, and validation planes
- Abstractions that don't align with Rust ecosystem patterns
- Integration friction with Rust-native tools

**5. Object Storage Abstraction Conflict**

iceberg-rust uses **OpenDAL** for file I/O, while much of the Rust data ecosystem uses the **`object_store` crate**:
- Projects already invested in `object_store` face integration challenges
- Can't easily share credential providers or configuration
- Forces dependency duplication or complex bridging code

This is explicitly mentioned in Issue #1797 as a "main concern" for projects like Spice AI.

## Community Fragmentation: The Fork Ecosystem

### Organizations Maintaining Forks

Multiple organizations have forked or are considering forking iceberg-rust:

**Known forks:**
- **Bauplanlabs** - Bauplan data platform
- **RisingWave** - Streaming database
- **Spice AI** - Data acceleration platform
- **Others** - Various projects mentioned in community discussions

### Why Organizations Fork

From Issue #1797 discussions, common reasons include:

**Performance Requirements:**
- Need better read path performance
- Require tighter DataFusion integration
- Can't wait for upstream performance fixes

**Integration Needs:**
- Already use `object_store` and face OpenDAL conflicts
- Need specific DataFusion features not exposed
- Require custom table provider implementations

**Velocity:**
- Upstream PR review bottleneck (see below)
- Can't wait months for critical fixes
- Need to iterate quickly on features

**Architectural Preferences:**
- Want kernel-style architecture (see below)
- Prefer different abstraction layers
- Need different trade-offs than core project

### Impact of Fragmentation

**Negative effects:**
- **Split development effort** - improvements don't flow between forks and upstream
- **Duplicate work** - multiple implementations of the same fixes
- **Reduced contributor pool** - community divided across multiple repositories
- **Integration challenges** - tools built on different forks can't interoperate
- **Maintenance burden** - each fork must track upstream changes and rebase

**Positive aspects:**
- **Experimentation space** - forks can try different architectural approaches
- **Faster iteration** - organizations can move at their own pace
- **Proof of concepts** - successful fork features can inform upstream design

## Maintenance Bottleneck

### Current State

From Issue #1797 and related discussions:

**Limited reviewer capacity:**
- Only **2 primary maintainers** (@liurenjie1024 and @Xuanwo) handling the bulk of PR reviews
- PR backlog with important fixes sitting unreviewed
- Difficulty keeping pace with DataFusion and Arrow-rs rapid release cycles

**Examples of stuck work:**
- PR #1486 (deadlock fixes) mentioned as unreviewed
- Long review cycles for new features
- Features like SigV4 support taking months

### Contributing Factors

**Technical complexity:**
- Large API surface across multiple crates
- Need deep knowledge of both Iceberg spec and Rust ecosystem
- Integration points with rapidly-evolving dependencies (DataFusion, Arrow)

**Review requirements:**
- Correctness crucial (data loss/corruption risks)
- Performance implications need careful evaluation
- Backward compatibility considerations
- Multiple stakeholders with different needs

## Proposed Solution: The "Kernel" Approach

### Inspiration: delta-kernel-rs

The Delta Lake project developed `delta-kernel-rs`, a minimal kernel that handles Delta Lake specifics while delegating execution to consumers like DataFusion.

This architecture has been successful and is proposed as a model for iceberg-rust.

### Current Architecture (Problematic)

```
┌─────────────────────────────────────┐
│          iceberg-rust               │
├─────────────────────────────────────┤
│ Catalog APIs (Glue, REST, HMS)      │
│ Table metadata handling             │
│ Snapshot/manifest management        │
│ File listing and pruning            │
│ Expression evaluation (custom)      │
│ Parquet scanning (custom)           │
│ Schema evolution                    │
├─────────────────────────────────────┤
│      iceberg-datafusion             │
│   (awkward integration layer)       │
└─────────────────────────────────────┘
         ↓
┌─────────────────────────────────────┐
│         DataFusion                  │
│  (underutilized capabilities)       │
└─────────────────────────────────────┘
```

**Problems:**
- Duplication between iceberg-rust and DataFusion
- Integration layer adds overhead
- Updates to DataFusion don't automatically benefit users
- Large API surface to maintain

### Proposed "Kernel" Architecture

```
┌─────────────────────────────────────┐
│       iceberg-kernel (minimal)      │
├─────────────────────────────────────┤
│ Iceberg spec implementations:       │
│  • JSON/Avro metadata parsing       │
│  • Snapshot management              │
│  • Manifest file handling           │
│  • File listing                     │
│  • Schema definitions               │
│  • Partition spec interpretation    │
└─────────────────────────────────────┘
         ↓ (exposes file lists + metadata)
┌─────────────────────────────────────┐
│         DataFusion                  │
├─────────────────────────────────────┤
│ • Query planning & optimization     │
│ • Expression evaluation             │
│ • Parquet scanning (ParquetExec)    │
│ • Predicate pushdown                │
│ • Execution engine                  │
│ • I/O and caching                   │
└─────────────────────────────────────┘
```

**Benefits:**
- **Reduced duplication** - single code path for execution
- **Better performance** - leverage DataFusion's battle-tested optimizations automatically
- **Simpler maintenance** - smaller API surface, clearer responsibilities
- **Natural ecosystem fit** - aligns with how most Rust data tools work
- **Automatic improvements** - DataFusion enhancements benefit Iceberg users for free

### Kernel Responsibilities

The minimal kernel would handle **only Iceberg-specific concerns**:

**Metadata layer:**
- Parse `metadata.json` files
- Read and interpret Avro manifest files
- Track snapshots and snapshot history
- Manage schema evolution

**File discovery:**
- List data files for a snapshot
- Apply partition pruning (Iceberg-specific logic)
- Provide file-level statistics
- Handle delete files (position and equality deletes)

**NOT in kernel:**
- Parquet reading (delegate to DataFusion)
- Expression evaluation (delegate to DataFusion)
- Query optimization (delegate to DataFusion)
- Data scanning and filtering (delegate to DataFusion)

### DataFusion Responsibilities

DataFusion handles **execution and optimization**:

**Query layer:**
- Parse SQL or construct DataFrame plans
- Optimize logical plans
- Generate physical execution plans

**Execution layer:**
- Scan Parquet files (using `ParquetExec`)
- Evaluate predicates and projections
- Handle row group and page filtering
- Manage parallel execution
- Coordinate I/O and caching

**Integration point:**
- Kernel provides list of files + metadata
- DataFusion's `TableProvider` implementation consumes this
- DataFusion handles all actual data reading

## Implications for Different Use Cases

### For Query Engines (Most Projects)

**Current state:**
- Must work around iceberg-rust's custom execution path
- Can't fully leverage DataFusion optimizations
- Integration requires complex glue code

**With kernel approach:**
- Natural DataFusion integration
- Full access to DataFusion features
- Simpler implementation

**Verdict:** Kernel approach is significantly better

### For Inspection Tools (like bergr)

**Current state:**
- Can access metadata and manifests via spec types
- Has high-level catalog APIs
- Can inspect tables without query execution

**With kernel approach:**
- Same capabilities preserved in kernel
- Simpler, more focused API
- Less dependency weight if not using DataFusion

**Verdict:** Kernel approach is neutral to slightly better

### For Writers and Metadata Tools

**Current state:**
- Transaction APIs for table modifications
- Metadata manipulation capabilities
- Writer implementations

**With kernel approach:**
- Same capabilities in kernel
- Clearer separation from read path
- Potentially simpler to maintain

**Verdict:** Kernel approach is neutral to slightly better

## Current State of Discussion

### Where Things Stand

As of the latest information (early 2025):

**Issue #1797 status:**
- Open discussion with 26+ comments
- Active engagement from multiple organizations
- No clear consensus or implementation plan yet

**Maintainer response:**
- Acknowledged reviewer capacity challenges
- Open to growing maintainer base
- Need experienced contributors to step up

**Community sentiment:**
- Strong interest in kernel approach
- Concerns about migration path
- Questions about backward compatibility

### Open Questions

**Architecture:**
- Exact boundary between kernel and execution layer
- How to handle catalog implementations (in kernel or separate?)
- Migration path for existing users

**Implementation:**
- Who will drive the refactoring?
- Timeline and phases
- Backward compatibility strategy

**Ecosystem:**
- Will forks rejoin if kernel approach is adopted?
- How to coordinate across projects?
- Impact on downstream dependencies

## Recommendations for bergr

### Current Position

bergr is well-positioned relative to these debates:

**What bergr uses:**
- Core metadata types (`iceberg::spec`)
- Catalog APIs (`Catalog` trait)
- High-level table inspection
- AWS integration (Glue catalog, S3 FileIO)

**What bergr doesn't use:**
- DataFusion integration
- Custom Parquet scanning
- Query execution

**Verdict:** bergr is largely **insulated from the architectural tensions** because it's an inspection tool, not a query engine.

### If Adding Query Capabilities

If bergr were to add query functionality (e.g., `bergr query "SELECT ..."`):

**Recommendation:** Use DataFusion directly

**Approach:**
1. Use existing `iceberg-datafusion` crate
2. Register Iceberg tables as DataFusion data sources
3. Execute queries via DataFusion's SQL engine
4. Leverage optimizations automatically

**Example architecture:**
```rust
use datafusion::prelude::*;
use iceberg_datafusion::IcebergTableProvider;

// Register Iceberg table with DataFusion
let ctx = SessionContext::new();
let table = iceberg_catalog.load_table(&namespace_id, &table_name).await?;
let provider = IcebergTableProvider::try_new(table).await?;
ctx.register_table("my_table", Arc::new(provider))?;

// Execute SQL
let df = ctx.sql("SELECT * FROM my_table WHERE x > 10").await?;
df.show().await?;
```

### Monitoring the Situation

**Watch for:**
- Resolution of Issue #1797 discussions
- Adoption of kernel architecture (or not)
- iceberg-rust version 0.8.0+ releases with architectural changes
- Fork consolidation or continued fragmentation

**Participation opportunities:**
- Comment on Issue #1797 from bergr's perspective as an inspection tool
- Test proposed changes against bergr's use cases
- Contribute feedback on kernel API design

## Additional Context

### PyIceberg and iceberg-rust

[PyIceberg](https://py.iceberg.apache.org/) is the official Python implementation and has better maturity than iceberg-rust. However:

**PyIceberg integration efforts:**
- `pyiceberg-core` provides Python bindings to iceberg-rust's implementations
- Leverages Rust's performance for core operations
- Exposes DataFusion functionality to Python users

This cross-language integration adds complexity but demonstrates the value of having a well-designed Rust core.

### Comparison with Delta Lake

**delta-rs and delta-kernel-rs** have successfully navigated similar architectural questions:

**Their approach:**
- `delta-kernel-rs` - minimal kernel handling Delta Lake specifics
- Tight DataFusion integration in `delta-rs` consumer library
- Clear separation of concerns

**Lessons for iceberg-rust:**
- Kernel approach is proven in production
- Can maintain backward compatibility during transition
- Ecosystem benefits from clear architecture

### OpenDAL vs object_store

This debate is mentioned frequently in Issue #1797:

**OpenDAL:**
- More feature-rich
- Used by iceberg-rust currently
- Rust-native

**object_store:**
- Standard in DataFusion ecosystem
- Used by most Rust data tools
- Simpler, focused API

**Impact:**
- Projects using `object_store` face integration friction with iceberg-rust
- Mentioned as a "main concern" for forks
- Likely needs resolution as part of architectural discussions

## References

### Primary Sources

**Architecture discussions:**
- [Issue #1797 - Reduce the need for iceberg-rust forks](https://github.com/apache/iceberg-rust/issues/1797)
- [Issue #242 - Integrate with datafusion](https://github.com/apache/iceberg-rust/issues/242)
- [Issue #357 - Tracking Issue: Integration with Datafusion](https://github.com/apache/iceberg-rust/issues/357)

**DataFusion:**
- [Apache DataFusion documentation](https://datafusion.apache.org/)
- [DataFusion GitHub](https://github.com/apache/datafusion)
- [iceberg-datafusion README](https://github.com/apache/iceberg-rust/blob/main/crates/integrations/datafusion/README.md)

**Related projects:**
- [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) - kernel architecture inspiration
- [OpenDAL](https://opendal.apache.org/) - current file I/O abstraction
- [object_store crate](https://docs.rs/object_store/) - alternative abstraction

### Community Discussions

- [Apache Iceberg mailing list discussion](http://www.mail-archive.com/dev@iceberg.apache.org/msg11611.html) - growing the community
- [Discussion #468 - Questions around Iceberg-rust](https://github.com/apache/iceberg-rust/discussions/468)

## Conclusion

The iceberg-rust ecosystem is at a crossroads. The current architecture creates duplication with DataFusion, leading to performance gaps, maintenance burden, and community fragmentation through forks. The proposed kernel approach—inspired by delta-kernel-rs—offers a path forward that better aligns with the Rust data ecosystem.

For bergr specifically, these architectural debates have limited direct impact since it's an inspection tool rather than a query engine. However, understanding this context helps explain upstream development velocity and informs decisions about future feature additions.

The outcome of Issue #1797 and related discussions will shape the future of Iceberg in Rust. Projects like bergr should monitor these developments and contribute feedback from the perspective of metadata inspection use cases.
