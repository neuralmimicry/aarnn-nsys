# aarnn-nsys — Ultra-low-latency MPMC message bus

aarnn-nsys is a tiny, production-grade message bus designed for single-host ultra-low latency pub/sub.
It delivers multi-producer, multi-subscriber fan-out using a lock-free ring over a shared memory region.

- 0 allocations on the hot path
- Cacheline-friendly indices only cross cores
- Linux shared-memory backend (default), plus a `no_std`/bare-metal in-memory backend
- CLI demo and examples included
- Concatenation (relay) between two bus instances supported

## Features at a glance
- MPMC ring using a global `claim_seq` and per-slot commit sequence (Disruptor-style)
- Backpressure from the slowest subscriber
- Fixed-size per-slot payload regions for MP predictability (`slot_bytes = slab_bytes / desc_capacity`)
- `publish` (blocking) and `try_publish` (non-blocking)
- `try_recv` (non-blocking) and `recv_blocking` (std-only)
- Relay utilities (`concat` CLI) to forward traffic across buses

## Building
- Default (Linux + shared memory):
```
cargo build
```
- no_std/bare-metal library (no CLI):
```
cargo build --no-default-features --features bare
```

### Feature flags
- `std` (default): enables Rust standard library; required for the Linux shared memory backend and the CLI tools.
- `linux-shm` (default): builds the POSIX shared memory backend on Linux. Implies `std` and `libc`.
- `bare`: builds the core bus for `no_std` environments. No CLI, no OS APIs. Use `BusHandle::from_slice` to place the bus into an in-memory region you provide.

## Using aarnn-nsys in bare-metal (no_std)
This crate exposes a no_std-safe API to instantiate the bus entirely in caller-provided memory.

Key points:
- The bus memory layout is deterministic and can be sized at compile time using const helpers.
- You provide a single contiguous byte slice that will hold:
  - Header + slots ring (metadata), aligned to cacheline
  - Payload slab: `desc_capacity * slot_bytes` bytes
- No allocator required; no syscalls.

### Sizing helpers (const)
Use these in const contexts to reserve static memory for the bus:
- `bus::header_layout_size(desc_capacity: usize) -> usize`
- `bus::min_buffer_size(desc_capacity: usize, slot_bytes: usize) -> usize`

Example sizing:
```
const DESC_CAPACITY: usize = 1024;  // must be power of two
const SLOT_BYTES: usize = 256;      // payload bytes per message
const BUF_LEN: usize = aarnn_nsys::bus::min_buffer_size(DESC_CAPACITY, SLOT_BYTES);
#[repr(align(64))] struct Aligned<const N: usize>([u8; N]);
static mut BUS_MEM: Aligned<BUF_LEN> = Aligned([0u8; BUF_LEN]);
```

### Minimal no_std usage example
The example below demonstrates initializing the bus from a static buffer, creating a producer and a subscriber, publishing once, and receiving. It follows Rust 2024 rules around `static mut` by using raw pointers and `from_raw_parts_mut` rather than taking references to the `static mut`.

```
#![no_std]
use aarnn_nsys::bus;

const DESC_CAPACITY: usize = 8;   // power of two
const SLOT_BYTES: usize = 64;     // per-slot payload size
const BUF_LEN: usize = bus::min_buffer_size(DESC_CAPACITY, SLOT_BYTES);

static mut BUS_MEM: [u8; BUF_LEN] = [0u8; BUF_LEN];

fn demo() {
    // SAFETY: We construct a &mut [u8] view from a raw pointer to the static buffer.
    let bus = unsafe {
        let ptr = core::ptr::addr_of_mut!(BUS_MEM) as *mut u8;
        let buf: &mut [u8] = core::slice::from_raw_parts_mut(ptr, BUF_LEN);
        bus::BusHandle::from_slice(buf, DESC_CAPACITY).expect("bus init")
    };

    let sub = bus.subscribe().expect("subscribe");
    let prod = bus.producer();

    let msg = b"hello from bare";
    prod.publish(msg).expect("publish");

    let mut scratch = [0u8; SLOT_BYTES];
    if let Ok(Some(n)) = sub.try_recv(&mut scratch) {
        let _ = &scratch[..n];
        // ... use payload bytes ...
    }
}
```

### Choosing sizes
- `desc_capacity` (ring size): Must be a power of two. Larger rings reduce backpressure at the cost of more memory.
- `slot_bytes` (per-message payload): Fixed per slot. Every publish must fit within this size. Increase if you need larger messages.
- Total memory: `min_buffer_size(desc_capacity, slot_bytes)` bytes. This includes metadata overhead and the payload slab.

### Safety notes (Rust 2024)
- Avoid taking `&mut` or `&` references to `static mut` globals. Construct slices from raw pointers instead, as shown above.
- The bus uses atomics and performs `unsafe` pointer arithmetic internally; the public API enforces alignment and size checks when you call `from_slice`.
- In `no_std`, `publish` will busy-wait when back-pressured; there is no scheduler to yield to.

### End-to-end bare-metal example on Raspberry Pi
See `raspi-bare-metal/` in this repo for a complete bootable image using the PL011 UART and an in-memory bus. The README there includes build and QEMU instructions as well as expected UART output.

## CLI usage (Linux backend)
```
aarnn-nsys create <name> <desc_capacity_pow2> <slab_bytes>
aarnn-nsys prod   <name> <desc_capacity_pow2> <slab_bytes> [msg_size]
aarnn-nsys sub    <name> <desc_capacity_pow2> <slab_bytes>
aarnn-nsys concat <src_name> <src_desc_cap> <src_slab> <dst_name> <dst_desc_cap> <dst_slab> [seconds]
```

Example:
```
# Create a region: 16384 slots, 64 MiB slab (=> slot_bytes = 4096)
./target/debug/aarnn-nsys create /demo 16384 67108864

# Start a subscriber (10s)
./target/debug/aarnn-nsys sub /demo 16384 67108864

# In another terminal, start 1+ producers (10s) with 1024B messages
./target/debug/aarnn-nsys prod /demo 16384 67108864 1024
```

### Concatenate two buses
Forward all messages from `/src` to `/dst` for 10 seconds:
```
./target/debug/aarnn-nsys concat /src 16384 67108864 /dst 16384 67108864 10
```
Notes:
- Messages larger than `dst.slot_bytes()` are dropped and reported.
- For continuous relaying, supervise the process or wrap in a service.

## End-to-end tutorial (start → finish)
This walkthrough shows a realistic pipeline across processes using the CLI tools. You will:
- Create two buses (`/src` and `/dst`).
- Start 2 producers writing to `/src`.
- Run a relay that forwards from `/src` → `/dst`.
- Run a subscriber on `/dst` that reports per-message throughput.

Prereqs: build the project first with `cargo build`.

1) Create both buses (16384 slots, 64 MiB each; `slot_bytes = 4096`):
```
./target/debug/aarnn-nsys create /src 16384 67108864
./target/debug/aarnn-nsys create /dst 16384 67108864
```

2) Start a subscriber on the destination in Terminal A (runs ~10s and prints rate):
```
./target/debug/aarnn-nsys sub /dst 16384 67108864
```

3) In Terminal B, start the relay for 10s:
```
./target/debug/aarnn-nsys concat /src 16384 67108864 /dst 16384 67108864 10
```

4) In Terminal C and D, start two producers with distinct message sizes (both ≤ 4096B):
```
./target/debug/aarnn-nsys prod /src 16384 67108864 512
./target/debug/aarnn-nsys prod /src 16384 67108864 1024
```

Observe:
- Producers report Mmsg/s and Gbps rates.
- The relay prints the count of forwarded messages.
- The subscriber on `/dst` prints receive rates and total messages.

Alternative: run a single-process tutorial with the example program (spawns producers, relay, and subscriber all in one):
```
cargo run --example pipeline -- /src 16384 67108864 /dst 16384 67108864 2 1024 5
```

no_std/bare-metal tutorial (single-process, in-memory region — build the library in bare mode):
```
cargo run --no-default-features --features bare --example bare_in_memory -- 1024 131072 2 256
```

Troubleshooting:
- If you see "message too large", reduce `msg_size` or increase `slab_bytes/desc_capacity` so `slot_bytes` grows.
- Ensure the bus names are unique (POSIX shm namespace). Prefix with `/` and consider incorporating `$PID`.
- On busy systems, variance improves with CPU pinning and huge pages; see Tuning.

## Library API (overview)
```
use aarnn_nsys::bus::{BusHandle, BusError};

// Linux shared memory
let bus = BusHandle::create("/demo", 16384, 64<<20)?;
let prod = bus.producer();
let sub  = bus.subscribe()?;

prod.publish(&[1,2,3,4])?;
let mut buf = vec![0u8; bus.slot_bytes()];
if let Some(n) = sub.try_recv(&mut buf)? { /* ... */ }

// no_std/bare-metal (library only):
// let mut region: [u8; N] = [0; N];
// let bus = BusHandle::from_slice(&mut region, 1024)?;
```

See `src/bus.rs` for detailed in-code documentation, memory ordering and safety notes.

## Examples
- `examples/pipeline.rs` — full end-to-end pipeline: N producers → source bus → relay → destination bus → subscriber with per-source counts.
- `examples/multi_producers.rs` — two different producers publish distinct payloads; a subscriber validates counts per source.
- `examples/subscriber.rs` — standalone subscriber example.

Run examples:
```
cargo run --example pipeline -- /src 16384 67108864 /dst 16384 67108864 2 1024 5
cargo run --example multi_producers -- /demo 8192 33554432 512
cargo run --example subscriber -- /demo 8192 33554432
```

## Tests
There are two complementary test configurations:

1) Default (Linux shared memory backend; `std + linux-shm`):
```
cargo test -p aarnn-nsys
```
Covers:
- MPMC sequencing and ordering
- Backpressure behavior
- End-to-end create/open shared-memory region and publish/recv across two handles

2) Core/no-shm (`from_slice` path; `std + bare`, with `linux-shm` disabled):
```
cargo test -p aarnn-nsys --no-default-features --features "std bare"
```
Covers:
- In-memory ring (no OS backend) using caller-provided slices
- Fan-out correctness with multiple subscribers
- Oversize message errors followed by successful publish
- Relay (`relay_once`) between two in-memory buses

Notes:
- Tests rely on `std` for threading/timing even in the "core/no-shm" mode. The underlying logic is the same as used in `no_std` environments.
- Keep `raspi-bare-metal` free of Rust test harnesses; use its QEMU smoke test (see that crate’s README) instead.

## Design & Safety
- Producers write payload bytes and descriptor length, then `Release`-publish the slot sequence. Subscribers `Acquire`-check before copying.
- Only indices cross cores; payload stays in the shared mapping.
- Crash-safety is best-effort. A torn publish leaves an uncommitted slot, ignored by readers.

## Tuning
To minimize latency variance on Linux: consider `mlockall`, `MADV_HUGEPAGE`, prefaulting the region, and CPU pinning (isolated cores). See comments in `src/bus.rs`.

## Limitations
- Fixed per-slot payload size in this version; choose `slab_bytes` accordingly.
- No dynamic subscriber removal/reuse.
- No zero-copy receive API (planned).
