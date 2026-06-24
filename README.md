# aarnn-nsys — Ultra-low-latency MPMC message bus

## Sponsor NeuralMimicry

This crate is a production-grade, zero-allocation publish/subscribe message bus designed for ultra-low-latency neuromorphic and real-time systems work, with support for bare-metal `no_std` environments. NeuralMimicry is an independent open-source initiative and we rely on community support to sustain this work.

**[☕ Support us on Crowdfunder](https://www.crowdfunder.co.uk/p/qr/aWggxwPW?utm_campaign=sharemodal&utm_medium=referral&utm_source=shortlink)**

---

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



## usb-sim bridge notes (PTY endpoints)
The `usbsim_bridge` binary can attach to a QEMU PTY (`-serial pty`) or a QEMU `pipe:` chardev and translate framed usb-sim traffic into a local `aarnn-nsys` bus.

Quick start:
```
cargo run --bin usbsim_bridge -- \
  --name /bm2lnx --desc 16384 --slab $((64<<20)) \
  --endpoint pty:/dev/pts/N --max-frame 4096 --ttl 8 --self-test
```

Tips and requirements:
- Frame size must match the bare-metal build. If BM uses `usbsim_frame_2048`, run the bridge with `--max-frame 2048`.
- On PTY endpoints we set raw mode and non-blocking I/O to avoid stalls and CR/LF transformations.
- EOF/pipe closure is handled gracefully: the bridge exits cleanly when the peer closes, instead of panicking.
- In `--self-test` mode, the bridge periodically re-sends a small probe until it sees an ACK, which helps absorb early attach timing.
- When used with the provided scripts, the Linux side prints `[MBUS-LNX] PASS` on success, or the BM side prints `[MBUS-BM] PASS`.

Troubleshooting:
- If you get `slot_bytes < max_frame`, increase `--slab` or reduce `--max-frame` (slot_bytes = slab/desc).
- If the script times out on the first run, try re-running once; PTY attach timing can vary across hosts.
- The scripts stop QEMU before stopping `socat` to avoid a benign warning on PTY teardown (I/O error while restoring term settings). This warning is filtered from summaries and does not indicate a failure.



## L2 bridge quickstart (Linux↔Linux over raw Ethernet)
The `l2bridge` binary forwards between a local `aarnn-nsys` bus and an L2 (Ethernet) link using AF_PACKET raw sockets. It supports simple fragmentation and a minimal header over a custom EtherType (default `0xCAFE`).

Quick start using the helper script (creates a veth pair and starts two bridge processes):
```
sudo ./scripts/l2_linux2linux.sh
```
Expected: `[MBUS-L2-LNX] PASS` within the timeout, and the script tears down all processes and the veth pair.

Manual run (one side):
```
cargo run -p aarnn-nsys --bin l2bridge -- \
  --iface vethA \
  --name /l2demoA --desc 16384 --slab $((64<<20)) \
  --dst-mac 02:00:00:00:00:02 --ethertype 0xCAFE \
  --max-frame 4096 --ttl 8 --self-test
```
Notes:
- Requires CAP_NET_RAW (root) to open AF_PACKET sockets; run with `sudo`.
- `slot_bytes` must be `slab/desc` and must be >= `--max-frame`.
- EtherType must match on both sides; default `0xCAFE` is fine for local tests.
- MAC handling:
  - When the helper script auto-creates a veth pair, it programs deterministic locally administered MACs on `vethA` and `vethB` and uses those as the peers’ `--src-mac`/`--dst-mac` values so unicast frames are accepted.
  - When using existing interfaces via `IFACE_A`/`IFACE_B`, the script does not change their MACs; instead it enables promiscuous mode on both to ensure unicast frames with the peer MAC are received.
- The script starts children in new sessions and tears down process groups on exit, preventing orphans.
- On PASS, one side prints `[MBUS-L2-LNX] PASS` after receiving an ACK (channel 1) from the peer.

Troubleshooting:
- If you see `slot_bytes < max_frame`, reduce `--max-frame` or increase `--slab`/decrease `--desc`.
- If the script times out the first time, re-run once — interface bring-up timing can vary across hosts.
- To use existing interfaces instead of a veth pair, provide `IFACE_A` and `IFACE_B` in the environment and set explicit `--src-mac`/`--dst-mac` values.



### L2 trace mode and troubleshooting
When running the Linux↔Linux L2 bridge you can enable a lightweight trace to aid debugging:

```
cargo run -p aarnn-nsys --bin l2bridge -- \
  --iface vethA --name /l2demoA --desc 16384 --slab $((64<<20)) \
  --dst-mac 02:00:00:00:00:02 --ethertype 0xCAFE \
  --max-frame 4096 --ttl 8 --self-test --trace
```

What you’ll see (rate-limited to ~1 line/sec):
- wrong_ethertype — frames received on the interface but with a different EtherType; usually normal background traffic.
- short_frame — frames that were too short for the header or claimed payload length would overflow the capture buffer.
- crc_fail — frames whose header+payload CRC did not match. On veth this should be 0; if not, another process may be injecting noise.
- reasm_reset — reassembly state was reset (overflow or wrap). Should be 0 in the demo; rising counts indicate fragment loss.

Tips:
- For the helper script, set TRACE=1: `sudo TRACE=1 ./scripts/l2_linux2linux.sh`.
- Ensure `slot_bytes = slab/desc` is >= `--max-frame`.
- With the helper script’s auto veth, we program locally administered MACs and also enable promiscuous mode on both ends to make delivery deterministic. External interfaces use promisc without changing their MACs.


### Updates in v0.1.7 (L2 bridge)
- Self-test now sends an immediate on-wire probe over L2 (in addition to publishing to the local bus) to guarantee initial traffic.
- Zero-length ACKs are padded to the Ethernet minimum payload so frames are never dropped for being too short.
- Trace mode (`--trace`) now includes transmit counters and first-event hints:
  - `tx_frag`, `tx_bytes`, and one-time messages `first TX` / `first RX` to confirm link activity.

Example with trace (one side):
```
cargo run -p aarnn-nsys --bin l2bridge -- \
  --iface vethA --name /l2demoA --desc 16384 --slab $((64<<20)) \
  --dst-mac 02:00:00:00:00:02 --ethertype 0xCAFE \
  --max-frame 4096 --ttl 8 --self-test --trace
```
You should see periodic lines like:
```
[l2bridge/trace] wrong_ethertype=0 short_frame=0 crc_fail=0 reasm_reset=0 tx_frag=3 tx_bytes=540
[l2bridge/trace] first TX: 60 bytes
[l2bridge/trace] first RX: 74 bytes
```
These confirm the link is active and frames are being exchanged.

### Updates in v0.1.9 (L2 bridge)
- The raw socket now joins AF_PACKET promiscuous membership (PACKET_ADD_MEMBERSHIP with PACKET_MR_PROMISC) so unicast frames destined for the configured peer MAC are reliably received on both veth and external NICs.
- In `--self-test` mode the bridge periodically re-sends a small on-wire probe until the first ACK is seen (trace logs are rate-limited).
