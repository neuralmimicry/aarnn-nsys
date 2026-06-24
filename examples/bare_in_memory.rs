// Demonstrates in-memory (no shared memory) operation using the `bare` feature.
// Build & run:
//   cargo run --no-default-features --features bare --example bare_in_memory -- 1024 131072 2 256
// Args:
//   <desc_capacity_pow2> <total_bytes> [producers] [msg_size]
// Notes:
// - This uses BusHandle::from_slice over a caller-owned buffer. The example is a std binary
//   for convenience, but the library is compiled in no_std mode.

use std::time::{Duration, Instant};
use aarnn_nsys::bus::BusHandle;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: bare_in_memory <desc_capacity_pow2> <total_bytes> [producers] [msg_size]");
        std::process::exit(1);
    }
    let desc: usize = args[1].parse().expect("desc_capacity");
    let total_bytes: usize = args[2].parse().expect("total_bytes");
    let producers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(2);
    let msg_size: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(128);

    // Allocate a backing region and construct the bus from the slice
    let mut region = vec![0u8; total_bytes].into_boxed_slice();
    let bus = BusHandle::from_slice(&mut region, desc).expect("from_slice");
    let slot = bus.slot_bytes();
    assert!(msg_size <= slot, "msg_size {} must be <= slot_bytes {}", msg_size, slot);

    // Spawn producers in the same process
    let mut handles = Vec::with_capacity(producers);
    for p in 0..producers {
        let prod = bus.producer();
        let mut payload = vec![0u8; msg_size];
        payload[0] = (p as u8) + 1; // ID marker
        handles.push(std::thread::spawn(move || {
            let start = Instant::now();
            let mut sent = 0u64;
            while start.elapsed() < Duration::from_secs(1) {
                match prod.try_publish(&payload) {
                    Ok(true) => sent += 1,
                    Ok(false) => { prod.publish(&payload).unwrap(); sent += 1; }
                    Err(e) => { eprintln!("publish error: {e}"); break; }
                }
            }
            sent
        }));
    }

    let sub = bus.subscribe().expect("subscribe");
    let mut buf = vec![0u8; slot];
    let start = Instant::now();
    let mut counts = vec![0u64; producers + 1];
    while start.elapsed() < Duration::from_secs(2) {
        match sub.try_recv(&mut buf) {
            Ok(Some(n)) => { assert!(n > 0); counts[buf[0] as usize] += 1; }
            Ok(None) => std::thread::yield_now(),
            Err(e) => { eprintln!("recv error: {e}"); break; }
        }
    }

    let mut total_sent = 0u64;
    for h in handles { total_sent += h.join().unwrap(); }
    let total_recv: u64 = counts.iter().sum();
    println!("bare_in_memory summary: total_sent={} total_recv={} slot_bytes={} desc_capacity={}"
        , total_sent, total_recv, slot, bus.desc_capacity());
}