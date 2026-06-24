use std::time::{Duration, Instant};
use aarnn_nsys::bus::BusHandle;

// Usage:
// cargo run --example subscriber -- /demo 8192 33554432
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: subscriber <name> <desc_capacity_pow2> <slab_bytes>");
        std::process::exit(1);
    }
    let name = &args[1];
    let desc_cap: usize = args[2].parse().expect("desc_capacity");
    let slab: usize = args[3].parse().expect("slab_bytes");

    let bus = BusHandle::open(name, desc_cap, slab).expect("open bus");
    let sub = bus.subscribe().expect("subscribe");
    let mut buf = vec![0u8; bus.slot_bytes()];

    let start = Instant::now();
    let mut got = 0u64;
    while start.elapsed() < Duration::from_secs(10) {
        match sub.try_recv(&mut buf) {
            Ok(Some(_n)) => got += 1,
            Ok(None) => std::thread::yield_now(),
            Err(e) => { eprintln!("recv error: {e}"); break; }
        }
    }

    println!("subscriber: got={got}");
}
