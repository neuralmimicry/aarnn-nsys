use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aarnn_nsys::bus::BusHandle;

// Usage:
// cargo run --example multi_producers -- /demo 8192 33554432 512
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: multi_producers <name> <desc_capacity_pow2> <slab_bytes> <msg_size>");
        std::process::exit(1);
    }
    let name = &args[1];
    let desc_cap: usize = args[2].parse().expect("desc_capacity");
    let slab: usize = args[3].parse().expect("slab_bytes");
    let msg_size: usize = args[4].parse().expect("msg_size");

    let bus = Arc::new(BusHandle::create(name, desc_cap, slab).expect("create bus"));
    let slot = bus.slot_bytes();
    assert!(msg_size <= slot, "msg_size must be <= slot_bytes ({slot})");

    let sub = bus.subscribe().expect("subscribe");

    // Two producer threads with distinct source IDs in payload[0]
    let payload_a = vec![0xAAu8; msg_size];
    let payload_b = vec![0xBBu8; msg_size];
    let bus_a = Arc::clone(&bus);
    let bus_b = Arc::clone(&bus);

    let th_a = thread::spawn(move || {
        let prod_a = bus_a.producer();
        let start = Instant::now();
        let mut sent = 0u64;
        while start.elapsed() < Duration::from_secs(3) {
            prod_a.publish(&payload_a).unwrap();
            sent += 1;
        }
        sent
    });

    let th_b = thread::spawn(move || {
        let prod_b = bus_b.producer();
        let start = Instant::now();
        let mut sent = 0u64;
        while start.elapsed() < Duration::from_secs(3) {
            prod_b.publish(&payload_b).unwrap();
            sent += 1;
        }
        sent
    });

    // Subscriber: count per-source markers
    let mut buf = vec![0u8; slot];
    let start = Instant::now();
    let mut got_a = 0u64;
    let mut got_b = 0u64;
    while start.elapsed() < Duration::from_secs(3) {
        match sub.try_recv(&mut buf) {
            Ok(Some(n)) => {
                if n > 0 {
                    match buf[0] {
                        0xAA => got_a += 1,
                        0xBB => got_b += 1,
                        _ => {}
                    }
                }
            }
            Ok(None) => std::thread::yield_now(),
            Err(e) => panic!("recv error: {e}"),
        }
    }

    let sent_a = th_a.join().unwrap();
    let sent_b = th_b.join().unwrap();

    println!("multi_producers: sent_a={sent_a} sent_b={sent_b} got_a={got_a} got_b={got_b}");
}
