use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aarnn_nsys::bus::{relay_once, BusHandle};

// A full start-to-finish demo in one process showing a realistic pipeline:
// - Multiple producers publish telemetry frames into a SOURCE bus
// - A relay task forwards messages into a DESTINATION bus (concatenation)
// - A subscriber on the destination validates and reports counts per source
//
// Run:
//   cargo run --example pipeline -- /src 16384 67108864 /dst 16384 67108864 2 1024 5
// Args:
//   <src_name> <src_desc_cap> <src_slab> <dst_name> <dst_desc_cap> <dst_slab> [producers] [msg_size] [seconds]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!(
            "Usage: pipeline <src_name> <src_desc_cap> <src_slab> <dst_name> <dst_desc_cap> <dst_slab> [producers] [msg_size] [seconds]"
        );
        std::process::exit(1);
    }
    let src_name = &args[1];
    let src_desc: usize = args[2].parse().expect("src desc cap");
    let src_slab: usize = args[3].parse().expect("src slab");
    let dst_name = &args[4];
    let dst_desc: usize = args[5].parse().expect("dst desc cap");
    let dst_slab: usize = args[6].parse().expect("dst slab");
    let producers: usize = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(2);
    let msg_size: usize = args.get(8).and_then(|s| s.parse().ok()).unwrap_or(512);
    let seconds: u64 = args.get(9).and_then(|s| s.parse().ok()).unwrap_or(5);

    let src = Arc::new(BusHandle::create(src_name, src_desc, src_slab).expect("create src bus"));
    let dst = Arc::new(BusHandle::create(dst_name, dst_desc, dst_slab).expect("create dst bus"));

    let slot_src = src.slot_bytes();
    let slot_dst = dst.slot_bytes();
    assert!(msg_size <= slot_src && msg_size <= slot_dst, "msg_size must fit both buses (src={}, dst={})", slot_src, slot_dst);

    // Spawn producers writing to the source bus with unique IDs in first byte
    let mut prod_threads = Vec::with_capacity(producers);
    for p in 0..producers {
        let bus = Arc::clone(&src);
        let payload = {
            let mut v = vec![0u8; msg_size];
            v[0] = (p as u8).wrapping_add(1); // ID byte (0 reserved)
            v
        };
        prod_threads.push(thread::spawn(move || {
            let prod = bus.producer();
            let start = Instant::now();
            let mut sent = 0u64;
            while start.elapsed() < Duration::from_secs(seconds) {
                // Favor throughput: use try then fallback to blocking
                match prod.try_publish(&payload) {
                    Ok(true) => sent += 1,
                    Ok(false) => { prod.publish(&payload).unwrap(); sent += 1; }
                    Err(e) => { eprintln!("producer error: {e}"); break; }
                }
            }
            (p as u8 + 1, sent)
        }));
    }

    // Relay task: forward from src to dst
    let src_for_relay = Arc::clone(&src);
    let dst_for_relay = Arc::clone(&dst);
    let relay_t = thread::spawn(move || {
        let sub = src_for_relay.subscribe().expect("sub src");
        let prod = dst_for_relay.producer();
        let mut scratch = vec![0u8; src_for_relay.slot_bytes()];
        let start = Instant::now();
        let mut forwarded = 0u64;
        let mut dropped = 0u64;
        while start.elapsed() < Duration::from_secs(seconds + 1) { // small tail to drain
            match relay_once(&sub, &prod, &mut scratch) {
                Ok(Some(_)) => forwarded += 1,
                Ok(None) => thread::yield_now(),
                Err(aarnn_nsys::bus::BusError::MsgTooLarge) => dropped += 1,
                Err(e) => { eprintln!("relay error: {e}"); break; }
            }
        }
        (forwarded, dropped)
    });

    // Destination subscriber: count per-source ID
    let sub_dst = dst.subscribe().expect("sub dst");
    let mut buf = vec![0u8; slot_dst];
    let start = Instant::now();
    let mut counts = vec![0u64; producers + 1]; // index by ID byte
    while start.elapsed() < Duration::from_secs(seconds + 2) {
        match sub_dst.try_recv(&mut buf) {
            Ok(Some(n)) => {
                assert!(n >= 1);
                let id = buf[0] as usize;
                if id < counts.len() { counts[id] += 1; }
            }
            Ok(None) => thread::yield_now(),
            Err(e) => { eprintln!("dst recv error: {e}"); break; }
        }
    }

    let (fwd, drop_os) = relay_t.join().unwrap();
    let mut total_sent = 0u64;
    for t in prod_threads { let (_id, s) = t.join().unwrap(); total_sent += s; }
    let total_recv: u64 = counts.iter().sum();

    println!("pipeline summary:\n  producers={} msg_size={}B seconds={}\n  total_sent={} forwarded={} dropped_oversize={} total_recv={}"
        , producers, msg_size, seconds, total_sent, fwd, drop_os, total_recv);
    for id in 1..counts.len() {
        println!("  dst.count[id={}]={} msgs", id, counts[id]);
    }

    // Sanity: we expect to receive close to what was forwarded; allow small tail loss on exit
    assert!(total_recv <= fwd);
}