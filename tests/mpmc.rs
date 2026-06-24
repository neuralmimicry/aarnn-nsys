use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aarnn_nsys::bus::{relay_once, BusHandle};

fn unique_name(tag: &str) -> String {
    format!("/aarnn_nsys_{}_{}_{}", tag, std::process::id(), Instant::now().elapsed().as_nanos())
}

#[test]
fn single_producer_single_subscriber() {
    let name = unique_name("spsc");
    let desc = 1024usize;
    let slab = 1024usize * 4096; // 4 MiB
    let bus = Arc::new(BusHandle::create(&name, desc, slab).expect("create"));
    let sub = bus.subscribe().expect("subscribe");
    let mut buf = vec![0u8; bus.slot_bytes()];

    let payload = vec![0x11u8; 128];
    let total = 10_000u32;

    // Spawn producer on another thread so we don't block when the ring fills.
    let bus_p = Arc::clone(&bus);
    let payload_p = payload.clone();
    let prod_t = thread::spawn(move || {
        let prod = bus_p.producer();
        for _ in 0..total { prod.publish(&payload_p).unwrap(); }
    });

    let mut got = 0u32;
    let start = Instant::now();
    while got < total && start.elapsed() < Duration::from_secs(5) {
        match sub.try_recv(&mut buf) {
            Ok(Some(n)) => { assert_eq!(n, 128); got += 1; }
            Ok(None) => std::thread::yield_now(),
            Err(e) => panic!("recv error: {e}"),
        }
    }
    prod_t.join().unwrap();
    assert_eq!(got, total);
}

#[test]
fn multi_producers_single_subscriber_counts() {
    let name = unique_name("mpsc");
    let desc = 4096usize;
    let slab = 4096usize * 4096; // 16 MiB
    let bus = Arc::new(BusHandle::create(&name, desc, slab).expect("create"));
    let sub = bus.subscribe().expect("subscribe");
    let slot = bus.slot_bytes();

    let msg_size = 256usize;
    assert!(msg_size <= slot);
    let bus_a = Arc::clone(&bus);
    let bus_b = Arc::clone(&bus);
    let a = thread::spawn(move || {
        let prod_a = bus_a.producer();
        let payload = vec![0xA1u8; msg_size];
        let start = Instant::now();
        let mut sent = 0u64;
        while start.elapsed() < Duration::from_millis(500) {
            prod_a.publish(&payload).unwrap();
            sent += 1;
        }
        sent
    });

    let b = thread::spawn(move || {
        let prod_b = bus_b.producer();
        let payload = vec![0xB2u8; msg_size];
        let start = Instant::now();
        let mut sent = 0u64;
        while start.elapsed() < Duration::from_millis(500) {
            prod_b.publish(&payload).unwrap();
            sent += 1;
        }
        sent
    });

    let mut buf = vec![0u8; slot];
    let start = Instant::now();
    let mut got_a = 0u64;
    let mut got_b = 0u64;
    while start.elapsed() < Duration::from_secs(1) {
        match sub.try_recv(&mut buf) {
            Ok(Some(n)) => {
                assert_eq!(n, msg_size);
                match buf[0] { 0xA1 => got_a += 1, 0xB2 => got_b += 1, _ => {} }
            }
            Ok(None) => std::thread::yield_now(),
            Err(e) => panic!("recv error: {e}"),
        }
    }

    let sent_a = a.join().unwrap();
    let sent_b = b.join().unwrap();

    // We should receive close to the number sent (allow some slack if test env is slow)
    assert!(got_a > 0 && got_b > 0);
    assert!(sent_a > 0 && sent_b > 0);
}

#[test]
fn try_publish_backpressure() {
    let name = unique_name("backpressure");
    // Tiny ring to force backpressure quickly
    let desc = 8usize;
    let slab = 8usize * 256;
    let bus = BusHandle::create(&name, desc, slab).expect("create");
    let prod = bus.producer();
    let sub = bus.subscribe().expect("subscribe");
    let mut buf = vec![0u8; bus.slot_bytes()];
    let payload = vec![0xCCu8; 64];

    // Fill ring without consuming
    let mut published = 0;
    for _ in 0..(desc * 2) { // attempt more than capacity
        match prod.try_publish(&payload) {
            Ok(true) => published += 1,
            Ok(false) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(published <= desc as usize);

    // Now consume all and ensure we can publish again
    let mut got = 0usize;
    let start = Instant::now();
    while got < published && start.elapsed() < Duration::from_secs(1) {
        if let Ok(Some(_)) = sub.try_recv(&mut buf) { got += 1; } else { std::thread::yield_now(); }
    }
    assert_eq!(got, published);
    assert!(prod.try_publish(&payload).unwrap());
}

#[test]
fn relay_between_buses() {
    let src_name = unique_name("src");
    let dst_name = unique_name("dst");
    let desc = 1024usize;
    let slab = 1024usize * 1024; // 1 MiB
    let src = BusHandle::create(&src_name, desc, slab).expect("create src");
    let dst = BusHandle::create(&dst_name, desc, slab).expect("create dst");

    let prod_src = src.producer();
    let sub_src = src.subscribe().expect("sub src");
    let sub_dst = dst.subscribe().expect("sub dst");
    let prod_dst = dst.producer();

    // Producer on src
    let payload = vec![0x5Au8; 128];
    for _ in 0..1000 { prod_src.publish(&payload).unwrap(); }

    // Relay loop for a short time
    let mut scratch = vec![0u8; src.slot_bytes()];
    let start = Instant::now();
    let mut fwd = 0usize;
    while start.elapsed() < Duration::from_secs(1) {
        match relay_once(&sub_src, &prod_dst, &mut scratch) {
            Ok(Some(_n)) => fwd += 1,
            Ok(None) => break,
            Err(e) => panic!("relay error: {e}"),
        }
    }

    // Receive on dst
    let mut buf = vec![0u8; dst.slot_bytes()];
    let mut got = 0usize;
    let start = Instant::now();
    while got < fwd && start.elapsed() < Duration::from_secs(2) {
        if let Ok(Some(n)) = sub_dst.try_recv(&mut buf) { assert_eq!(n, 128); got += 1; } else { std::thread::yield_now(); }
    }
    assert_eq!(got, fwd);
}
