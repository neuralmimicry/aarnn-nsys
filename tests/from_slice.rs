#![cfg(all(feature = "std", feature = "bare", not(feature = "linux-shm")))]
// Integration tests for the core in-memory (`from_slice`) path without the linux-shm backend.
use aarnn_nsys::bus::{self, BusError, BusHandle};

fn make_bus(desc: usize, slot_bytes: usize) -> BusHandle {
    let total = bus::min_buffer_size(desc, slot_bytes);
    let mut region = vec![0u8; total].into_boxed_slice();
    BusHandle::from_slice(&mut region, desc).expect("from_slice")
}

#[test]
fn from_slice_basic_publish_recv() {
    let desc = 16usize;
    let slot = 64usize;
    let bus = make_bus(desc, slot);

    let sub = bus.subscribe().expect("subscribe");
    let prod = bus.producer();

    let msg = b"hello-from-slice";
    assert!(msg.len() <= slot);
    prod.publish(msg).expect("publish");

    let mut buf = vec![0u8; slot];
    match sub.try_recv(&mut buf) {
        Ok(Some(n)) => assert_eq!(&buf[..n], msg),
        other => panic!("unexpected recv result: {:?}", other),
    }
}

#[test]
fn from_slice_fanout_and_backpressure() {
    let desc = 4usize; // tiny ring to force backpressure easily
    let slot = 32usize;
    let bus = make_bus(desc, slot);

    let s0 = bus.subscribe().unwrap();
    let s1 = bus.subscribe().unwrap();
    let prod = bus.producer();

    let msg = b"fanout";
    prod.publish(msg).unwrap();

    let mut b0 = vec![0u8; slot];
    let mut b1 = vec![0u8; slot];
    assert_eq!(s0.try_recv(&mut b0).unwrap(), Some(msg.len()));
    assert_eq!(&b0[..msg.len()], msg);
    assert_eq!(s1.try_recv(&mut b1).unwrap(), Some(msg.len()));
    assert_eq!(&b1[..msg.len()], msg);

    // Fill the ring completely
    let payload = [0xAAu8; 8];
    for _ in 0..desc { prod.publish(&payload).unwrap(); }

    // Nonblocking publish should now observe backpressure
    match prod.try_publish(&payload) {
        Ok(false) => {}
        other => panic!("expected backpressure Ok(false), got {:?}", other),
    }

    // Free a slot by advancing one subscriber, then try again
    let _ = s0.try_recv(&mut b0).unwrap();
    assert_eq!(prod.try_publish(&payload), Ok(true));
}

#[test]
fn from_slice_msg_too_large_then_ok() {
    let desc = 8usize;
    let slot = 16usize;
    let bus = make_bus(desc, slot);
    let sub = bus.subscribe().unwrap();
    let prod = bus.producer();

    let oversize = vec![0u8; slot + 1];
    match prod.publish(&oversize) {
        Err(BusError::MsgTooLarge) => {}
        other => panic!("expected MsgTooLarge, got {:?}", other),
    }

    let ok = [0x5Au8; 8];
    prod.publish(&ok).unwrap();
    let mut buf = vec![0u8; slot];
    assert_eq!(sub.try_recv(&mut buf).unwrap(), Some(ok.len()));
    assert_eq!(&buf[..ok.len()], &ok);
}
