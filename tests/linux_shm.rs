#![cfg(feature = "linux-shm")]
// Integration test for the Linux shared memory backend (default features).
use aarnn_nsys::bus::{BusHandle, BusError};

#[test]
fn shm_create_open_publish_recv() {
    // Use a unique name per test run (include pid)
    let name = format!("/nsys_test_{}", std::process::id());
    let desc = 1024usize; // power of two
    let slab = 1024usize * 64; // slot_bytes = 64

    let bus = BusHandle::create(&name, desc, slab).expect("create bus");
    assert_eq!(bus.desc_capacity(), desc);
    assert_eq!(bus.slot_bytes(), slab / desc);

    // Open a second handle to the same region
    let bus2 = BusHandle::open(&name, desc, slab).expect("open bus");

    // Sub on 2nd handle, publish from 1st
    let sub = bus2.subscribe().expect("subscribe");
    let prod = bus.producer();

    let msg = b"hello-linux-shm";
    prod.publish(msg).expect("publish");

    let mut buf = vec![0u8; bus.slot_bytes()];
    match sub.try_recv(&mut buf) {
        Ok(Some(n)) => assert_eq!(&buf[..n], msg),
        other => panic!("unexpected recv result: {:?}", other),
    }
}

#[test]
fn shm_oversize_and_backpressure() {
    let name = format!("/nsys_test_over_{}", std::process::id());
    let desc = 8usize; // small ring to exercise backpressure
    let slab = 8usize * 32; // slot_bytes = 32
    let bus = BusHandle::create(&name, desc, slab).expect("create bus");
    let sub = bus.subscribe().expect("subscribe");
    let prod = bus.producer();

    // Oversize must be rejected
    let too_big = vec![0u8; 64];
    match prod.publish(&too_big) {
        Err(BusError::MsgTooLarge) => {}
        other => panic!("expected MsgTooLarge, got {:?}", other),
    }

    // Fill the ring
    let payload = [0xABu8; 8];
    for _ in 0..desc { prod.publish(&payload).unwrap(); }

    // Now try_publish should report backpressure
    match prod.try_publish(&payload) {
        Ok(false) => {}
        other => panic!("expected backpressure Ok(false), got {:?}", other),
    }

    // Free a slot and try again
    let mut buf = vec![0u8; bus.slot_bytes()];
    let _ = sub.try_recv(&mut buf).unwrap();
    assert_eq!(prod.try_publish(&payload), Ok(true));
}
