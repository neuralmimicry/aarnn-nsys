use aarnn_nsys::bus;

use std::env;
use std::time::{Duration, Instant};

fn parse_args() -> Vec<String> { env::args().collect() }

fn usage() {
    eprintln!("Usage:\n  aarnn-nsys create <name> <desc_capacity_pow2> <slab_bytes>\n  aarnn-nsys prod   <name> <desc_capacity_pow2> <slab_bytes> [msg_size]\n  aarnn-nsys sub    <name> <desc_capacity_pow2> <slab_bytes>\n  aarnn-nsys concat <src_name> <src_desc_cap> <src_slab> <dst_name> <dst_desc_cap> <dst_slab> [seconds]");
}

fn main() {
    let args = parse_args();
    if args.len() < 2 { usage(); return; }
    match args[1].as_str() {
        "create" => {
            if args.len() < 5 { usage(); return; }
            let name = &args[2];
            let desc_cap: usize = args[3].parse().expect("desc_capacity");
            let slab: usize = args[4].parse().expect("slab_bytes");
            let bus = bus::BusHandle::create(name, desc_cap, slab).expect("create bus");
            println!("bus created: {} desc_cap={} slab={}B slot_bytes={}", name, desc_cap, slab, bus.slot_bytes());
        }
        "prod" => {
            if args.len() < 5 { usage(); return; }
            let name = &args[2];
            let desc_cap: usize = args[3].parse().expect("desc_capacity");
            let slab: usize = args[4].parse().expect("slab_bytes");
            let bus = match bus::BusHandle::open(name, desc_cap, slab) {
                Ok(b) => b,
                Err(bus::BusError::Platform(code)) => {
                    if code == 2 { // ENOENT
                        eprintln!("error: bus '{}' not found (ENOENT). Create it first: aarnn-nsys create {} {} {}", name, name, desc_cap, slab);
                    } else {
                        eprintln!("error: failed to open bus '{}' (desc_cap={}, slab={}): platform errno={}", name, desc_cap, slab, code);
                    }
                    return;
                }
                Err(bus::BusError::InvalidArg) => {
                    eprintln!("error: size mismatch opening bus '{}'. Ensure desc_capacity_pow2 and slab_bytes match the values used at creation.", name);
                    return;
                }
                Err(e) => { eprintln!("error: failed to open bus '{}': {}", name, e); return; }
            };
            // Touch desc_capacity() so it isn't considered dead and to assert mapping is as expected
            let _ = bus.desc_capacity();
            let slot_bytes = bus.slot_bytes();
            let msg_size: usize = if args.len() > 5 { args[5].parse().unwrap_or(256) } else { 256 };
            if msg_size > slot_bytes {
                eprintln!("error: msg_size {} exceeds slot_bytes {} (slab/desc_cap).", msg_size, slot_bytes);
                return;
            }
            let prod = bus.producer();
            let payload = vec![0u8; msg_size];
            eprintln!("producer: starting — name='{}' desc_cap={} slab={}B slot_bytes={} msg_size={}B duration=10s", name, desc_cap, slab, slot_bytes, msg_size);
            let start = Instant::now();
            let mut last_report = start;
            let mut sent: u64 = 0;
            while start.elapsed() < Duration::from_secs(10) {
                match prod.try_publish(&payload) {
                    Ok(true) => { sent += 1; }
                    Ok(false) => { // backpressure: fall back to blocking publish to preserve throughput
                        if let Err(e) = prod.publish(&payload) { eprintln!("publish error: {}", e); break; }
                        sent += 1;
                    }
                    Err(e) => { eprintln!("publish error: {}", e); break; }
                }
                if last_report.elapsed() >= Duration::from_secs(1) {
                    let secs = start.elapsed().as_secs_f64();
                    let mps = (sent as f64 / 1e6) / secs.max(1e-9);
                    let gbps = (sent as f64 * msg_size as f64 * 8.0) / 1e9 / secs.max(1e-9);
                    eprintln!("producer: progress — sent={} rate={:.1} Mmsg/s {:.1} Gbps elapsed={:.1}s", sent, mps, gbps, secs);
                    last_report = Instant::now();
                }
            }
            let secs = start.elapsed().as_secs_f64();
            let gbps = (sent as f64 * msg_size as f64 * 8.0) / 1e9 / secs;
            eprintln!("producer: done — sent={} msgs, size={}B, rate={:.1} Mmsg/s, {:.1} Gbps", sent, msg_size, (sent as f64 / 1e6) / secs, gbps);
        }
        "concat" => {
            if args.len() < 8 { usage(); return; }
            let s_name = &args[2];
            let s_desc: usize = args[3].parse().expect("src desc_capacity");
            let s_slab: usize = args[4].parse().expect("src slab_bytes");
            let d_name = &args[5];
            let d_desc: usize = args[6].parse().expect("dst desc_capacity");
            let d_slab: usize = args[7].parse().expect("dst slab_bytes");
            let seconds: u64 = if args.len() > 8 { args[8].parse().unwrap_or(10) } else { 10 };
            let src = match bus::BusHandle::open(s_name, s_desc, s_slab) {
                Ok(b) => b,
                Err(bus::BusError::Platform(code)) => {
                    if code == 2 { // ENOENT
                        eprintln!("error: source bus '{}' not found (ENOENT). Create it first: aarnn-nsys create {} {} {}", s_name, s_name, s_desc, s_slab);
                    } else {
                        eprintln!("error: failed to open source bus '{}' (desc_cap={}, slab={}): platform errno={}", s_name, s_desc, s_slab, code);
                    }
                    return;
                }
                Err(bus::BusError::InvalidArg) => {
                    eprintln!("error: size mismatch opening source bus '{}'. Ensure desc_capacity_pow2 and slab_bytes match the values used at creation.", s_name);
                    return;
                }
                Err(e) => { eprintln!("error: failed to open source bus '{}': {}", s_name, e); return; }
            };
            let dst = match bus::BusHandle::open(d_name, d_desc, d_slab) {
                Ok(b) => b,
                Err(bus::BusError::Platform(code)) => {
                    if code == 2 { // ENOENT
                        eprintln!("error: destination bus '{}' not found (ENOENT). Create it first: aarnn-nsys create {} {} {}", d_name, d_name, d_desc, d_slab);
                    } else {
                        eprintln!("error: failed to open destination bus '{}' (desc_cap={}, slab={}): platform errno={}", d_name, d_desc, d_slab, code);
                    }
                    return;
                }
                Err(bus::BusError::InvalidArg) => {
                    eprintln!("error: size mismatch opening destination bus '{}'. Ensure desc_capacity_pow2 and slab_bytes match the values used at creation.", d_name);
                    return;
                }
                Err(e) => { eprintln!("error: failed to open destination bus '{}': {}", d_name, e); return; }
            };
            let sub = src.subscribe().expect("subscribe src");
            let prod = dst.producer();
            if dst.slot_bytes() < 1 { eprintln!("invalid dst slot size"); return; }
            // scratch sized to source slot size to be able to accept any src message
            let mut buf = vec![0u8; src.slot_bytes()];
            eprintln!("concat: starting — src='{}' dst='{}' seconds={} src_desc_cap={} src_slab={}B dst_desc_cap={} dst_slab={}B src.slot_bytes={} dst.slot_bytes={}", s_name, d_name, seconds, s_desc, s_slab, d_desc, d_slab, src.slot_bytes(), dst.slot_bytes());
            let start = Instant::now();
            let mut last_report = start;
            let mut fwd: u64 = 0;
            let mut drop_oversize: u64 = 0;
            loop {
                match bus::relay_once(&sub, &prod, &mut buf) {
                    Ok(Some(_n)) => { fwd += 1; }
                    Ok(None) => { std::thread::yield_now(); }
                    Err(bus::BusError::MsgTooLarge) => { drop_oversize += 1; }
                    Err(e) => { eprintln!("concat error: {}", e); break; }
                }
                if last_report.elapsed() >= Duration::from_secs(1) {
                    let secs = start.elapsed().as_secs_f64();
                    let mps = (fwd as f64 / 1e6) / secs.max(1e-9);
                    eprintln!("concat: progress — forwarded={} dropped_oversize={} rate={:.1} Mmsg/s elapsed={:.1}s", fwd, drop_oversize, mps, secs);
                    last_report = Instant::now();
                }
                if start.elapsed() > Duration::from_secs(seconds) { break; }
            }
            eprintln!("concat: done — forwarded={} dropped_oversize={}", fwd, drop_oversize);
        }
        "sub" => {
            if args.len() < 5 { usage(); return; }
            let name = &args[2];
            let desc_cap: usize = args[3].parse().expect("desc_capacity");
            let slab: usize = args[4].parse().expect("slab_bytes");
            let bus = match bus::BusHandle::open(name, desc_cap, slab) {
                Ok(b) => b,
                Err(bus::BusError::Platform(code)) => {
                    if code == 2 { // ENOENT
                        eprintln!("error: bus '{}' not found (ENOENT). Create it first: aarnn-nsys create {} {} {}", name, name, desc_cap, slab);
                    } else {
                        eprintln!("error: failed to open bus '{}' (desc_cap={}, slab={}): platform errno={}", name, desc_cap, slab, code);
                    }
                    return;
                }
                Err(bus::BusError::InvalidArg) => {
                    eprintln!("error: size mismatch opening bus '{}'. Ensure desc_capacity_pow2 and slab_bytes match the values used at creation.", name);
                    return;
                }
                Err(e) => { eprintln!("error: failed to open bus '{}': {}", name, e); return; }
            };
            let sub = bus.subscribe().expect("subscribe");
            let mut buf = vec![0u8; bus.slot_bytes()];
            let start = Instant::now();
            let mut got: u64 = 0;
            loop {
                match sub.recv_blocking(&mut buf, Duration::from_millis(50)) {
                    Ok(_n) => { got += 1; }
                    Err(bus::BusError::NotReady) => { /* timeout: continue */ }
                    Err(e) => { eprintln!("recv error: {}", e); break; }
                }
                if start.elapsed() > Duration::from_secs(10) { break; }
            }
            let secs = start.elapsed().as_secs_f64();
            eprintln!("subscriber: got={} msgs, rate={:.1} Mmsg/s", got, (got as f64 / 1e6) / secs);
        }
        _ => usage(),
    }
}
