// Linux Layer-2 (Ethernet) messagebus bridge using AF_PACKET raw sockets (no IP/TCP/UDP).
// Frames carry a small header over a custom EtherType (default 0xCAFE) and a payload fragment.
// The bridge forwards between a local aarnn-nsys bus (POSIX shared memory backend) and the L2 link.
//
// CLI example:
//   cargo run -p aarnn-nsys --bin l2bridge -- \
//     --iface veth0 --name /demo --desc 16384 --slab 67108864 \
//     --dst-mac 02:00:00:00:00:02 --ethertype 0xCAFE --max-frame 4096 --ttl 8 --self-test
//
// Notes:
// - Requires CAP_NET_RAW (typically run as root) to open AF_PACKET sockets.
// - Uses non-blocking socket + poll to avoid blocking the process.
// - Performs fragmentation/reassembly for payloads larger than MTU-headers.

use std::ffi::CString;
use std::io::ErrorKind;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Instant;

use aarnn_nsys::bus::BusHandle;

#[derive(Clone, Copy, Debug)]
struct Cfg {
    ethertype: u16,
    max_frame: usize, // logical message size (e.g., 4096)
    ttl: u8,
}

// L2 payload header (12 bytes)
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct L2Hdr {
    ver: u8,       // 1
    chan: u8,      // 0=data, 1=ack
    flags: u8,     // bit0=frag, bit1=last
    ttl: u8,       // hop limit
    msg_id: u16,   // reassembly key
    frag_idx: u16, // fragment index
    frag_len: u16, // bytes in this fragment
    crc16: u16,    // crc over header (first 10 bytes) + payload
}

const VER: u8 = 1;
const FLAG_FRAG: u8 = 1 << 0;
const FLAG_LAST: u8 = 1 << 1;

fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let parts: Vec<_> = s.split(':').collect();
    if parts.len() != 6 { return None; }
    let mut mac = [0u8; 6];
    for i in 0..6 {
        mac[i] = u8::from_str_radix(parts[i], 16).ok()?;
    }
    Some(mac)
}

fn mac_to_string(mac: &[u8; 6]) -> String {
    format!("{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5])
}

fn crc16_ccitt(mut crc: u16, data: &[u8]) -> u16 {
    for &b in data { crc ^= (b as u16) << 8; for _ in 0..8 { if (crc & 0x8000) != 0 { crc = (crc << 1) ^ 0x1021; } else { crc <<= 1; } } }
    crc
}

fn htons(x: u16) -> u16 { x.to_be() }

#[allow(non_camel_case_types)]
mod ll {
    pub const AF_PACKET: i32 = 17;
    pub const SOCK_RAW: i32 = 3;
    pub const SOCK_NONBLOCK: i32 = 0o00004000;
    pub const SOCK_CLOEXEC: i32 = 0o2000000;
    pub const POLLIN: i16 = 0x0001;
    pub const POLLOUT: i16 = 0x0004;
    pub const SIOCGIFINDEX: u64 = 0x8933;

    #[repr(C)]
    pub struct sockaddr_ll {
        pub sll_family: u16,
        pub sll_protocol: u16,
        pub sll_ifindex: i32,
        pub sll_hatype: u16,
        pub sll_pkttype: u8,
        pub sll_halen: u8,
        pub sll_addr: [u8; 8],
    }

    #[repr(C)]
    pub struct ifreq {
        pub ifr_name: [u8; 16],
        pub ifr_ifindex: i32, // overlay field when using SIOCGIFINDEX
    }

    extern "C" {
        pub fn socket(domain: i32, ty: i32, proto: i32) -> i32;
        pub fn bind(fd: i32, addr: *const libc::sockaddr, len: libc::socklen_t) -> i32;
        pub fn send(fd: i32, buf: *const libc::c_void, len: usize, flags: i32) -> isize;
        pub fn recv(fd: i32, buf: *mut libc::c_void, len: usize, flags: i32) -> isize;
        pub fn ioctl(fd: i32, req: u64, ifr: *mut ifreq) -> i32;
        pub fn poll(fds: *mut libc::pollfd, nfds: libc::nfds_t, timeout: i32) -> i32;
    }
}

struct RawSock {
    fd: OwnedFd,
    src_mac: [u8; 6],
}

fn get_ifindex(fd: RawFd, ifname: &str) -> std::io::Result<i32> {
    let mut ifr = ll::ifreq { ifr_name: [0; 16], ifr_ifindex: 0 };
    let name = CString::new(ifname).unwrap();
    let bytes = name.as_bytes_with_nul();
    if bytes.len() > 16 { return Err(std::io::Error::new(ErrorKind::InvalidInput, "ifname too long")); }
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ifr.ifr_name.as_mut_ptr(), bytes.len()); }
    let rc = unsafe { ll::ioctl(fd, ll::SIOCGIFINDEX, &mut ifr as *mut _ as *mut _) };
    if rc != 0 { return Err(std::io::Error::last_os_error()); }
    Ok(ifr.ifr_ifindex)
}

fn open_raw_socket(ifname: &str, ethertype: u16) -> std::io::Result<RawSock> {
    let fd = unsafe { ll::socket(ll::AF_PACKET, ll::SOCK_RAW | ll::SOCK_NONBLOCK | ll::SOCK_CLOEXEC, htons(ethertype) as i32) };
    if fd < 0 { return Err(std::io::Error::last_os_error()); }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let ifindex = get_ifindex(fd.as_raw_fd(), ifname)?;
    // Bind
    let addr = ll::sockaddr_ll {
        sll_family: ll::AF_PACKET as u16,
        sll_protocol: htons(ethertype),
        sll_ifindex: ifindex,
        sll_hatype: 1,
        sll_pkttype: 0,
        sll_halen: 6,
        sll_addr: [0; 8],
    };
    let rc = unsafe {
        ll::bind(
            fd.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            size_of::<ll::sockaddr_ll>() as libc::socklen_t,
        )
    };
    if rc != 0 { return Err(std::io::Error::last_os_error()); }

    // Join promiscuous membership at the socket level so we receive unicast for the peer MAC reliably
    #[cfg(target_os = "linux")]
    unsafe {
        let mut mreq: libc::packet_mreq = core::mem::zeroed();
        mreq.mr_ifindex = ifindex;
        mreq.mr_type = libc::PACKET_MR_PROMISC as u16;
        // mr_alen/mr_address not required for PROMISC
        let _ = libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_PACKET,
            libc::PACKET_ADD_MEMBERSHIP,
            &mreq as *const _ as *const libc::c_void,
            core::mem::size_of::<libc::packet_mreq>() as libc::socklen_t,
        );
    }

    // Try to read source MAC via recv of any packet? Simpler: require --src-mac or generate locally administered MAC.
    Ok(RawSock { fd, src_mac: gen_local_mac() })
}

fn gen_local_mac() -> [u8; 6] {
    // Locally administered unicast: set bit1 (LAA), clear multicast bit
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let mut mac = [0u8; 6];
    let x = t as u64;
    mac[0] = 0x02; // LAA
    mac[1] = (x & 0xFF) as u8;
    mac[2] = ((x >> 8) & 0xFF) as u8;
    mac[3] = ((x >> 16) & 0xFF) as u8;
    mac[4] = ((x >> 24) & 0xFF) as u8;
    mac[5] = ((x >> 32) & 0xFF) as u8;
    mac
}

fn build_and_send_frag(
    sock: &RawSock,
    dst_mac: &[u8; 6],
    cfg: &Cfg,
    chan: u8,
    ttl: u8,
    msg_id: u16,
    frag_idx: u16,
    last: bool,
    payload: &[u8],
) -> std::io::Result<usize> {
    let mut frame = Vec::with_capacity(14 + size_of::<L2Hdr>() + payload.len());
    // Ethernet header
    frame.extend_from_slice(dst_mac);
    frame.extend_from_slice(&sock.src_mac);
    // EtherType must be in network byte order (big-endian) exactly once
    frame.extend_from_slice(&cfg.ethertype.to_be_bytes());
    // L2 header (first 10 bytes used for CRC calc, last 2 for CRC)
    // Ensure minimum Ethernet payload size for zero-length ACKs: ETH min payload is 46 bytes (excludes 14B eth hdr).
    // Our L2 header is 12 bytes, so ensure payload section is at least 34 bytes when we would otherwise send 0.
    let mut eff_payload_storage: Vec<u8> = Vec::new();
    let mut eff_payload: &[u8] = payload;
    let min_payload_len = 46usize.saturating_sub(size_of::<L2Hdr>()); // 34
    if chan == 1 && last && payload.is_empty() {
        eff_payload_storage.resize(min_payload_len, 0);
        eff_payload = &eff_payload_storage[..];
    }

    let mut flags = 0u8;
    if !eff_payload.is_empty() { flags |= FLAG_FRAG; }
    if last { flags |= FLAG_LAST; }
    let mut hdr = [0u8; 12];
    hdr[0] = VER; hdr[1] = chan; hdr[2] = flags; hdr[3] = ttl;
    hdr[4] = (msg_id & 0xFF) as u8; hdr[5] = (msg_id >> 8) as u8;
    hdr[6] = (frag_idx & 0xFF) as u8; hdr[7] = (frag_idx >> 8) as u8;
    hdr[8] = (eff_payload.len() as u16 & 0xFF) as u8; hdr[9] = ((eff_payload.len() as u16) >> 8) as u8;
    let mut crc = crc16_ccitt(0xFFFF, &hdr[..10]);
    crc = crc16_ccitt(crc, eff_payload);
    hdr[10] = (crc & 0xFF) as u8; hdr[11] = (crc >> 8) as u8;
    frame.extend_from_slice(&hdr);
    frame.extend_from_slice(eff_payload);

    let rc = unsafe { ll::send(sock.fd.as_raw_fd(), frame.as_ptr() as *const _, frame.len(), 0) };
    if rc < 0 { return Err(std::io::Error::last_os_error()); }
    Ok(frame.len())
}

fn poll_once(fd: RawFd, timeout_ms: i32, want_read: bool, want_write: bool) -> std::io::Result<(bool, bool)> {
    let mut pfd = libc::pollfd { fd, events: 0, revents: 0 };
    if want_read { pfd.events |= ll::POLLIN; }
    if want_write { pfd.events |= ll::POLLOUT; }
    let rc = unsafe { ll::poll(&mut pfd as *mut _, 1, timeout_ms) };
    if rc < 0 { return Err(std::io::Error::last_os_error()); }
    Ok(((pfd.revents & ll::POLLIN) != 0, (pfd.revents & ll::POLLOUT) != 0))
}

fn recv_into(buf: &mut [u8], fd: RawFd) -> std::io::Result<usize> {
    let rc = unsafe { ll::recv(fd, buf.as_mut_ptr() as *mut _, buf.len(), 0) };
    if rc < 0 { return Err(std::io::Error::last_os_error()); }
    Ok(rc as usize)
}

fn main() {
    // Parse CLI
    let mut args = std::env::args().skip(1);
    let mut iface = String::new();
    let mut name = String::from("/demo");
    let mut desc: usize = 16384;
    let mut slab: usize = 64<<20;
    let mut ethertype: u16 = 0xCAFE;
    let mut dst_mac_opt: Option<[u8; 6]> = None;
    let mut src_mac_opt: Option<[u8; 6]> = None;
    let mut max_frame: usize = 4096;
    let mut ttl: u8 = 8;
    let mut self_test = false;
    let mut trace = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iface" => iface = args.next().expect("--iface value"),
            "--name" => name = args.next().expect("--name value"),
            "--desc" => desc = args.next().unwrap().parse().unwrap(),
            "--slab" => slab = args.next().unwrap().parse().unwrap(),
            "--ethertype" => {
                let s = args.next().expect("--ethertype value");
                ethertype = if s.starts_with("0x") || s.starts_with("0X") { u16::from_str_radix(&s[2..], 16).unwrap() } else { s.parse().unwrap() };
            }
            "--dst-mac" => dst_mac_opt = parse_mac(&args.next().expect("--dst-mac value")),
            "--src-mac" => src_mac_opt = parse_mac(&args.next().expect("--src-mac value")),
            "--max-frame" => max_frame = args.next().unwrap().parse().unwrap(),
            "--ttl" => ttl = args.next().unwrap().parse().unwrap(),
            "--self-test" => self_test = true,
            "--trace" => trace = true,
            other => { eprintln!("Unknown arg: {}", other); std::process::exit(2); }
        }
    }

    if iface.is_empty() { eprintln!("--iface is required"); std::process::exit(2); }
    let slot_bytes = slab / desc;
    if slot_bytes < max_frame { eprintln!("slot_bytes={} < max_frame={} — increase slab/desc", slot_bytes, max_frame); std::process::exit(2); }
    // Build runtime config
    let cfg = Cfg { ethertype, max_frame, ttl };

    // Bus
    let bus = BusHandle::create(&name, desc, slab).or_else(|_| BusHandle::open(&name, desc, slab)).expect("bus");
    let sub = bus.subscribe().expect("subscribe");
    let prod = bus.producer();

    // Open raw socket
    let mut sock = open_raw_socket(&iface, ethertype).expect("open AF_PACKET");
    if let Some(mac) = src_mac_opt { sock.src_mac = mac; }
    let dst_mac = if let Some(m) = dst_mac_opt { m } else { [0xff; 6] }; // default broadcast if none provided

    eprintln!("[l2bridge] iface={} src={} dst={} ethertype=0x{:04x} slot_bytes={} max_frame={}",
        iface, mac_to_string(&sock.src_mac), mac_to_string(&dst_mac), ethertype, slot_bytes, max_frame);
    // Counters for diagnostics (used when --trace)
    let mut short_frame: u64 = 0;
    let mut wrong_ethertype: u64 = 0;
    let mut crc_fail: u64 = 0;
    let mut reasm_reset: u64 = 0;
    let mut tx_frag: u64 = 0;
    let mut tx_bytes: u64 = 0;
    let mut first_tx: bool = true;
    let mut first_rx: bool = true;
    let mut last_trace = Instant::now();

    // Initialize message ID counter before any use (including self-test TX)
    let mut msg_id_ctr: u16 = 1;

    // Self-test: publish a probe locally and also send one directly over L2 so we have deterministic wire TX.
    if self_test {
        let probe = b"MBUS-L2-PROBE";
        prod.publish(probe).expect("publish probe");
        // Immediate on-wire probe (chan=0, single fragment)
        if let Ok(nbytes) = build_and_send_frag(&sock, &dst_mac, &cfg, 0, cfg.ttl, msg_id_ctr, 0, true, probe) {
            tx_frag = tx_frag.saturating_add(1);
            tx_bytes = tx_bytes.saturating_add(nbytes as u64);
            if trace && first_tx { eprintln!("[l2bridge/trace] first TX: {} bytes", nbytes); first_tx = false; }
        }
        msg_id_ctr = msg_id_ctr.wrapping_add(1);
    }

    // MTU guess: if we can’t query, assume 1500
    let mtu_payload = 1500usize - 14 - size_of::<L2Hdr>();

    let mut rx_buf = vec![0u8; 2048]; // big enough for Ethernet frame
    let mut printed_pass = false;
    let mut probe_tick: u32 = 0;

    // Reassembly map: simplistic, single in-flight msg per peer for now
    let mut reasm_buf: Vec<u8> = vec![0u8; cfg.max_frame];
    let mut reasm_len: usize = 0;
    let mut reasm_id: u16 = 0;
    let mut reasm_next_idx: u16 = 0;

    loop {
        // Periodically (low-rate) retransmit the self-test probe on wire until we see an ACK, then stop
        if self_test && !printed_pass {
            probe_tick = probe_tick.wrapping_add(1);
            if (probe_tick & 0x0FFF) == 0 {
                let probe = b"MBUS-L2-PROBE";
                if let Ok(bytes) = build_and_send_frag(&sock, &dst_mac, &cfg, 0, cfg.ttl, msg_id_ctr, 0, true, probe) {
                    tx_frag = tx_frag.saturating_add(1);
                    tx_bytes = tx_bytes.saturating_add(bytes as u64);
                    if trace && first_tx { eprintln!("[l2bridge/trace] first TX: {} bytes", bytes); first_tx = false; }
                }
                msg_id_ctr = msg_id_ctr.wrapping_add(1);
            }
        }

        // bus → L2 (fragment if needed)
        let mut out_buf = vec![0u8; slot_bytes];
        if let Ok(Some(n)) = sub.try_recv(&mut out_buf) {
            let mut sent = 0usize;
            let msg_id = msg_id_ctr; msg_id_ctr = msg_id_ctr.wrapping_add(1);
            let mut frag_idx: u16 = 0;
            while sent < n {
                let take = std::cmp::min(mtu_payload, n - sent);
                let last = sent + take >= n;
                let ttl2 = if cfg.ttl > 0 { cfg.ttl - 1 } else { 0 };
                let frag = &out_buf[sent..sent+take];
                if let Ok(bytes) = build_and_send_frag(&sock, &dst_mac, &cfg, 0, ttl2, msg_id, frag_idx, last, frag) {
                    tx_frag = tx_frag.saturating_add(1);
                    tx_bytes = tx_bytes.saturating_add(bytes as u64);
                    if trace && first_tx { eprintln!("[l2bridge/trace] first TX: {} bytes", bytes); first_tx = false; }
                }
                frag_idx = frag_idx.wrapping_add(1);
                sent += take;
            }
        }

        // poll for incoming
        let (_r, _w) = poll_once(sock.fd.as_raw_fd(), 5, true, false).unwrap();
        match recv_into(&mut rx_buf, sock.fd.as_raw_fd()) {
            Ok(nbytes) if nbytes >= 14 + size_of::<L2Hdr>() => {
                // Check EtherType
                let ethertype_be = u16::from_be_bytes([rx_buf[12], rx_buf[13]]);
                if ethertype_be != cfg.ethertype { wrong_ethertype += 1; continue; }
                // Parse L2 header
                let off = 14;
                // Bounds for header fields
                if nbytes < off + size_of::<L2Hdr>() { short_frame += 1; continue; }
                let ver = rx_buf[off + 0]; if ver != VER { continue; }
                let chan = rx_buf[off + 1];
                let flags = rx_buf[off + 2];
                let rttl = rx_buf[off + 3];
                let msg_id = (rx_buf[off + 4] as u16) | ((rx_buf[off + 5] as u16) << 8);
                let frag_idx = (rx_buf[off + 6] as u16) | ((rx_buf[off + 7] as u16) << 8);
                let frag_len = (rx_buf[off + 8] as u16) | ((rx_buf[off + 9] as u16) << 8);
                let crc_rx = (rx_buf[off + 10] as u16) | ((rx_buf[off + 11] as u16) << 8);
                let start = off + size_of::<L2Hdr>();
                // Guard against overflow
                let end = match start.checked_add(frag_len as usize) { Some(e) => e, None => { short_frame += 1; continue } };
                if end > nbytes { short_frame += 1; continue; }
                let pay = &rx_buf[start..end];
                // CRC check
                let mut hdr10 = [0u8; 10];
                hdr10.copy_from_slice(&rx_buf[off..off+10]);
                let mut crc = crc16_ccitt(0xFFFF, &hdr10);
                crc = crc16_ccitt(crc, pay);
                if crc != crc_rx { crc_fail += 1; continue; }

                if trace && first_rx { eprintln!("[l2bridge/trace] first RX: {} bytes", nbytes); first_rx = false; }
                if chan == 0 {
                    // Reassemble
                    if reasm_id != msg_id || frag_idx == 0 { reasm_id = msg_id; reasm_len = 0; reasm_next_idx = 0; }
                    if frag_idx == reasm_next_idx {
                        let new_end = match reasm_len.checked_add(pay.len()) { Some(v) => v, None => { reasm_reset += 1; reasm_len = 0; reasm_next_idx = 0; continue } };
                        if new_end <= reasm_buf.len() {
                            reasm_buf[reasm_len..new_end].copy_from_slice(pay);
                            reasm_len = new_end;
                            reasm_next_idx = reasm_next_idx.wrapping_add(1);
                        } else {
                            // overflow: reset this message
                            reasm_reset += 1;
                            reasm_len = 0; reasm_next_idx = 0; continue;
                        }
                    }
                    // Detect last fragment via FLAG_LAST
                    if (flags & FLAG_LAST) != 0 {
                        if reasm_len > 0 { let _ = prod.publish(&reasm_buf[..reasm_len]); }
                        // send ACK on chan 1 (echo payload back via same fragmentation)
                        let ack_ttl = if rttl > 0 { rttl - 1 } else { 0 };
                        if reasm_len == 0 {
                            // Edge case: zero-length payload; still send an ACK frame with LAST flag
                            if let Ok(bytes) = build_and_send_frag(&sock, &dst_mac, &cfg, 1, ack_ttl, msg_id, 0, true, &[]) {
                                tx_frag = tx_frag.saturating_add(1);
                                tx_bytes = tx_bytes.saturating_add(bytes as u64);
                                if trace && first_tx { eprintln!("[l2bridge/trace] first TX: {} bytes", bytes); first_tx = false; }
                            }
                        } else {
                            let mut sent = 0usize; let mut aidx: u16 = 0;
                            while sent < reasm_len {
                                let t = std::cmp::min(mtu_payload, reasm_len - sent);
                                let last_ack = sent + t >= reasm_len;
                                if let Ok(bytes) = build_and_send_frag(&sock, &dst_mac, &cfg, 1, ack_ttl, msg_id, aidx, last_ack, &reasm_buf[sent..sent+t]) {
                                    tx_frag = tx_frag.saturating_add(1);
                                    tx_bytes = tx_bytes.saturating_add(bytes as u64);
                                    if trace && first_tx { eprintln!("[l2bridge/trace] first TX: {} bytes", bytes); first_tx = false; }
                                }
                                aidx = aidx.wrapping_add(1); sent += t;
                            }
                        }
                        reasm_len = 0;
                    }
                } else if chan == 1 {
                    if self_test && !printed_pass {
                        eprintln!("[MBUS-L2-LNX] PASS");
                        printed_pass = true;
                    }
                }
            }
            Ok(_) => { /* short frame */ }
            Err(e) if e.kind() == ErrorKind::WouldBlock => { /* no data */ }
            Err(e) if e.kind() == ErrorKind::Interrupted => { /* retry */ }
            Err(_) => { /* drop */ }
        }
        // Rate-limited trace output
        if trace && last_trace.elapsed().as_secs_f32() >= 1.0 {
            eprintln!("[l2bridge/trace] wrong_ethertype={} short_frame={} crc_fail={} reasm_reset={} tx_frag={} tx_bytes={}",
                wrong_ethertype, short_frame, crc_fail, reasm_reset, tx_frag, tx_bytes);
            last_trace = Instant::now();
        }
        std::thread::yield_now();
    }
}
