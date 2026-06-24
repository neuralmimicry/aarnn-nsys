// Linux usb-sim messagebus bridge (no TCP). Uses PTY or named pipe endpoints as a duplex byte stream.
// Builds with default features (std + linux-shm).
//
// Example endpoints:
//   --endpoint pty:/dev/pts/7
//   --endpoint pipe:/tmp/usbsimA   (expects /tmp/usbsimA.in and /tmp/usbsimA.out)
//
// CLI example:
//   cargo run -p aarnn-nsys --bin usbsim_bridge -- \
//     --name /demo --desc 16384 --slab 67108864 \
//     --endpoint pty:/dev/pts/7 --max-frame 4096 --ttl 8
//
// Notes:
// - Framing matches raspi-bare-metal/src/usbsim.rs
// - Channel 0 carries payload frames. Channel 1 is used for simple ACKs to enable bm<->linux self-tests.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;

use aarnn_nsys::bus::BusHandle;

#[cfg(unix)]
fn ignore_sigpipe() {
    unsafe { let _ = libc::signal(libc::SIGPIPE, libc::SIG_IGN); }
}
#[cfg(not(unix))]
fn ignore_sigpipe() {}

const MAGIC: u16 = 0xABCD;
const VER: u8 = 1;

#[derive(Clone, Copy, Debug)]
struct Cfg {
    max_frame: usize,
    ttl: u8,
}

fn crc16_ccitt(mut crc: u16, data: &[u8]) -> u16 {
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 { crc = (crc << 1) ^ 0x1021; } else { crc <<= 1; }
        }
    }
    crc
}

fn put_u16(buf: &mut Vec<u8>, x: u16) { buf.push((x & 0xFF) as u8); buf.push((x >> 8) as u8); }

fn write_frame<W: Write>(mut w: W, chan: u8, ttl: u8, seq: u32, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len();
    let mut out = Vec::with_capacity(2 + 10 + len + 2);
    put_u16(&mut out, MAGIC);
    out.push(VER);
    out.push(chan);
    out.push(ttl);
    out.push(0); // reserved
    put_u16(&mut out, len as u16);
    out.push((seq & 0xFF) as u8);
    out.push(((seq >> 8) & 0xFF) as u8);
    out.push(((seq >> 16) & 0xFF) as u8);
    out.push(((seq >> 24) & 0xFF) as u8);
    out.extend_from_slice(payload);
    let mut hdr = [0u8; 10];
    hdr[0] = VER; hdr[1] = chan; hdr[2] = ttl; hdr[3] = 0; hdr[4] = (len as u16 & 0xFF) as u8; hdr[5] = ((len as u16) >> 8) as u8;
    hdr[6] = (seq & 0xFF) as u8; hdr[7] = ((seq >> 8) & 0xFF) as u8; hdr[8] = ((seq >> 16) & 0xFF) as u8; hdr[9] = ((seq >> 24) & 0xFF) as u8;
    let mut crc = crc16_ccitt(0xFFFF, &hdr);
    crc = crc16_ccitt(crc, payload);
    put_u16(&mut out, crc);
    w.write_all(&out)
}

#[derive(Debug)]
enum RxErr { Io(std::io::Error), LenTooBig, BadCrc }

fn read_frame<R: Read>(r: &mut R, cfg: Cfg, work: &mut Vec<u8>) -> Result<(u8, u8, u32, usize), RxErr> {
    // Resync on MAGIC
    let mut b = [0u8; 1];
    loop {
        r.read_exact(&mut b).map_err(RxErr::Io)?;
        if b[0] as u16 == (MAGIC & 0xFF) {
            r.read_exact(&mut b).map_err(RxErr::Io)?;
            if b[0] as u16 == (MAGIC >> 8) { break; }
        }
    }
    // Read header fields
    let mut hdr = [0u8; 10];
    r.read_exact(&mut hdr).map_err(RxErr::Io)?;
    let ver = hdr[0]; if ver != VER { /* accept current only */ }
    let chan = hdr[1];
    let ttl = hdr[2];
    let len = (hdr[4] as usize) | ((hdr[5] as usize) << 8);
    if len > cfg.max_frame { return Err(RxErr::LenTooBig); }
    // Payload
    work.resize(len, 0);
    r.read_exact(&mut work[..]).map_err(RxErr::Io)?;
    // CRC
    let mut crcb = [0u8; 2];
    r.read_exact(&mut crcb).map_err(RxErr::Io)?;
    let crc_rx = (crcb[0] as u16) | ((crcb[1] as u16) << 8);
    let mut crc = crc16_ccitt(0xFFFF, &hdr);
    crc = crc16_ccitt(crc, &work[..]);
    if crc != crc_rx { return Err(RxErr::BadCrc); }
    let seq: u32 = (hdr[6] as u32) | ((hdr[7] as u32) << 8) | ((hdr[8] as u32) << 16) | ((hdr[9] as u32) << 24);
    Ok((chan, ttl, seq, len))
}

struct Endpoint {
    r: Box<dyn Read + Send>,
    w: Box<dyn Write + Send>,
}

#[cfg(unix)]
fn set_raw_fd(fd: std::os::fd::RawFd) {
    unsafe {
        let mut tio: libc::termios = core::mem::zeroed();
        if libc::tcgetattr(fd, &mut tio) != 0 { return; }
        // input flags: disable CR->NL, IXON, BRKINT, INPCK, ISTRIP
        tio.c_iflag &= !(libc::ICRNL | libc::IXON | libc::BRKINT | libc::INPCK | libc::ISTRIP);
        // output flags: disable post-processing
        tio.c_oflag &= !libc::OPOST;
        // control flags: set 8-bit chars
        tio.c_cflag |= libc::CS8;
        // local flags: disable echo, canonical mode, signals, extended funcs
        tio.c_lflag &= !(libc::ECHO | libc::ICANON | libc::IEXTEN | libc::ISIG);
        // control chars: minimum 1 byte, no timeout
        tio.c_cc[libc::VMIN] = 1;
        tio.c_cc[libc::VTIME] = 0;
        let _ = libc::tcsetattr(fd, libc::TCSANOW, &tio);
    }
}
#[cfg(unix)]
fn set_nonblock(fd: std::os::fd::RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            let _ = libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}
#[cfg(not(unix))]
fn set_raw_fd(_fd: i32) {}
#[cfg(not(unix))]
fn set_nonblock(_fd: i32) {}

fn open_endpoint(spec: &str) -> std::io::Result<Endpoint> {
    if let Some(path) = spec.strip_prefix("pty:") {
        // PTY is full duplex; open read and write handles on same path
        let r = OpenOptions::new().read(true).open(path)?;
        let w = OpenOptions::new().write(true).open(path)?;
        // Ensure raw mode and non-blocking so read loop can progress without stalling
        set_raw_fd(r.as_raw_fd());
        set_raw_fd(w.as_raw_fd());
        set_nonblock(r.as_raw_fd());
        set_nonblock(w.as_raw_fd());
        return Ok(Endpoint { r: Box::new(r), w: Box::new(w) });
    }
    if let Some(base) = spec.strip_prefix("pipe:") {
        // QEMU pipe chardev: base.in for input to guest (host->guest), base.out for output from guest (guest->host)
        // We treat r = base.out, w = base.in from the Linux process perspective when connected to a BM guest
        let rin = format!("{}.out", base);
        let win = format!("{}.in", base);
        let r = OpenOptions::new().read(true).open(&rin)?;
        let w = OpenOptions::new().write(true).open(&win)?;
        return Ok(Endpoint { r: Box::new(r), w: Box::new(w) });
    }
    Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "unsupported endpoint"))
}

fn main() {
    // Avoid SIGPIPE terminating the process when peer closes
    ignore_sigpipe();
    // Parse CLI
    let mut args = std::env::args().skip(1);
    let mut name = String::from("/demo");
    let mut desc: usize = 16384;
    let mut slab: usize = 64<<20;
    let mut endpoint = String::new();
    let mut max_frame: usize = 4096;
    let mut ttl: u8 = 8;
    let mut self_test = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => name = args.next().expect("--name value"),
            "--desc" => desc = args.next().unwrap().parse().unwrap(),
            "--slab" => slab = args.next().unwrap().parse().unwrap(),
            "--endpoint" => endpoint = args.next().expect("--endpoint value"),
            "--max-frame" => max_frame = args.next().unwrap().parse().unwrap(),
            "--ttl" => ttl = args.next().unwrap().parse().unwrap(),
            "--self-test" => self_test = true,
            _ => { eprintln!("Unknown arg: {arg}"); std::process::exit(2); }
        }
    }

    if endpoint.is_empty() { eprintln!("--endpoint is required (pty:/dev/pts/N | pipe:/tmp/name)"); std::process::exit(2); }

    let slot_bytes = slab / desc;
    if slot_bytes < max_frame { eprintln!("slot_bytes={} < max_frame={}, increase slab/desc", slot_bytes, max_frame); std::process::exit(2); }

    let bus = match BusHandle::create(&name, desc, slab).or_else(|_| BusHandle::open(&name, desc, slab)) {
        Ok(b) => Some(b),
        Err(e) => { eprintln!("[usbsim-bridge] bus unavailable ({e:?}); running without bus"); None }
    };
    let mut sub_opt = bus.as_ref().and_then(|b| b.subscribe().ok());
    let mut prod_opt = bus.as_ref().map(|b| b.producer());

    let mut ep = open_endpoint(&endpoint).expect("open endpoint");
    let cfg = Cfg { max_frame, ttl };

    eprintln!("[usbsim-bridge] online name={} desc={} slot_bytes={} endpoint={}", name, desc, slot_bytes, endpoint);

    let mut rx_buf: Vec<u8> = Vec::with_capacity(max_frame);
    let mut seq_ctr: u32 = 0;
    let mut printed_pass = false;

    // Optional self-test: inject one probe either via bus (if available) or directly over usb-sim
    if self_test {
        let probe = b"MBUS-PROBE";
        if let Some(ref mut prod) = prod_opt { let _ = prod.publish(probe); }
        else {
            let _ = write_frame(&mut ep.w, 0, cfg.ttl, seq_ctr, probe).and_then(|_| ep.w.flush());
            seq_ctr = seq_ctr.wrapping_add(1);
        }
    }

    let mut tick: u32 = 0;
    loop {
        tick = tick.wrapping_add(1);
        // Periodically resend the probe over USB in self-test mode until we see an ACK
        if self_test && !printed_pass && (tick & 0x0FFF) == 0 {
            let probe = b"MBUS-PROBE";
            eprintln!("[usbsim-bridge] sending probe len={}", probe.len());
            let _ = write_frame(&mut ep.w, 0, cfg.ttl, seq_ctr, probe).and_then(|_| ep.w.flush());
            seq_ctr = seq_ctr.wrapping_add(1);
        }

        // bus → usb (chan 0)
        let mut out_buf = vec![0u8; slot_bytes];
        if let Some(sub) = sub_opt.as_mut() {
            if let Ok(Some(n)) = sub.try_recv(&mut out_buf) {
                match write_frame(&mut ep.w, 0, cfg.ttl, seq_ctr, &out_buf[..n]) {
                    Ok(()) => { let _ = ep.w.flush(); }
                    Err(ref e) if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof) => {
                        eprintln!("[usbsim-bridge] peer closed on write: {:?}", e.kind());
                        break;
                    }
                    Err(e) => { eprintln!("[usbsim-bridge] write error: {e}"); }
                }
                seq_ctr = seq_ctr.wrapping_add(1);
            }
        }

        // usb → bus
        // Non-blocking behavior: peek with a short timeout by attempting to read 1 byte with a short timeout
        // For simplicity here, we set the underlying fds blocking and rely on the bridge being polled by the other side too.
        // Do a small try: set a tiny read with timeout semantics via read_exact_timeout wrapper for 1 byte and push back logic.
        // Instead, use a very small read deadline for full frames and swallow WouldBlock via io::ErrorKind.
        match read_frame(&mut ep.r, cfg, &mut rx_buf) {
            Ok((chan, rttl, _seq, n)) => {
                if chan == 0 {
                    if n <= slot_bytes {
                        if let Some(ref prod) = prod_opt { let _ = prod.publish(&rx_buf[..n]); }
                    }
                    // emit ACK on chan1 to help bm/linux scripts
                    let ack_ttl = if rttl > 0 { rttl - 1 } else { 0 };
                    match write_frame(&mut ep.w, 1, ack_ttl, 0xB000_0000, &rx_buf[..n]) {
                        Ok(()) => { let _ = ep.w.flush(); }
                        Err(ref e) if matches!(e.kind(), std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof) => {
                            eprintln!("[usbsim-bridge] peer closed on ack write: {:?}", e.kind());
                            break;
                        }
                        Err(e) => { eprintln!("[usbsim-bridge] ack write error: {e}"); }
                    }
                } else if chan == 1 {
                    if self_test && !printed_pass {
                        eprintln!("[MBUS-LNX] PASS");
                        printed_pass = true;
                    }
                }
            }
            Err(RxErr::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => { /* no data */ }
            Err(RxErr::Io(ref e)) if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset) => {
                eprintln!("[usbsim-bridge] input closed: {:?}", e.kind());
                break;
            }
            Err(RxErr::LenTooBig) => { eprintln!("[usbsim-bridge] LenTooBig, resyncing..."); }
            Err(RxErr::BadCrc) => { eprintln!("[usbsim-bridge] BadCrc, resyncing..."); }
            Err(RxErr::Io(e)) => { eprintln!("[usbsim-bridge] read error: {e}"); break; }
        }
        std::thread::yield_now();
    }
}
