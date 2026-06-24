//! Very low-latency shared-memory pub/sub message bus (multi-producer, multi-subscriber; Linux and no_std)
//!
//! Design highlights:
//! - MPMC ring using a global claim sequence and per-slot commit sequence (Disruptor-style publish)
//! - Fan-out (multi-subscriber) with backpressure from the slowest subscriber
//! - Fixed-size per-slot payload region (Phase A) to avoid allocator contention in MP
//! - Zero allocations on hot path; callers provide buffers for recv
//! - Platform backends: Linux shared memory (feature "linux-shm") and bare-metal (`from_slice`)

#![allow(clippy::missing_safety_doc)]
//! aarnn-nsys: Ultra-low-latency MPMC message bus with shared memory backend (Linux) and no_std in-memory backend.
//!
//! Key properties:
//! - Multi-producer, multi-subscriber fan-out with backpressure from slowest subscriber.
//! - Per-slot fixed payload region for predictable MP behavior (Phase A design).
//! - Zero allocations on the hot path; deterministic memory layout.
//! - Portable core: works in `no_std` via `from_slice`; Linux shared memory via feature `linux-shm`.
//!
//! Memory ordering summary:
//! - Producer: copy payload -> write descriptor.len -> `fence(Release)` -> set slot.seq = claim+1.
//! - Subscriber: read slot.seq with `Acquire` and proceed only if equals `read_seq+1`, then copy payload and advance its `read_seq`.
//!
//! Safety:
//! - This is a low-level, lock-free structure relying on atomics and `unsafe` pointer math. All shared memory regions must be sized and aligned as constructed here.
//! - Crash-safety is best-effort: a producer that dies mid-publish could leave an uncommitted slot; subscribers ignore it until committed.

use core::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
use core::{mem::size_of, ptr::NonNull};

/// Public constants and helpers for buffer sizing in `no_std`.
pub const fn header_layout_size(desc_capacity: usize) -> usize {
    Header::layout_size(desc_capacity)
}

/// Minimum total bytes required for a bus buffer with the given `desc_capacity` and `slot_bytes`.
/// Layout: `[Header | SlotsRing] + [PayloadSlab]`, where `payload = desc_capacity * slot_bytes`.
pub const fn min_buffer_size(desc_capacity: usize, slot_bytes: usize) -> usize {
    Header::layout_size(desc_capacity) + desc_capacity * slot_bytes
}

#[cfg(feature = "linux-shm")]
use {
    libc,
    std::ffi::CString,
    std::os::fd::FromRawFd,
    std::os::fd::AsRawFd,
    std::os::unix::io::OwnedFd,
};

#[cfg(feature = "linux-shm")]
#[inline]
fn last_errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

const CACHELINE: usize = 64;
const MAX_SUBSCRIBERS: usize = 64;

// Minimal error type that works in no_std
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusError {
    InvalidArg,
    NoSubscriberSlots,
    MsgTooLarge,
    NotReady,
    Platform(i32),
}

impl core::fmt::Display for BusError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            BusError::InvalidArg => f.write_str("invalid argument"),
            BusError::NoSubscriberSlots => f.write_str("no subscriber slots available"),
            BusError::MsgTooLarge => f.write_str("message too large"),
            BusError::NotReady => f.write_str("not ready"),
            BusError::Platform(code) => {
                // Best-effort human description for common errno values
                let desc = match *code {
                    #[cfg(feature = "linux-shm")]
                    x if x == libc::ENOENT => "(ENOENT) shared memory object not found",
                    #[cfg(feature = "linux-shm")]
                    x if x == libc::EEXIST => "(EEXIST) object already exists",
                    #[cfg(feature = "linux-shm")]
                    x if x == libc::EACCES => "(EACCES) permission denied",
                    #[cfg(feature = "linux-shm")]
                    x if x == libc::EINVAL => "(EINVAL) invalid size or flags",
                    #[cfg(feature = "linux-shm")]
                    x if x == libc::ENOMEM => "(ENOMEM) not enough memory for mapping",
                    _ => "platform error",
                };
                write!(f, "platform error: errno={} {}", code, desc)
            }
        }
    }
}

pub type Result<T> = core::result::Result<T, BusError>;

#[repr(C)]
#[derive(Clone, Copy)]
struct Descriptor {
    len: u32,
    _pad: u32,
}

#[repr(C, align(64))]
struct Slot {
    // Publish sequence for this slot. A message with global sequence `n` is
    // considered committed when `seq == n + 1`.
    seq: AtomicU64,
    desc: Descriptor,
}

#[repr(C, align(64))]
struct Header {
    // Highest contiguous committed sequence (advanced by producers post-commit)
    write_seq: AtomicU64,
    // Next sequence to claim (fetch_add by producers)
    claim_seq: AtomicU64,
    // Constant sizes
    desc_capacity: u64,
    slot_bytes: u64,
    // Active subscribers
    n_subs: AtomicU32,
    _pad0: [u8; 64 - 4],
    // Per-subscriber read sequence. Initialized to current write_seq at subscribe.
    sub_read_seq: [AtomicU64; MAX_SUBSCRIBERS],
}

impl Header {
    const fn layout_size(desc_capacity: usize) -> usize {
        // header + slots ring
        let header_size = core::mem::size_of::<Header>();
        let slots_size = desc_capacity * core::mem::size_of::<Slot>();
        align_up(header_size + slots_size, CACHELINE)
    }
}

const fn align_up(x: usize, a: usize) -> usize { (x + (a - 1)) & !(a - 1) }
const fn is_pow2(x: usize) -> bool { x != 0 && (x & (x - 1)) == 0 }

#[cfg(feature = "linux-shm")]
pub struct Region {
    fd: OwnedFd,
    ptr: NonNull<u8>,
    len: usize,
}

#[cfg(feature = "linux-shm")]
impl Region {
    // Create or open a named POSIX shared memory object and size it.
    pub fn create_named(name: &str, total_len: usize) -> Result<Self> {
        unsafe {
            let cname = CString::new(name).map_err(|_| BusError::InvalidArg)?;
            let fd = libc::shm_open(cname.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600);
            if fd < 0 { return Err(BusError::Platform(last_errno())); }
            if libc::ftruncate(fd, total_len as i64) != 0 { let e = last_errno(); let _ = libc::close(fd); return Err(BusError::Platform(e)); }
            let ptr = libc::mmap(core::ptr::null_mut(), total_len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0);
            if ptr == libc::MAP_FAILED { let e = last_errno(); let _ = libc::close(fd); return Err(BusError::Platform(e)); }
            Ok(Region { fd: OwnedFd::from_raw_fd(fd), ptr: NonNull::new_unchecked(ptr as *mut u8), len: total_len })
        }
    }

    pub fn open_named(name: &str, total_len: usize) -> Result<Self> {
        unsafe {
            let cname = CString::new(name).map_err(|_| BusError::InvalidArg)?;
            let fd = libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600);
            if fd < 0 { return Err(BusError::Platform(last_errno())); }
            let ptr = libc::mmap(core::ptr::null_mut(), total_len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0);
            if ptr == libc::MAP_FAILED { let e = last_errno(); let _ = libc::close(fd); return Err(BusError::Platform(e)); }
            Ok(Region { fd: OwnedFd::from_raw_fd(fd), ptr: NonNull::new_unchecked(ptr as *mut u8), len: total_len })
        }
    }

    fn as_mut_ptr(&self) -> *mut u8 { self.ptr.as_ptr() }
}

#[cfg(feature = "linux-shm")]
impl Drop for Region {
    fn drop(&mut self) {
        // Touch fd so the field is considered used (for robustness and to catch invalid FDs under sanitizers).
        let _ = self.fd.as_raw_fd();
        unsafe {
            let _ = libc::munmap(self.ptr.as_ptr() as *mut _, self.len);
            // `OwnedFd` will close automatically on drop via OwnedFd.
        }
    }
}

impl Drop for BusHandle {
    fn drop(&mut self) {
        // Touch region pointer to avoid false "never read" warnings and to make misuse more visible in Miri/sanitizers.
        let _ = self.region.as_mut_ptr();
    }
}

#[cfg(not(feature = "linux-shm"))]
pub struct Region {
    ptr: NonNull<u8>,
}

#[cfg(not(feature = "linux-shm"))]
impl Region {
    pub fn from_slice(buf: &mut [u8]) -> Result<Self> {
        if buf.is_empty() { return Err(BusError::InvalidArg); }
        Ok(Self { ptr: NonNull::new(buf.as_mut_ptr()).unwrap() })
    }
    fn as_mut_ptr(&self) -> *mut u8 { self.ptr.as_ptr() }
}

pub struct BusHandle {
    region: Region,
    header: *mut Header,
    slots_base: *mut Slot,
    payload_base: *mut u8,
    slot_bytes: usize,
}

unsafe impl Send for BusHandle {}
unsafe impl Sync for BusHandle {}

impl BusHandle {
    /// Create a new shared-memory bus region (Linux `shm_open` backend).
    #[cfg(feature = "linux-shm")]
    pub fn create(name: &str, desc_capacity: usize, slab_bytes: usize) -> Result<Self> {
        if !is_pow2(desc_capacity) || slab_bytes == 0 || slab_bytes % desc_capacity != 0 {
            return Err(BusError::InvalidArg);
        }
        let slot_bytes = slab_bytes / desc_capacity;
        let header_bytes = Header::layout_size(desc_capacity);
        let total = header_bytes + slab_bytes;
        let region = Region::create_named(name, total)?;
        unsafe { Self::init_in_region(region, desc_capacity, slot_bytes, header_bytes) }
    }

    #[cfg(feature = "linux-shm")]
    pub fn open(name: &str, desc_capacity: usize, slab_bytes: usize) -> Result<Self> {
        if !is_pow2(desc_capacity) || slab_bytes == 0 || slab_bytes % desc_capacity != 0 {
            return Err(BusError::InvalidArg);
        }
        let slot_bytes = slab_bytes / desc_capacity;
        let header_bytes = Header::layout_size(desc_capacity);
        let total = header_bytes + slab_bytes;
        let region = Region::open_named(name, total)?;
        unsafe { Self::map_in_region(region, desc_capacity, slot_bytes, header_bytes) }
    }

    #[cfg(not(feature = "linux-shm"))]
    pub fn from_slice(buf: &mut [u8], desc_capacity: usize) -> Result<Self> {
        if !is_pow2(desc_capacity) { return Err(BusError::InvalidArg); }
        let header_bytes = Header::layout_size(desc_capacity);
        if buf.len() <= header_bytes { return Err(BusError::InvalidArg); }
        // Choose slot_bytes as evenly dividing remainder
        let slab_bytes = buf.len() - header_bytes;
        if slab_bytes % desc_capacity != 0 { return Err(BusError::InvalidArg); }
        let slot_bytes = slab_bytes / desc_capacity;
        let region = Region::from_slice(buf)?;
        unsafe { Self::init_in_region(region, desc_capacity, slot_bytes, header_bytes) }
    }

    unsafe fn init_in_region(region: Region, desc_capacity: usize, slot_bytes: usize, header_bytes: usize) -> Result<Self> {
        let header = region.as_mut_ptr() as *mut Header;
        core::ptr::write_bytes(header as *mut u8, 0, size_of::<Header>());
        (*header).write_seq = AtomicU64::new(0);
        (*header).claim_seq = AtomicU64::new(0);
        (*header).desc_capacity = desc_capacity as u64;
        (*header).slot_bytes = slot_bytes as u64;
        (*header).n_subs = AtomicU32::new(0);
        let slots_base = (region.as_mut_ptr() as usize + size_of::<Header>()) as *mut Slot;
        // Initialize slots
        for i in 0..desc_capacity {
            let s = slots_base.add(i);
            (*s).seq = AtomicU64::new(0);
            (*s).desc = Descriptor { len: 0, _pad: 0 };
        }
        let payload_base = (region.as_mut_ptr() as usize + header_bytes) as *mut u8;
        Ok(BusHandle { region, header, slots_base, payload_base, slot_bytes })
    }

    #[cfg(feature = "linux-shm")]
    unsafe fn map_in_region(region: Region, desc_capacity: usize, slot_bytes: usize, header_bytes: usize) -> Result<Self> {
        let header = region.as_mut_ptr() as *mut Header;
        // Validate the on-disk/in-memory header matches requested sizes
        let hdr_desc = (*header).desc_capacity as usize;
        let hdr_slot = (*header).slot_bytes as usize;
        if hdr_desc != desc_capacity || hdr_slot != slot_bytes {
            // Mismatch: return a targeted error so the caller can hint the fix
            return Err(BusError::InvalidArg);
        }
        let slots_base = (region.as_mut_ptr() as usize + size_of::<Header>()) as *mut Slot;
        let payload_base = (region.as_mut_ptr() as usize + header_bytes) as *mut u8;
        Ok(BusHandle { region, header, slots_base, payload_base, slot_bytes })
    }

    pub fn subscribe(&self) -> Result<Subscriber<'_>> {
        unsafe {
            let n = (*self.header).n_subs.fetch_add(1, Ordering::AcqRel) as usize;
            if n >= MAX_SUBSCRIBERS { return Err(BusError::NoSubscriberSlots); }
            // Initialize read_seq to current write_seq so new subscriber starts at latest
            let start = (*self.header).write_seq.load(Ordering::Acquire);
            (*self.header).sub_read_seq[n].store(start, Ordering::Release);
            Ok(Subscriber { bus: self, sub_id: n as u32 })
        }
    }

    pub fn producer(&self) -> Producer<'_> { Producer { bus: self } }

    #[inline] pub fn slot_bytes(&self) -> usize { self.slot_bytes }
    #[inline] pub fn desc_capacity(&self) -> usize { (unsafe { (*self.header).desc_capacity }) as usize }
}

pub struct Producer<'a> { bus: &'a BusHandle }


impl<'a> Producer<'a> {
    /// Try to publish without blocking. Returns Ok(true) if published, Ok(false) if back-pressured.
    /// Errors: `MsgTooLarge` if payload doesn't fit the fixed per-slot size.
    pub fn try_publish(&self, payload: &[u8]) -> Result<bool> {
        if payload.is_empty() || payload.len() > self.bus.slot_bytes { return Err(BusError::MsgTooLarge); }
        unsafe {
            let h = &*self.bus.header;
            let cap = (*h).desc_capacity as u64;
            // One-shot space check and CAS-based claim to avoid permanent reservation on failure
            let tail = h.claim_seq.load(Ordering::Acquire);
            // Compute min read among active subscribers (or write_seq if none)
            let n = h.n_subs.load(Ordering::Acquire) as usize;
            let mut min_read = h.write_seq.load(Ordering::Acquire);
            for i in 0..n {
                let rs = h.sub_read_seq[i].load(Ordering::Acquire);
                if rs < min_read { min_read = rs; }
            }
            if tail >= min_read + cap { return Ok(false); }
            // Attempt to reserve exactly one slot
            let claim = match h.claim_seq.compare_exchange_weak(tail, tail + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => tail,
                Err(_) => return Ok(false),
            };
            let idx = (claim & (cap - 1)) as usize;
            let slot = self.bus.slots_base.add(idx);
            let dst = self.bus.payload_base.add(idx * self.bus.slot_bytes);
            core::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
            (*slot).desc = Descriptor { len: payload.len() as u32, _pad: 0 };
            fence(Ordering::Release);
            (*slot).seq.store(claim + 1, Ordering::Release);
            // Attempt to advance contiguous write_seq (best-effort)
            loop {
                let w = h.write_seq.load(Ordering::Acquire);
                let w_idx = (w & (cap - 1)) as usize;
                let w_slot = self.bus.slots_base.add(w_idx);
                let committed = (*w_slot).seq.load(Ordering::Acquire);
                if committed == w + 1 { let _ = h.write_seq.compare_exchange(w, w + 1, Ordering::AcqRel, Ordering::Acquire); } else { break; }
            }
            Ok(true)
        }
    }

    /// Publish a payload; blocks (spins/yields on std) on backpressure until the slot is available.
    /// Returns `MsgTooLarge` if `payload.len() > slot_bytes` or zero.
    pub fn publish(&self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() || payload.len() > self.bus.slot_bytes { return Err(BusError::MsgTooLarge); }
        unsafe {
            let h = &*self.bus.header;
            let cap = (*h).desc_capacity as u64;
            // Claim a global sequence number
            let claim = h.claim_seq.fetch_add(1, Ordering::AcqRel);
            // Backpressure: ensure we don't overrun slowest subscriber
            loop {
                let n = h.n_subs.load(Ordering::Acquire) as usize;
                let mut min_read = h.write_seq.load(Ordering::Acquire);
                for i in 0..n {
                    let rs = h.sub_read_seq[i].load(Ordering::Acquire);
                    if rs < min_read { min_read = rs; }
                }
                if claim < min_read + cap { break; }
                // On std platforms we can yield; on bare we just spin
                #[cfg(feature = "std")]
                { std::thread::yield_now(); }
            }

            let idx = (claim & (cap - 1)) as usize;
            let slot = self.bus.slots_base.add(idx);
            let dst = self.bus.payload_base.add(idx * self.bus.slot_bytes);
            core::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
            // Set desc length
            (*slot).desc = Descriptor { len: payload.len() as u32, _pad: 0 };
            // Publish commit for this sequence
            fence(Ordering::Release);
            (*slot).seq.store(claim + 1, Ordering::Release);

            // Try to advance global contiguous write_seq
            loop {
                let w = h.write_seq.load(Ordering::Acquire);
                let w_idx = (w & (cap - 1)) as usize;
                let w_slot = self.bus.slots_base.add(w_idx);
                let committed = (*w_slot).seq.load(Ordering::Acquire);
                if committed == w + 1 { let _ = h.write_seq.compare_exchange(w, w + 1, Ordering::AcqRel, Ordering::Acquire); } else { break; }
            }
            Ok(())
        }
    }
}

pub struct Subscriber<'a> {
    bus: &'a BusHandle,
    sub_id: u32,
}

/// Relay one message from `src` to `dst` using `scratch` buffer.
/// Returns Ok(Some(n)) if a message of length `n` was forwarded; Ok(None) if source had no message.
pub fn relay_once<'a>(src: &Subscriber<'a>, dst: &Producer<'a>, scratch: &mut [u8]) -> Result<Option<usize>> {
    if let Some(n) = src.try_recv(scratch)? {
        // If destination cannot accept now, use blocking publish to preserve ordering
        // Truncate is not allowed; if message doesn't fit, drop and signal error
        if n > dst.bus.slot_bytes() { return Err(BusError::MsgTooLarge); }
        dst.publish(&scratch[..n])?;
        return Ok(Some(n));
    }
    Ok(None)
}

impl<'a> Subscriber<'a> {
    /// Try to receive into provided buffer; returns Ok(Some(n)) if a message was copied.
    pub fn try_recv(&self, out: &mut [u8]) -> Result<Option<usize>> {
        unsafe {
            let h = &*self.bus.header;
            let seq = h.sub_read_seq[self.sub_id as usize].load(Ordering::Acquire);
            let idx = (seq & ((*h).desc_capacity - 1)) as usize;
            let slot = self.bus.slots_base.add(idx);
            // Check if committed
            let committed = (*slot).seq.load(Ordering::Acquire);
            if committed != seq + 1 { return Ok(None); }
            let len = (*slot).desc.len as usize;
            if len > out.len() { return Err(BusError::MsgTooLarge); }
            let src = self.bus.payload_base.add(idx * self.bus.slot_bytes);
            core::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), len);
            // Advance reader sequence
            h.sub_read_seq[self.sub_id as usize].store(seq + 1, Ordering::Release);
            Ok(Some(len))
        }
    }

    /// Receive one message, blocking up to `timeout`. Returns `Ok(n)` on success, or `Err(NotReady)` on timeout.
    #[cfg(feature = "std")]
    pub fn recv_blocking(&self, out: &mut [u8], timeout: std::time::Duration) -> Result<usize> {
        let start = std::time::Instant::now();
        loop {
            if let Some(n) = self.try_recv(out)? { return Ok(n); }
            if start.elapsed() >= timeout { return Err(BusError::NotReady); }
            std::thread::yield_now();
        }
    }
}
