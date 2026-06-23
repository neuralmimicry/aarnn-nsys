# aarnn-nsys — Wiki Home

**aarnn-nsys** is a tiny, production-grade, zero-allocation pub/sub message bus designed for ultra-low-latency single-host neuromorphic and real-time systems work. It delivers multi-producer, multi-subscriber fan-out using a lock-free ring over a shared memory region, and supports bare-metal `no_std` environments.

> ☕ [Support NeuralMimicry on Crowdfunder](https://www.crowdfunder.co.uk/p/qr/aWggxwPW?utm_campaign=sharemodal&utm_medium=referral&utm_source=shortlink)

---

## Quick navigation

| Page | Description |
|---|---|
| [Getting Started](Getting-Started) | Add as a dependency, first producer/subscriber |
| [API Reference](API-Reference) | Bus, producer, subscriber, relay API |
| [Bare-Metal Usage](Bare-Metal-Usage) | `no_std` in-memory backend |
| [Examples](Examples) | Multi-producer, pipeline, relay patterns |
| [Contributing](Contributing) | Running tests, PR guidelines |

---

## Key properties

- **0 allocations** on the hot path
- Cacheline-friendly indices only cross cores
- Linux shared-memory backend (default)
- `no_std`/bare-metal in-memory backend
- Concatenation (relay) between two bus instances
- CLI demo and examples included

## Quick example

```rust
use aarnn_nsys::Bus;

let bus = Bus::new(/* capacity */ 256)?;
let mut producer = bus.producer()?;
let mut subscriber = bus.subscriber()?;

producer.send(b"hello neuromorphic")?;
let msg = subscriber.recv()?;
```

## Related

- [raspi-bare-metal](https://github.com/neuralmimicry/raspi-bare-metal) — bare-metal AArch64 demo on Raspberry Pi 4 using this crate

## Get involved

- 🐛 [Report a bug or request a feature](https://github.com/neuralmimicry/aarnn-nsys/issues)
- 💬 [Join the discussion](https://github.com/neuralmimicry/aarnn-nsys/discussions)
- 📧 Direct support: [info@neuralmimicry.ai](mailto:info@neuralmimicry.ai) · **£1,000/day + VAT**
- 🌐 [neuralmimicry.ai](https://neuralmimicry.ai)
