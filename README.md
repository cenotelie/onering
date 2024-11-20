<div align="center">
  <h1>Onering</h1>
    <strong>High throughput synchronous channels</strong>
  </a>
  <br>
  <br>
</div>

High throughput synchronous queue and channels.
The implementation of the queue is freely inspired by the [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor).
As in a typical disruptor-fashion, consumers typically receive all items pushed onto the queue.
The implementation is then better suited for dispatching all items to all consumers.

Therefore, the queue provided here do not allow sending the ownership of queued items onto other threads.
Instead, receivers (consumers) will only see immutable references to the items.

## Example

```rust
use std::sync::Arc;
use onering::errors::TryRecvError;
use onering::queue::{Consumer, ConsumerMode, RingBuffer, SingleProducer};

let ring = Arc::new(RingBuffer::<usize, _>::new_single_producer(256));
let mut consumers = (0..5)
    .map(|_| Consumer::new(ring.clone(), ConsumerMode::Blocking))
    .collect::<Vec<_>>();
let mut producer = SingleProducer::new(ring);

let consumer_threads = (0..5)
    .map(|_| {
        let mut consumer = consumers.pop().unwrap();
        std::thread::spawn({
            move || {
                let mut count = 0;
                loop {
                    match consumer.try_recv() {
                        Ok(items) => {
                            for _item in items {
                                // handle item
                            }
                        },
                        Err(TryRecvError::Disconnected) => {
                            break;
                        }
                        Err(_) => {/* retry */}
                    }
                }
            }
        })
    })
    .collect::<Vec<_>>();

for item in 0..1000 {
    while producer.try_push(item).is_err() {}
}
drop(producer); // so that `TryRecvError::Disconnected` is raised

for consumer in consumer_threads {
    consumer.join().unwrap();
}
```

## Contributing

Contributions are welcome!

Open a ticket, ask a question or submit a pull request.


## License

This project is licensed under the [MIT license](LICENSE).
