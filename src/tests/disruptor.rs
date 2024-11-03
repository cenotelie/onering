/*******************************************************************************
 * Copyright (c) 2024 Cénotélie Opérations SAS (cenotelie.fr)
 ******************************************************************************/

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::disruptor::Builder;
use crate::pipeline::{new_pipeline, PipelineElement};
use crate::tests::{SCALE_MSG_COUNT, SCALE_QUEUE_SIZE};
use crate::Sender;

#[test]
fn disruptor_1p_1c() {
    let cell = AtomicUsize::new(0);

    let mut disruptor = Builder::default()
        .mode_single()
        .queue_size(SCALE_QUEUE_SIZE)
        .on_thread_end(|_, _| std::time::Instant::now())
        .pipeline(new_pipeline::<usize>().then(move |value| {
            let expected = cell.fetch_add(1, Ordering::Relaxed);
            assert_eq!(expected, value);
        }))
        .start();

    let start = std::time::Instant::now();
    let sender = disruptor.sender();
    for i in 0..SCALE_MSG_COUNT {
        while sender.try_send(i).is_err() {}
    }

    let end = disruptor.shutdown()[0];

    let duration = end.duration_since(start).as_secs_f64();
    #[allow(clippy::cast_precision_loss)]
    let throughput = (SCALE_MSG_COUNT as f64) / duration;
    println!("disruptor_1p_1c queue_size={SCALE_QUEUE_SIZE}, items={SCALE_MSG_COUNT}, throughput={throughput}");
}
