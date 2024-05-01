use std::cell::Cell;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::task::{Context, Poll};

use crate::driver::Driver;
use crate::local_executor;
use crate::waker_fn::waker_fn;

pub(crate) struct Runtime {
    driver: Driver,
}

impl Runtime {
    pub fn new() -> io::Result<Runtime> {
        Ok(Runtime {
            driver: Driver::new()?,
        })
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        thread_local! {
            static NOTIFIED: Cell<bool> = const { Cell::new(false) };
        }
        let mut future = pin!(future);
        let waker = waker_fn(|| NOTIFIED.with(|notified| notified.set(true)));
        let cx = &mut Context::from_waker(&waker);

        self.driver.with(|| loop {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return output;
            }
            if local_executor::tick() {
                continue;
            }
            if !NOTIFIED.with(Cell::get) {
                self.driver.wait().expect("driver wait error");
            }
            NOTIFIED.with(|notified| notified.set(false));
        })
    }
}
