use std::io;
use std::ops::Sub;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::driver::{self, Action};

pub mod delay;
pub mod interval;
pub mod timeout;

pub use delay::{delay_for, delay_until, Delay};
pub use interval::{interval, interval_at, Interval};
pub use timeout::{timeout, timeout_at, Timeout};

enum State {
    Idle,
    Waiting(Action<driver::Timeout>),
}

pub struct Timer {
    deadline: Instant,
    state: State,
    waker: Option<Waker>,
}

impl Timer {
    pub fn new(deadline: Instant) -> Timer {
        Timer {
            deadline,
            state: State::Idle,
            waker: None,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn is_elapsed(&self) -> bool {
        self.deadline < Instant::now()
    }

    pub fn reset(&mut self, when: Instant) {
        self.state = State::Idle;
        self.deadline = when;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn poll_timeout(&mut self, cx: &mut Context) -> Poll<io::Result<Instant>> {
        loop {
            match &mut self.state {
                State::Idle => {
                    let duration = self.deadline.sub(Instant::now());
                    let action = Action::timeout(duration.as_secs(), duration.subsec_nanos())?;
                    self.state = State::Waiting(action);
                }
                State::Waiting(action) => {
                    match &self.waker {
                        Some(waker) => {
                            if !waker.will_wake(cx.waker()) {
                                self.waker = Some(cx.waker().clone());
                            }
                        }
                        None => {
                            self.waker = Some(cx.waker().clone());
                        }
                    }
                    ready!(Pin::new(action).poll_timeout(cx))?;
                    return Poll::Ready(Ok(self.deadline));
                }
            }
        }
    }
}
