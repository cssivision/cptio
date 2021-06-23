use std::cell::RefCell;
use std::io;
use std::mem;
use std::panic;
use std::rc::Rc;
use std::task::Waker;

use io_uring::opcode::ProvideBuffers;
use io_uring::squeue::Entry;
use io_uring::{cqueue, IoUring};
use scoped_tls::scoped_thread_local;
use slab::Slab;

pub(crate) mod accept;
pub(crate) mod action;
pub(crate) mod buffers;
pub(crate) mod connect;
pub(crate) mod read;
pub(crate) mod stream;
pub(crate) mod timeout;
pub(crate) mod write;

pub use action::Action;
use buffers::Buffers;
pub use read::Read;
pub use stream::Stream;
pub use timeout::Timeout;
pub use write::Write;

pub const DEFAULT_BUFFER_SIZE: usize = 2048;
const DEFAULT_BUFFER_NUM: usize = 1024;

scoped_thread_local!(static CURRENT: Driver);

pub struct Driver {
    pub inner: Rc<RefCell<Inner>>,
}

impl Clone for Driver {
    fn clone(&self) -> Self {
        Driver {
            inner: self.inner.clone(),
        }
    }
}

pub struct Inner {
    ring: IoUring,
    actions: Slab<State>,
    buffers: Buffers,
}

impl Driver {
    pub fn new() -> io::Result<Driver> {
        let mut ring = IoUring::new(256)?;

        // check if IORING_FEAT_FAST_POLL is supported
        if !ring.params().is_feature_fast_poll() {
            panic!("IORING_FEAT_FAST_POLL not supported");
        }

        // check if buffer selection is supported
        let mut probe = io_uring::Probe::new();
        ring.submitter().register_probe(&mut probe).unwrap();
        if !probe.is_supported(ProvideBuffers::CODE) {
            panic!("buffer selection not supported");
        }
        let buffers = Buffers::new(DEFAULT_BUFFER_NUM, DEFAULT_BUFFER_SIZE);
        provide_buffers(&mut ring, &buffers)?;

        let driver = Driver {
            inner: Rc::new(RefCell::new(Inner {
                ring,
                actions: Slab::new(),
                buffers,
            })),
        };
        Ok(driver)
    }

    pub fn wait(&self) -> io::Result<()> {
        let inner = &mut *self.inner.borrow_mut();
        let ring = &mut inner.ring;

        if let Err(e) = ring.submit_and_wait(1) {
            if e.raw_os_error() == Some(libc::EBUSY) {
                return Ok(());
            }
            if e.kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(e);
        }

        let mut cq = ring.completion();
        cq.sync();
        for cqe in cq {
            let key = cqe.user_data();
            if key == u64::MAX {
                continue;
            }
            let action = &mut inner.actions[key as usize];
            action.complete(cqe);
        }

        Ok(())
    }

    pub fn with<T>(&self, f: impl FnOnce() -> T) -> T {
        CURRENT.set(self, f)
    }

    pub fn submit(&self, sqe: Entry) -> io::Result<u64> {
        let mut inner = self.inner.borrow_mut();
        let inner = &mut *inner;
        let key = inner.actions.insert(State::Submitted) as u64;

        let ring = &mut inner.ring;
        if ring.submission().is_full() {
            ring.submit()?;
            ring.submission().sync();
        }

        let sqe = sqe.user_data(key);
        unsafe {
            ring.submission().push(&sqe).expect("push entry fail");
        }
        ring.submit()?;
        Ok(key)
    }
}

fn provide_buffers(ring: &mut IoUring, buffers: &Buffers) -> io::Result<()> {
    let entry = ProvideBuffers::new(buffers.mem, buffers.size as i32, buffers.num as u16, 0, 0)
        .build()
        .user_data(0);
    unsafe {
        ring.submission().push(&entry).expect("push entry fail");
    }
    ring.submit_and_wait(1)?;
    for cqe in ring.completion() {
        let ret = cqe.result();
        if cqe.user_data() != 0 {
            panic!("provide_buffers user_data error");
        }
        if ret < 0 {
            panic!("provide_buffers submit error, ret: {}", ret);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum State {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,
    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),
    /// The operation has completed.
    Completed(cqueue::Entry),
}

impl State {
    pub fn complete(&mut self, cqe: cqueue::Entry) {
        match mem::replace(self, State::Submitted) {
            State::Submitted => {
                *self = State::Completed(cqe);
            }
            State::Waiting(waker) => {
                *self = State::Completed(cqe);
                waker.wake();
            }
            State::Completed(_) => unreachable!("invalid operation state"),
        };
    }
}
