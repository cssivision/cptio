use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use io_uring::squeue::Entry;
use io_uring::{cqueue, opcode, IoUring};
use scoped_tls::scoped_thread_local;
use slab::Slab;

use crate::buffer::{Buf, BufRing, Builder};

mod op;

pub(crate) use op::*;

pub const BUF_BGID: u16 = 666;
const DEFAULT_RING_ENTRIES: u16 = 128;
const DEFAULT_BUF_CNT: u16 = 128;
const DEFAULT_BUF_LEN: usize = 4096;

scoped_thread_local!(static CURRENT: Driver);

pub(crate) struct Driver {
    inner: Rc<RefCell<Inner>>,
}

impl Clone for Driver {
    fn clone(&self) -> Self {
        Driver {
            inner: self.inner.clone(),
        }
    }
}

struct Inner {
    buf_ring: BufRing,
    ring: IoUring,
    ops: Slab<Lifecycle>,
}

impl Inner {
    fn new() -> io::Result<Inner> {
        let ring = IoUring::new(256)?;
        let buf_ring = Builder::new(BUF_BGID)
            .ring_entries(DEFAULT_RING_ENTRIES)
            .buf_cnt(DEFAULT_BUF_CNT)
            .buf_len(DEFAULT_BUF_LEN)
            .build()?;
        let mut inner = Inner {
            ring,
            ops: Slab::with_capacity(256),
            buf_ring,
        };
        inner.register_buf_ring()?;
        Ok(inner)
    }

    fn register_buf_ring(&mut self) -> io::Result<()> {
        // Safety: The ring, represented by the ring_start and the ring_entries remains valid until
        // it is unregistered. The backing store is an AnonymousMmap which remains valid until it
        // is dropped which in this case, is when Self is dropped.
        let res = unsafe {
            self.ring.submitter().register_buf_ring(
                self.buf_ring.as_ptr() as _,
                self.buf_ring.ring_entries(),
                self.buf_ring.bgid(),
            )
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    // using buf_ring requires kernel 5.19 or greater.
                    return Err(io::Error::new(
                            io::ErrorKind::Other, format!(
                                "buf_ring.register returned {}, most likely indicating this kernel is not 5.19+", e),
                            ));
                }
                Some(libc::EEXIST) => {
                    // Registering a duplicate bgid is not allowed. There is an `unregister`
                    // operations that can remove the first, but care must be taken that there
                    // are no outstanding operations that will still return a buffer from that
                    // one.
                    return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "buf_ring.register returned `{}`, indicating the attempted buffer group id {} was already registered",
                            e,
                            self.buf_ring.bgid()),
                        ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "buf_ring.register returned `{}` for group id {}",
                            e,
                            self.buf_ring.bgid()
                        ),
                    ));
                }
            }
        };
        res
    }

    fn submit(&mut self, sqe: Entry) -> io::Result<()> {
        if self.ring.submission().is_full() {
            self.ring.submit()?;
        }
        self.ring.submission().sync();
        unsafe {
            self.ring.submission().push(&sqe).expect("push entry fail");
        }
        self.ring.submit()?;
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        if let Err(e) = self.ring.submit_and_wait(1) {
            if e.raw_os_error() == Some(libc::EBUSY) {
                return Ok(());
            }
            if e.kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(e);
        }

        let mut cq = self.ring.completion();
        cq.sync();
        for cqe in cq {
            if cqe.user_data() == u64::MAX {
                continue;
            }
            let index = cqe.user_data() as _;
            let op = &mut self.ops[index];
            if op.complete(cqe, &self.buf_ring) {
                self.ops.remove(index);
            }
        }
        Ok(())
    }

    fn submit_op<T>(&mut self, driver: Driver, op: T, sqe: Entry) -> io::Result<Op<T>> {
        let key = self.ops.insert(Lifecycle::Submitted);
        let sqe = sqe.user_data(key as u64);
        self.submit(sqe)?;
        Ok(Op {
            driver,
            op: Some(op),
            key,
        })
    }
}

impl Driver {
    pub(crate) fn new() -> io::Result<Driver> {
        Ok(Driver {
            inner: Rc::new(RefCell::new(Inner::new()?)),
        })
    }

    pub(crate) fn wait(&self) -> io::Result<()> {
        self.inner.borrow_mut().wait()
    }

    pub(crate) fn with<T>(&self, f: impl FnOnce() -> T) -> T {
        CURRENT.set(self, f)
    }

    pub(crate) fn submit<T>(&self, op: T, sqe: Entry) -> io::Result<Op<T>> {
        self.inner.borrow_mut().submit_op(self.clone(), op, sqe)
    }
}

enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,
    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),
    /// The operation has completed.
    Completed(CqeResult),
    /// The operations list.
    CompletionList(Vec<CqeResult>),
    /// Ignored
    #[allow(dead_code)]
    Ignored(Box<dyn Any>),
}

impl Lifecycle {
    fn complete(&mut self, entry: cqueue::Entry, buf_ring: &BufRing) -> bool {
        let mut cqe: CqeResult = entry.into();
        if let Some(bid) = cqueue::buffer_select(cqe.flags) {
            match cqe.result {
                Ok(len) => {
                    cqe.buf = Some(buf_ring.get_buf(len as usize, bid));
                }
                Err(_) => {
                    buf_ring.drop_buf(bid);
                }
            }
        }

        match mem::replace(self, Lifecycle::Submitted) {
            s @ Lifecycle::Submitted | s @ Lifecycle::Waiting(..) => {
                if cqueue::more(cqe.flags) {
                    *self = Lifecycle::CompletionList(vec![cqe]);
                } else {
                    *self = Lifecycle::Completed(cqe);
                }
                if let Lifecycle::Waiting(waker) = s {
                    waker.wake();
                }
                false
            }
            s @ Lifecycle::Ignored(..) => {
                if cqueue::more(cqe.flags) {
                    *self = s;
                    false
                } else {
                    true
                }
            }
            Lifecycle::CompletionList(mut list) => {
                list.push(cqe);
                *self = Lifecycle::CompletionList(list);
                false
            }
            Lifecycle::Completed(..) => unreachable!("invalid lifecycle"),
        }
    }
}

pub(crate) trait Completable {
    type Output;
    /// `complete` will be called for cqe's do not have the `more` flag set
    fn complete(self, cqe: CqeResult) -> Self::Output;
    /// Update will be called for cqe's which have the `more` flag set.
    /// The Op should update any internal state as required.
    fn update(&mut self, _cqe: CqeResult) {}
}

pub(crate) struct Op<T: 'static> {
    pub driver: Driver,
    pub op: Option<T>,
    pub key: usize,
}

impl<T> Op<T> {
    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.op.as_mut().unwrap()
    }

    pub(crate) fn submit(op: T, entry: Entry) -> io::Result<Op<T>> {
        CURRENT.with(|driver| driver.submit(op, entry))
    }

    pub(crate) fn reset(&self, waker: Waker) {
        let mut inner = self.driver.inner.borrow_mut();
        if let Some(lifecycle) = inner.ops.get_mut(self.key) {
            *lifecycle = Lifecycle::Waiting(waker);
        }
    }

    fn poll2(&mut self, cx: &mut Context) -> Poll<T::Output>
    where
        T: Completable,
    {
        let mut inner = self.driver.inner.borrow_mut();
        let lifecycle = inner.ops.get_mut(self.key).expect("invalid key");

        match mem::replace(lifecycle, Lifecycle::Submitted) {
            Lifecycle::Submitted => {
                *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                Poll::Pending
            }
            Lifecycle::Waiting(waker) => {
                if !waker.will_wake(cx.waker()) {
                    *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                } else {
                    *lifecycle = Lifecycle::Waiting(waker);
                }
                Poll::Pending
            }
            Lifecycle::Completed(cqe) => {
                inner.ops.remove(self.key);
                Poll::Ready(self.op.take().unwrap().complete(cqe))
            }
            Lifecycle::CompletionList(list) => {
                let data = self.op.as_mut().unwrap();
                let mut status = None;
                let mut updated = false;
                for cqe in list.into_iter() {
                    if cqueue::more(cqe.flags) {
                        updated = true;
                        data.update(cqe);
                    } else {
                        status = Some(cqe);
                        break;
                    }
                }
                if updated {
                    // because we update internal state, wake and rerun the task.
                    cx.waker().wake_by_ref();
                }
                match status {
                    None => {
                        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
                    }
                    Some(cqe) => {
                        *lifecycle = Lifecycle::Completed(cqe);
                    }
                }
                Poll::Pending
            }
            Lifecycle::Ignored(..) => unreachable!(),
        }
    }
}

impl<T> Drop for Op<T> {
    fn drop(&mut self) {
        let mut inner = self.driver.inner.borrow_mut();
        let lifecycle = match inner.ops.get_mut(self.key) {
            Some(v) => v,
            None => return,
        };

        let mut finished = true;
        match lifecycle {
            Lifecycle::Submitted | Lifecycle::Waiting(_) => {
                finished = false;
                *lifecycle = Lifecycle::Ignored(Box::new(self.op.take()));
            }
            Lifecycle::Completed(..) => {
                inner.ops.remove(self.key);
            }
            Lifecycle::CompletionList(list) => {
                let more = if !list.is_empty() {
                    cqueue::more(list.last().unwrap().flags)
                } else {
                    false
                };
                if more {
                    finished = false;
                    *lifecycle = Lifecycle::Ignored(Box::new(self.op.take()));
                } else {
                    inner.ops.remove(self.key);
                }
            }
            Lifecycle::Ignored(..) => unreachable!(),
        }
        if !finished {
            let sqe = opcode::AsyncCancel::new(self.key as u64)
                .build()
                .user_data(u64::MAX);
            let _ = inner.submit(sqe);
        }
    }
}

impl<T> Future for Op<T>
where
    T: Unpin + Completable,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.poll2(cx)
    }
}

#[allow(dead_code)]
pub(crate) struct CqeResult {
    pub result: io::Result<u32>,
    pub flags: u32,
    pub buf: Option<Buf>,
}

impl From<cqueue::Entry> for CqeResult {
    fn from(cqe: cqueue::Entry) -> Self {
        let res = cqe.result();
        let flags = cqe.flags();
        let result = if res >= 0 {
            Ok(res as u32)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        };
        CqeResult {
            result,
            flags,
            buf: None,
        }
    }
}
