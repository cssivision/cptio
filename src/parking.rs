use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::task::Waker;
use std::time::Duration;

use crate::waker_fn::waker_fn;

use parking_lot::{Condvar, Mutex};

pub fn pair() -> (Parker, Unparker) {
    let p = Parker::new();
    let u = p.unparker();
    (p, u)
}

pub fn parker_and_waker() -> (Parker, Waker) {
    let parker = Parker::new();
    let unparker = parker.unparker();
    let waker = waker_fn(move || {
        let _ = unparker.unpark();
    });
    (parker, waker)
}

impl Default for Parker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Parker {
    unparker: Unparker,
}

impl Parker {
    pub fn new() -> Parker {
        Parker {
            unparker: Unparker {
                inner: Arc::new(Inner {
                    state: AtomicUsize::new(0),
                    lock: Mutex::new(()),
                    cvar: Condvar::new(),
                }),
            },
        }
    }

    pub fn park(&self) -> bool {
        self.unparker.inner.park(None)
    }

    pub fn park_timeout(&self, timeout: Option<Duration>) -> bool {
        self.unparker.inner.park(timeout)
    }

    pub fn unparker(&self) -> Unparker {
        self.unparker.clone()
    }
}

impl Unparker {
    pub fn unpark(&self) -> bool {
        self.inner.unpark()
    }
}

impl Clone for Unparker {
    fn clone(&self) -> Unparker {
        Unparker {
            inner: self.inner.clone(),
        }
    }
}

pub struct Unparker {
    inner: Arc<Inner>,
}

const EMPTY: usize = 0;
const PARKED: usize = 1;
const NOTIFIED: usize = 2;

struct Inner {
    state: AtomicUsize,
    lock: Mutex<()>,
    cvar: Condvar,
}

impl Inner {
    fn park(&self, timeout: Option<Duration>) -> bool {
        if self
            .state
            .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
            .is_ok()
        {
            return true;
        }

        if let Some(d) = timeout {
            if d == Duration::from_secs(0) {
                return false;
            }
        }

        let mut m = self.lock.lock();

        match self.state.compare_exchange(EMPTY, PARKED, SeqCst, SeqCst) {
            Ok(_) => {}
            Err(NOTIFIED) => {
                let old = self.state.swap(EMPTY, SeqCst);
                assert_eq!(old, NOTIFIED, "park state changed unexpectedly");
                return true;
            }
            Err(_) => panic!("invalid park state"),
        }

        match timeout {
            None => loop {
                self.cvar.wait(&mut m);

                if self
                    .state
                    .compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst)
                    .is_ok()
                {
                    return true; // got a notification
                }
            },
            Some(d) => {
                // Wait with a timeout, and if we spuriously wake up or otherwise wake up from a
                // notification we just want to unconditionally set `state` back to `EMPTY`, either
                // consuming a notification or un-flagging ourselves as parked.
                let _result = self.cvar.wait_for(&mut m, d);

                match self.state.swap(EMPTY, SeqCst) {
                    NOTIFIED => true, // got a notification
                    PARKED => false,  // no notification
                    n => panic!("inconsistent park_timeout state: {}", n),
                }
            }
        }
    }

    fn unpark(&self) -> bool {
        // To ensure the unparked thread will observe any writes we made before this call, we must
        // perform a release operation that `park` can synchronize with. To do that we must write
        // `NOTIFIED` even if `state` is already `NOTIFIED`. That is why this must be a swap rather
        // than a compare-and-swap that returns if it reads `NOTIFIED` on failure.
        match self.state.swap(NOTIFIED, SeqCst) {
            EMPTY => return true,     // no one was waiting
            NOTIFIED => return false, // already unparked
            PARKED => {}              // gotta go wake someone up
            _ => panic!("inconsistent state in unpark"),
        }

        // There is a period between when the parked thread sets `state` to `PARKED` (or last
        // checked `state` in the case of a spurious wakeup) and when it actually waits on `cvar`.
        // If we were to notify during this period it would be ignored and then when the parked
        // thread went to sleep it would never wake up. Fortunately, it has `lock` locked at this
        // stage so we can acquire `lock` to wait until it is ready to receive the notification.
        //
        // Releasing `lock` before the call to `notify_one` means that when the parked thread wakes
        // it doesn't get woken only to have to wait for us to release `lock`.
        drop(self.lock.lock());
        self.cvar.notify_one();

        true
    }
}