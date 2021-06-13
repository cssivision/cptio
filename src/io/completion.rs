use std::io;
use std::panic;
use std::sync::{Arc, Mutex};
use std::thread;

use super::action::Action;
use crate::other;

use io_uring::{opcode::ProvideBuffers, squeue::Entry, IoUring};
use slab::Slab;

pub const MAX_MSG_LEN: i32 = 2048;
pub const BUFFERS_COUNT: u16 = 4096;
pub const GROUP_ID: u16 = 1028;

pub struct Completion {
    ring: IoUring,
    actions: Mutex<Slab<Arc<Action>>>,
    buffers: Vec<Vec<u8>>,
}

impl Completion {
    pub fn get_data(&self, bid: usize, len: usize) -> Option<Vec<u8>> {
        self.buffers
            .get(bid)
            .map(|v| v[..v.len().max(len)].to_vec())
    }

    pub fn get() -> &'static Completion {
        let ring = IoUring::new(256).expect("init io uring fail");

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

        let mut c = Completion {
            ring,
            actions: Mutex::new(Slab::new()),
            buffers: vec![vec![0u8; MAX_MSG_LEN as usize]; BUFFERS_COUNT as usize],
        };

        c.setup().unwrap();
        c
    }

    fn setup(&mut self) -> io::Result<()> {
        let buffers = self.buffers.as_mut_ptr() as *mut u8;
        let entry = ProvideBuffers::new(buffers, MAX_MSG_LEN, BUFFERS_COUNT, GROUP_ID, 0).build();

        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| other("push provide_buffers entry fail"))?;
        }

        self.ring.submit_and_wait(1)?;
        for cqe in self.ring.completion() {
            let ret = cqe.result();
            if ret < 0 {
                return Err(other(&format!(
                    "provide_buffers submit error, ret: {}",
                    ret
                )));
            }
        }

        Ok(())
    }

    fn wait(&self) -> io::Result<()> {
        self.ring.submit_and_wait(1)?;
        let mut wakers = Vec::new();
        let actions = self.actions.lock().unwrap();

        for cqe in self.ring.completion() {
            let key = cqe.user_data() as usize;
            if let Some(action) = actions.get(key) {
                action.trigger(&mut wakers, cqe);
            }
        }

        for waker in wakers {
            waker.wake();
        }

        Ok(())
    }

    pub fn submit(&self, sqe: Entry) -> io::Result<()> {
        let mut sq = self.ring.submission();

        if sq.is_full() {
            self.ring.submit()?;
        }

        unsafe {
            sq.push(&sqe).map_err(|_| other("sq push fail"))?;
        }

        self.ring.submit()?;

        Ok(())
    }

    pub fn insert(&self, action: Arc<Action>) -> usize {
        let mut actions = self.actions.lock().unwrap();
        actions.insert(action)
    }

    pub fn remove(&self, key: usize) {
        let mut actions = self.actions.lock().unwrap();
        actions.remove(key);
    }
}