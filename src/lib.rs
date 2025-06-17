pub mod message;
use message::{Message, Token};

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};
// use zmq;

type ProcessId = usize;
type RequestNumber = u64;

pub struct Monitor {
    id: ProcessId,
    total: usize,
    addrs: Vec<String>, // index by process ID

    rn: Arc<Mutex<Vec<RequestNumber>>>,
    token: Arc<Mutex<Option<Token>>>,

    local_m: Mutex<()>,
    local_c: Condvar,

    ctx: Arc<zmq::Context>,
    dealers: Vec<zmq::Socket>, // one dealer PER peer
    running: Arc<AtomicBool>,
}

impl Monitor {
    pub fn new(
        id: ProcessId,
        total: usize,
        initial_owner: Option<ProcessId>,
        host: &str,
        base_port: u16,
    ) -> Arc<Self> {
        let ctx = Arc::new(zmq::Context::new());
        let mut addrs = Vec::with_capacity(total);
        for pid in 0..total {
            addrs.push(format!("tcp://{}:{}", host, base_port + pid as u16));
        }

        let mut dealers = Vec::with_capacity(total);
        for pid in 0..total {
            let sock = ctx.socket(zmq::DEALER).unwrap();
            sock.set_identity(id.to_string().as_bytes()).unwrap();
            // Every dealer connects to _every_ peer (including itself is OK; we'll skip sending to self later)
            sock.connect(&addrs[pid]).unwrap();
            dealers.push(sock);
        }

        let initial_tok = if initial_owner == Some(id) {
            Some(Token {
                ln: vec![0; total],
                queue: VecDeque::new(),
            })
        } else {
            None
        };

        let mon = Arc::new(Self {
            id,
            total,
            addrs,
            rn: Arc::new(Mutex::new(vec![0; total])),
            token: Arc::new(Mutex::new(initial_tok)),
            local_m: Mutex::new(()),
            local_c: Condvar::new(),
            ctx: ctx.clone(),
            dealers,
            running: Arc::new(AtomicBool::new(true)),
        });

        // Startup barrier to ensure ROUTER binding before returning
        let barrier = Arc::new(std::sync::Barrier::new(2));
        {
            let me = mon.clone();
            let producer_barrier = barrier.clone();
            thread::spawn(move || {
                let router = me.ctx.socket(zmq::ROUTER).unwrap();
                router.set_identity(me.id.to_string().as_bytes()).unwrap();
                router.bind(&me.addrs[me.id]).unwrap();
                producer_barrier.wait();
                while me.running.load(Ordering::SeqCst) {
                    if let Ok(parts) = router.recv_multipart(0) {
                        if parts.len() >= 3 {
                            if let Some(msg) = Message::deserialize(&parts[2]) {
                                me.handle_incoming(msg);
                            }
                        }
                    }
                }
                // router socket closes when dropped
            });
        }
        // Wait until listener has bound
        barrier.wait();

        mon
    }

    fn handle_incoming(&self, msg: Message) {
        match msg {
            Message::Request { from, rn } => {
                let mut rn_lock = self.rn.lock().unwrap();
                rn_lock[from] = rn_lock[from].max(rn);
                drop(rn_lock);
                if let Some(tok) = self.token.lock().unwrap().as_mut() {
                    if rn > tok.ln[from] && !tok.queue.contains(&from) {
                        tok.queue.push_back(from);
                        self.local_c.notify_all();
                    }
                }
            }
            Message::Token(new_tok) => {
                *self.token.lock().unwrap() = Some(new_tok);
                self.local_c.notify_all();
            }
            Message::Signal => {
                self.local_c.notify_one();
            }
        }
    }

    pub fn acquire<F, R>(&self, cs: F) -> R
    where
        F: FnOnce() -> R,
    {
        // bump RN
        let my_rn = {
            let mut rn_lock = self.rn.lock().unwrap();
            rn_lock[self.id] += 1;
            rn_lock[self.id]
        };

        // send REQUEST to all peers
        let req = Message::Request {
            from: self.id,
            rn: my_rn,
        }
        .serialize();
        let dealer = self.ctx.socket(zmq::DEALER).unwrap();
        dealer.set_identity(self.id.to_string().as_bytes()).unwrap();
        for pid in 0..self.total {
            if pid == self.id {
                continue;
            }
            dealer.connect(&self.addrs[pid]).unwrap();
            dealer.send(&req, 0).unwrap();
        }

        // wait for token
        let mut guard = self.local_m.lock().unwrap();
        while self.token.lock().unwrap().is_none() {
            guard = self.local_c.wait(guard).unwrap();
        }

        // critical section
        let res = cs();
        self.release_token();
        res
    }

    fn release_token(&self) {
        let mut tok_opt = self.token.lock().unwrap();
        if let Some(tok) = tok_opt.as_mut() {
            tok.ln[self.id] = self.rn.lock().unwrap()[self.id];
            for pid in 0..self.total {
                if pid == self.id {
                    continue;
                }
                let rn_pid = self.rn.lock().unwrap()[pid];
                if rn_pid > tok.ln[pid] && !tok.queue.contains(&pid) {
                    tok.queue.push_back(pid);
                }
            }
            if let Some(next) = tok.queue.pop_front() {
                let tok_msg = Message::Token(tok.clone()).serialize();

                // let dealer = self.ctx.socket(zmq::DEALER).unwrap();
                // dealer.set_identity(self.id.to_string().as_bytes()).unwrap();
                // dealer.connect(&self.addrs[next]).unwrap();
                // dealer.send(&tok_msg, 0).unwrap();

                // self.dealers[next] is already connected and ready
                self.dealers[next].send(&tok_msg, 0).unwrap();

                *tok_opt = None;
            }
        }
    }

    pub fn wait(&self) {
        let guard = self.local_m.lock().unwrap();
        let _un = self.local_c.wait(guard).unwrap();
    }

    fn broadcast_signal(&self) {
        let sig = Message::Signal.serialize();
        // let dealer = self.ctx.socket(zmq::DEALER).unwrap();
        // dealer.set_identity(self.id.to_string().as_bytes()).unwrap();
        for pid in 0..self.total {
            if pid != self.id {
                // dealer.connect(&self.addrs[pid]).unwrap();
                // dealer.send(&sig, 0).unwrap();
                self.dealers[pid].send(&sig, 0).unwrap();
            }
        }
    }

    pub fn signal(&self) {
        self.broadcast_signal();
        self.local_c.notify_one();
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}
