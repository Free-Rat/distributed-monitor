pub mod message;
use message::{Message, Token};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
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
    addrs: Vec<String>,

    rn: Arc<Mutex<Vec<RequestNumber>>>,
    token: Arc<Mutex<Option<Token>>>,

    local_m: Mutex<()>,
    local_c: Condvar,

    // ZMQ context for listener and sender thread
    ctx: Arc<zmq::Context>,
    running: Arc<AtomicBool>,

    // Channel to offload all outgoing sends to a dedicated thread
    send_tx: Sender<(ProcessId, Vec<u8>)>,
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

        // Build sending thread
        let (tx, rx): (Sender<(usize, Vec<u8>)>, Receiver<(usize, Vec<u8>)>) = mpsc::channel();

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
            addrs: addrs.clone(),
            rn: Arc::new(Mutex::new(vec![0; total])),
            token: Arc::new(Mutex::new(initial_tok)),
            local_m: Mutex::new(()),
            local_c: Condvar::new(),
            ctx: ctx.clone(),
            running: Arc::new(AtomicBool::new(true)),
            send_tx: tx,
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
                        // parts might be [id, payload] or [id, empty, payload]
                        let payload = parts.last().expect("ROUTER multipart missing payload");
                        if let Some(msg) = Message::deserialize(payload) {
                            me.handle_incoming(msg);
                        } else {
                            eprintln!(
                                "[Node {}] failed to parse message from frames: {:?}",
                                me.id, parts
                            );
                        }
                    }
                }
            });
        }
        barrier.wait();

        {
            let ctx_s = Arc::clone(&ctx);
            let addrs_s = addrs.clone();
            let sender = id.clone();
            thread::spawn(move || {
                // Pre-connect one DEALER per peer, with debug prints
                let mut dealers = Vec::with_capacity(total);
                for pid in 0..total {
                    let sock = ctx_s.socket(zmq::DEALER).unwrap();
                    sock.set_identity(id.to_string().as_bytes()).unwrap();
                    let addr = &addrs_s[pid];
                    match sock.connect(addr) {
                        Ok(_) => println!(
                            "[Sender Thread {}] Connected DEALER[{}] to {}",
                            id, pid, addr
                        ),
                        Err(e) => eprintln!(
                            "[Sender Thread {}] Failed to connect DEALER[{}] to {}: {}",
                            id, pid, addr, e
                        ),
                    }
                    dealers.push(sock);
                }
                // Drain the channel and send each message
                while let Ok((pid, msg)) = rx.recv() {
                    // println!(
                    //     "[Sender Thread {}] Sending {} bytes to peer {}",
                    //     id,
                    //     msg.len(),
                    //     pid
                    // );
                    if let Err(e) = dealers[pid].send(&msg, 0) {
                        eprintln!("[Sender Thread {}] Error sending to {}: {}", id, pid, e);
                    } else {
                        // println!(
                        //     "[Sender Thread {}] Successfully sent message to {}",
                        //     id, pid
                        // );
                    }
                }
            });
        }
        mon
    }

    fn handle_incoming(&self, msg: Message) {
        match msg {
            Message::Request { from, rn } => {
                // println!(
                //     "[Node {}] Received REQUEST from {} (rn={})",
                //     self.id, from, rn
                // );
                let mut rn_lock = self.rn.lock().unwrap();
                rn_lock[from] = rn_lock[from].max(rn);
                drop(rn_lock);
                if let Some(tok) = self.token.lock().unwrap().as_mut() {
                    if rn > tok.ln[from] && !tok.queue.contains(&from) {
                        println!(
                            "[Node {}] Enqueued requester {} => queue={:?}",
                            self.id, from, tok.queue
                        );
                        tok.queue.push_back(from);
                    }
                }
                // Wake anyone waiting for the token or queue change
                self.local_c.notify_all();
            }
            Message::Token(new_tok) => {
                println!(
                    "[Node {}] Received TOKEN, ln={:?}, queue={:?}",
                    self.id, new_tok.ln, new_tok.queue
                );
                *self.token.lock().unwrap() = Some(new_tok);
                self.local_c.notify_all();
            }
            Message::Signal => {
                // println!("[Node {}] Received SIGNAL", self.id);
                self.local_c.notify_one();
            }
            _ => {
                println!("[Node {}] handles incoming did not matched msg", self.id);
            }
        }
    }

    pub fn acquire<F, R>(&self, cs: F) -> R
    where
        F: FnOnce() -> R,
    {
        // bump RN and broadcast
        let my_rn = {
            let mut rn_lock = self.rn.lock().unwrap();
            rn_lock[self.id] += 1;
            rn_lock[self.id]
        };
        let req = Message::Request {
            from: self.id,
            rn: my_rn,
        }
        .serialize();
        for pid in 0..self.total {
            if pid != self.id {
                self.send_tx.send((pid, req.clone())).unwrap();
            }
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
                self.send_tx.send((next, tok_msg)).unwrap();
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
        for pid in 0..self.total {
            if pid != self.id {
                // println!("[Node {}] Broadcasting SIGNAL to {}", self.id, pid);
                self.send_tx.send((pid, sig.clone())).unwrap();
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
