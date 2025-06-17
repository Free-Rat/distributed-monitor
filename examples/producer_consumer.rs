// examples/producer_consumer_peer.rs

use distributed_monitor::Monitor;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

const NODES: usize = 3;
const PRODUCERS_PER_NODE: usize = 1;
const CONSUMERS_PER_NODE: usize = 1;
const ITEMS_PER_PRODUCER: usize = 5;
const HOST: &str = "127.0.0.1";
const BASE_PORT: u16 = 7000;

fn main() {
    // 1) Create one Monitor per node
    let mut mons = Vec::with_capacity(NODES);
    for id in 0..NODES {
        let initial = if id == 0 { Some(0) } else { None };
        let mon = Monitor::new(id, NODES, initial, HOST, BASE_PORT);
        println!(
            "Node {}: Monitor initialized, initial token owner = {:?}",
            id, initial
        );
        mons.push(mon);
    }

    // Shared buffer
    let buffer = Arc::new(Mutex::new(VecDeque::new()));
    let mut handles = Vec::new();

    // 2) Spawn producers & consumers
    for id in 0..NODES {
        let mon = mons[id].clone();
        let buf = buffer.clone();

        // Producers
        for prod in 0..PRODUCERS_PER_NODE {
            let mon_p = mon.clone();
            let buf_p = buf.clone();
            handles.push(thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    println!(
                        "[Node{}-P{}] -> Requesting token to produce {}",
                        id, prod, i
                    );
                    mon_p.acquire(|| {
                        println!(
                            "[Node{}-P{}] **Acquired token** for produce {}",
                            id, prod, i
                        );
                        let mut q = buf_p.lock().unwrap();
                        q.push_back((id, prod, i));
                        println!("[Node{}-P{}] Produced item {}", id, prod, i);
                        println!("[Node{}-P{}] Signaling any waiters", id, prod);
                        mon_p.signal();
                        println!(
                            "[Node{}-P{}] **Releasing token** after produce {}",
                            id, prod, i
                        );
                    });
                    thread::sleep(Duration::from_millis(20));
                }
            }));
        }

        // Consumers
        for cons in 0..CONSUMERS_PER_NODE {
            let mon_c = mon.clone();
            let buf_c = buf.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ITEMS_PER_PRODUCER {
                    println!("[Node{}-C{}] -> Requesting token to consume", id, cons);
                    let item = mon_c.acquire(|| {
                        println!("[Node{}-C{}] **Acquired token** for consume", id, cons);
                        let mut q = buf_c.lock().unwrap();
                        while q.is_empty() {
                            println!("[Node{}-C{}] Buffer empty, waiting", id, cons);
                            mon_c.wait();
                            println!("[Node{}-C{}] Woke up, rechecking buffer", id, cons);
                            q = buf_c.lock().unwrap();
                        }
                        let popped = q.pop_front();
                        println!("[Node{}-C{}] Consumed {:?}", id, cons, popped);
                        println!("[Node{}-C{}] **Releasing token** after consume", id, cons);
                        mon_c.signal();
                        popped
                    });
                    thread::sleep(Duration::from_millis(30));
                }
            }));
        }
    }

    // 3) Join all threads
    for h in handles {
        h.join().unwrap();
    }

    // 4) Final buffer state
    println!("Final buffer contents: {:?}", buffer.lock().unwrap());
}
