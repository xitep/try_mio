extern crate mio;
extern crate fnv;
extern crate time;

#[macro_use]
extern crate log;
extern crate env_logger;

use std::env;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc;

mod client;

const REQ_DELAY_MILLIS: u64 = 10_000;

fn main() {
    env_logger::init().unwrap();

    // ~ command line parsing
    let mut args = env::args().skip(1);
    let addr: SocketAddr = {
        let s = args.next().unwrap_or_else(|| "127.0.0.1:10001".into());
        match s.to_socket_addrs() {
            Ok(mut addrs) => {
                // ~ simply use the first available
                match addrs.next() {
                    Some(addr) => addr,
                    None => {
                        println!("Cannot resolve: {}", s);
                        return;
                    }
                }
            }
            Err(_) => {
                println!("Not a socket address: {}", s);
                return;
            }
        }
    };
    let n: usize = {
        let s = args.next().unwrap_or_else(|| "10".into());
        match s.parse() {
            Ok(n) => n,
            Err(_) => {
                println!("Not a number: {}", s);
                return;
            }
        }
    };

    // ~ setting up the application state
    let nc = client::NetworkClient::run(n, addr).unwrap();
    debug!(">> network client created");

    // ~ issue N async requests
    let (tx, rx) = mpsc::channel();

    for i in 0..n {
        let now_ns = time::precise_time_ns();
        let r = client::ProtocolRequest::new(1024, (REQ_DELAY_MILLIS / 1000) as u8, b'x', now_ns);
        if let Err(e) =  nc.request_async(i, tx.clone(), r) {
            panic!("Failed to asynchronously request ({}): {}", i, e);
        }
    }
    debug!(">> placed {} requests on network client", n);

    // ~ now await N responses
    for _ in 0..n {
        let r = rx.recv().unwrap();
        let start_ns = r.req.loopback_data;
        let d = time::Duration::nanoseconds((time::precise_time_ns() - start_ns) as i64);
        match r.resp {
            Err(e) => {
                info!("[d: {}] error for client {} and request {:?}: {}",
                      d, r.client, r.req, e);
            }
            Ok(ref data) => {
                assert_eq!(r.req.bytes as usize, data.len());
                assert!(data.iter().all(|&x| x == r.req.echo));
                info!("[d: {}] success for client {} and request: {:?}: {} bytes",
                      d, r.client, r.req, data.len());
            }
        }
    }
    println!(">> awaited all {} responses", n);

    // XXX actually nc.shutdown() first
    if let Err(e) = nc.await() {
        println!("Failure: {}", e);
    }
}
