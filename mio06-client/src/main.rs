extern crate mio;
extern crate slab;
#[macro_use]
extern crate log;
extern crate env_logger;

mod client;

use std::sync::mpsc;
use std::net::SocketAddr;

const REQS_PER_CONN: u32 = 2;
const NUM_CONNS_PER_ADDR: usize = 20;

fn main() {
    env_logger::init().unwrap();

    let addrs: Vec<SocketAddr> = vec![
        "127.0.0.1:10001".parse().unwrap(),
        "127.0.0.1:10002".parse().unwrap(),
    ];

    let (tx, rx) = mpsc::channel();

    // ~ create client and open connections
    let n_conns = addrs.len() * NUM_CONNS_PER_ADDR;
    let client = client::new::<()>(n_conns).unwrap();
    let mut conns = Vec::with_capacity(n_conns);
    for i in 0..NUM_CONNS_PER_ADDR {
        for (a_i, addr) in addrs.iter().enumerate() {
            conns.push((client.connect(addr).unwrap(), b'a' + (i * addrs.len()) as u8 + a_i as u8));
        }
    }

    // ~ send requests
    for &(ref conn, b) in &conns {
        for mut i in 0..REQS_PER_CONN {
            i = i + 1;
            let req = client::Request::new(i * 10, 2 * i as u8, b, ());
            conn.request(req, tx.clone()).unwrap();
        }
    }

    // ~ await responses
    for _ in 0..conns.len() * REQS_PER_CONN as usize {
        let r = rx.recv().unwrap();
        println!("Received: {:?}", r);
    }

    // ~ drop the connection without explicitely calling '.close()'
    drop(conns.pop());

    // ~ close connections and shutdown the client
    println!("Closing connections");
    for (conn, _) in conns {
        conn.close().unwrap();
    }
    info!("Shutting down");
    let r = client.shutdown();
    info!("Shut down: {:?}", r);
}
