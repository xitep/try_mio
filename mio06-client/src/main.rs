extern crate mio;
extern crate slab;
#[macro_use]
extern crate log;
extern crate env_logger;

mod client;

use std::sync::mpsc;
use std::net::SocketAddr;

const REQS_PER_CONN: u32 = 2;

fn main() {
    env_logger::init().unwrap();

    let addrs: Vec<SocketAddr> = vec![
        "127.0.0.1:10001".parse().unwrap(),
        "127.0.0.1:10002".parse().unwrap(),
    ];

    let (tx, rx) = mpsc::channel();

    let client = client::new::<()>().unwrap();
    let c1 = client.connect(&addrs[0]).unwrap();
    let c2 = client.connect(&addrs[0]).unwrap();
    for i in 0..REQS_PER_CONN {
        c1.request(client::Request::new(i * 10, (1 + i) as u8, b'x' + i as u8, ()),
                     tx.clone())
            .unwrap();
        c2.request(client::Request::new(i * 10, (1 + i) as u8, b'y' + i as u8, ()),
                     tx.clone())
            .unwrap();
    }

    let mut i = 2*REQS_PER_CONN;
    for r in rx {
        if i == 0 {
            break;
        }
        i -= 1;
        println!("Received: {:?}", r);
    }

    // XXX close the connections

    info!("before shutdown");
    // XXX graceful shutdown
    let r = client.shutdown();
    info!("shutdown: {:?}", r);
}
