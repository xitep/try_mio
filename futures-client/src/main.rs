extern crate futures;
extern crate futures_io;
extern crate futures_mio;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::env;
use std::net::ToSocketAddrs;
use std::sync::mpsc;
use std::thread;
use std::time;

use futures::Future;
use futures_mio::Loop;

fn main() {
    env_logger::init().unwrap();

    let mut args = env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:10001".into())
        .to_socket_addrs().expect("cannot resolve")
        .next().expect("no ip available");
    let n: usize = args.next().unwrap_or_else(|| "10".into()).parse()
        .expect("not a number");

    let handle = {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut lp = Loop::new().unwrap();
            tx.send(lp.handle()).unwrap();
            // ~ run infinitly
            let _: () = lp.run(futures::empty::<(), ()>()).unwrap();
        });
        rx.recv().unwrap()
    };

    let now = time::Instant::now();
    let (tx, rx) = mpsc::channel();
    for _ in 0..n {
        let tx = tx.clone();
        handle.clone().tcp_connect(&addr)
            .and_then(|socket| {
                futures_io::write_all(socket, "03000123x")
            }).and_then(|(socket, _)| {
                futures_io::read_exact(socket, [0u8; 6])
                    .and_then(|(socket, buf)| {
                        use std::str;
                        let s = str::from_utf8(&buf[..]).unwrap();
                        let n = s.parse::<usize>().unwrap();
                        futures_io::read_exact(socket, vec![0; n])
                            .map(|(_, buf)| buf)
                    })
            })
            .and_then(move |data| {
                tx.send(data).unwrap();
                futures::finished(())
            }).forget();
    }
    for _ in 0..n {
        let data = rx.recv().unwrap();
        println!("{:?} - {}", now.elapsed(), String::from_utf8_lossy(&data));
    }
    handle.shutdown();
}
