use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, SocketAddr, ToSocketAddrs};
use std::str;

fn main() {
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

    let mut socket = TcpStream::connect(addr).unwrap();
    socket.set_nodelay(true).unwrap();

    let data_out = b"01000024x";
    let mut data_in = [0; 24];
    for _ in 0..n {
        socket.write(&data_out[..]).unwrap();

        let mut preemble = [0u8; 6];
        socket.read_exact(&mut preemble[..]).unwrap();
        let size = str::from_utf8(&preemble).unwrap()
            .parse::<u32>().unwrap();
        // since we requested 24 bytes in all our original requests
        assert_eq!(size, 24); 

        socket.read_exact(&mut data_in[..]).unwrap();

        // since we requested 'x'es
        assert!(&data_in[..].iter().all(|&x| x == b'x'));
    }

    println!("awaited all {} responses", n);
}
