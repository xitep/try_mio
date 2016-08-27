extern crate bytes;
extern crate tokio;
extern crate futures;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::io;
use std::net::SocketAddr;
use std::str;
use std::u32;

use bytes::{BlockBuf, MutBuf, Source};

use futures::Future;

use tokio::Service;
use tokio::io::{Framed, Parse, Readiness, Serialize, Transport};
use tokio::proto::pipeline;
use tokio::reactor::{Reactor, ReactorHandle};
use tokio::tcp::TcpStream;
use tokio::util::future::{Empty, Val};

#[derive(Debug)]
pub struct Request {
    pub bytes: u32, // ~ the number of bytes to request
    pub delay: u8, // ~ the delay in seconds to request
    pub echo: u8, // ~ the byte to be echoed (`bytes` times repeated)
}

impl Request {
    pub fn new(bytes: u32, delay: u8, echo: u8) -> Self {
        if bytes > 999_999 {
            panic!("Invalid bytes (bytes > 999999): {}", bytes);
        }
        if delay > 99 {
            panic!("Invalid delay (delay > 99): {}", delay);
        }
        Request { bytes: bytes, delay: delay, echo: echo }
    }
}

// --------------------------------------------------------------------

pub struct Client {
    inner: pipeline::Client<Request, Vec<u8>, Empty<(), io::Error>, io::Error>,
}

type ReqFrame = pipeline::Frame<Request, io::Error>;
type RespFrame = pipeline::Frame<Vec<u8>, io::Error>;

impl Client {
    pub fn connect(reactor: &ReactorHandle, addr: &SocketAddr) -> Client {
        let addr = addr.clone();
        let ch = pipeline::connect(reactor, move || {
            let stream = try!(TcpStream::connect(&addr));
            Ok(Framed::new(stream, Parser::new(), Serializer,
                           BlockBuf::new(2, 512), BlockBuf::default()))
        });
        Client { inner: ch }
    }
}

impl Service for Client {
    type Req = Request;
    type Resp = Vec<u8>;
    type Error = io::Error;
    type Fut = Val<Self::Resp, Self::Error>;

    fn call(&self, req: Self::Req) -> Self::Fut {
        self.inner.call(pipeline::Message::WithoutBody(req))
    }
}

struct Serializer;

impl Serialize for Serializer {
    type In = ReqFrame;

    fn serialize(&mut self, msg: Self::In, out: &mut BlockBuf) {
        match msg {
            pipeline::Frame::Message(msg) => {
                use std::io::Write;
                let mut buf = [0u8; 9];
                let _ = write!(&mut &mut buf[..], "{:02}{:06}", msg.delay, msg.bytes);
                buf[8] = msg.echo;
                out.write_slice(&buf[..]);
            }
            _ => unimplemented!(),
        }
    }
}

struct Parser {
    size: u32
}

impl Parser {
    fn new() -> Self { Parser { size: u32::max_value() } }
}

impl Parse for Parser {
    type Out = RespFrame;

    fn parse(&mut self, buf: &mut BlockBuf) -> Option<Self::Out> {
        if self.size == u32::max_value() {
            if buf.len() < 6 {
                return None;
            }
            let mut bb = [0u8; 6];
            {
                let mut cursor = io::Cursor::new(&mut bb);
                buf.shift(6).copy_to(&mut cursor);
            }
            self.size = str::from_utf8(&bb[..]).unwrap().parse::<u32>().unwrap();
        }

        if buf.len() < self.size as usize{
            return None;
        }
        let mut resp = Vec::with_capacity(self.size as usize);
        buf.shift(self.size as usize).copy_to(&mut resp);
        Some(pipeline::Frame::Message(resp))
    }
}

// --------------------------------------------------------------------

fn main() {
    env_logger::init().unwrap();

    let addr = "127.0.0.1:10001".parse().unwrap();
    let n = 2u32;

    let handle = {
        let reactor = Reactor::default().unwrap();
        let handle = reactor.handle();
        reactor.spawn();
        handle
    };

    let mut clients = Vec::with_capacity(2);
    clients.push(Client::connect(&handle, &addr));
    clients.push(Client::connect(&handle, &addr));

    let mut fs = Vec::with_capacity(n as usize);
    for i in 0..n {
        for client in &clients {
            let r = Request::new(10 + i, 2 + i as u8, b'c' + i as u8);
            fs.push(client.call(r));
        }
    }
    println!("placed {} requests ...", n);

    println!("... awaiting responses");
    let resp = await(futures::collect(fs)).unwrap();
    for r in resp {
        println!("awaited response: (len: {}) => {:?}", r.len(), r);
    }

    drop(clients);
}

// Why this isn't in futures-rs, I do not know...
fn await<T: Future>(f: T) -> Result<T::Item, T::Error> {
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel();

    f.then(move |res| {
        tx.send(res).unwrap();
        Ok::<(), ()>(())
    }).forget();

    rx.recv().unwrap()
}
