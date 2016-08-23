extern crate tokio;
extern crate futures;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::io;
use std::net::SocketAddr;
use std::str;

use futures::Future;

use tokio::Service;
use tokio::io::{Readiness, Transport};
use tokio::proto::pipeline;
use tokio::reactor::{Reactor, ReactorHandle};

pub struct Client {
    reactor: Option<ReactorHandle>,
}

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

    fn into_bytes(&self, out: &mut Vec<u8>) {
        use std::io::Write;
        let _ = write!(out, "{:>02}{:>06}", self.delay, self.bytes);
        out.push(self.echo);
    }
}

pub type ClientHandle = pipeline::ClientHandle<Request, Vec<u8>, io::Error>;

impl Client {
    pub fn new() -> Client {
        Client { reactor: None }
    }

    pub fn connect(self, addr: &SocketAddr) -> io::Result<ClientHandle> {
        let reactor = match self.reactor {
            Some(r) => r,
            None => {
                let reactor = try!(Reactor::default());
                let handle = reactor.handle();
                reactor.spawn();
                handle
            }
        };
        Ok(pipeline::connect(&reactor, addr.clone(),
                             |stream| Ok(Transmit::new(stream))))
    }
}

struct Transmit<T> {
    inner: T,
    rd: Vec<u8>,
    wr: io::Cursor<Vec<u8>>,
}

impl<T> Transmit<T>
    where T: io::Read + io::Write + Readiness
{
    fn new(inner: T) -> Self {
        Transmit {
            inner: inner,
            rd: Vec::new(),
            wr: io::Cursor::new(Vec::new()),
        }
    }
}

type ReqFrame = pipeline::Frame<Request, io::Error>;
type RespFrame = pipeline::Frame<Vec<u8>, io::Error>;

impl<T> Transport for Transmit<T>
    where T: io::Read + io::Write + Readiness
{
    type In = ReqFrame;
    type Out = RespFrame;

    fn read(&mut self) -> io::Result<Option<RespFrame>> {
        loop {
            // ~ first try to process so-far obtained data
            if self.rd.len() >= 6 {
                let n = str::from_utf8(&self.rd[..6])
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();
                if self.rd.len() >= n as usize + 6 {
                    let mut body = Vec::with_capacity(n as usize + 1);
                    body.extend_from_slice(&self.rd[6..(n as usize + 6)]);

                    if self.rd.len() == n as usize + 6 {
                        self.rd.truncate(0);
                    } else {
                        self.rd = self.rd.split_off(n as usize + 6);
                    }
                    return Ok(Some(pipeline::Frame::Message(body)));
                }
            }

            // ~ now try to read-in new data
            match self.inner.read_to_end(&mut self.rd) {
                Ok(0) => return Ok(Some(pipeline::Frame::Done)),
                Ok(_) => {}
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(None);
                    }
                    return Err(e);
                }
            }
        }
    }

    fn write(&mut self, req: ReqFrame) -> io::Result<Option<()>> {
        match req {
            pipeline::Frame::Message(req) => {
                if self.wr.position() < self.wr.get_ref().len() as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::Other, "transport has pending writes"));
                }

                // serialize the request into the write buffer
                { let out = self.wr.get_mut();
                  unsafe { out.set_len(0); }
                  req.into_bytes(out);
                }
                // reset the writer to its beginning
                self.wr.set_position(0);

                // try flusing the write buffer
                self.flush()
            }
            _ => unimplemented!(),
        }
    }

    fn flush(&mut self) -> io::Result<Option<()>> {
        loop {
            let r = {
                let pos = self.wr.position() as usize;
                let data = &self.wr.get_ref()[pos..];
                if data.is_empty() {
                    return Ok(Some(()));
                }
                self.inner.write(data)
            };
            match r {
                Ok(n) => {
                    let p = self.wr.position() + n as u64;
                    self.wr.set_position(p);
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(None);
                    }
                    return Err(e);
                }
            }
        }
    }
}

impl<T> Readiness for Transmit<T>
    where T: Readiness
{
    fn is_readable(&self) -> bool {
        self.inner.is_readable() || !self.rd.is_empty()
    }

    fn is_writable(&self) -> bool {
        self.wr.position() == self.wr.get_ref().len() as u64
    }
}

// --------------------------------------------------------------------

fn main() {
    env_logger::init().unwrap();

    let addr = "127.0.0.1:10001".parse().unwrap();
    let n = 2u32;

    let client = Client::new().connect(&addr).unwrap();

    let mut fs = Vec::with_capacity(n as usize);
    for i in 0..n {
        fs.push(client.call(Request::new(10 + i, 2, b'c' + i as u8)));
    }
    println!("placed {} requests (on one client) ...", n);

    println!("... awaiting responses");
    let resp = await(futures::collect(fs)).unwrap();
    for r in resp {
        println!("awaited response: (len: {}) => {:?}", r.len(), r);
    }

    drop(client);
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
