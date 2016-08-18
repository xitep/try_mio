///
/// A `mio` based (playground) server.
///
/// ClientRequest: `Delay` `Bytes` `Echo`
/// Delay: 2 ASCII digits (zero left padded) specifying number of seconds
/// Bytes: 6 ASCII digits (zero left padded) specifying number of bytes
/// Echo: Any byte, specifying the character to echo back
///
/// ClientResponse: After approx. `Delay` seconds: 6 ASCII digits
/// (zero left padded) specifying the number of bytes of the payload
/// following (equals to `Bytes` from the corresponding request.)
/// Repeated `Echo` character/byte `Bytes` times.
///
/// Client protocol errors are not tolarated; the server will
/// disconnect a client upon arrival of invalid requests.
///

extern crate mio;

#[macro_use]
extern crate log;
extern crate env_logger;

use std::env;
use std::io;

use mio::{EventLoop, EventSet, Handler, PollOpt, Token};
use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;

// --------------------------------------------------------------------

mod parser {
    use std::io::{Error, ErrorKind, Result};
    use mio::TryRead;

    /// An incremental parser of a byte stream into protocol request
    /// structures.
    #[derive(Default, Debug)]
    pub struct RequestParser {
        pos: u32,
        buf: [u8; 9],
    }

    #[derive(Default, Debug)]
    pub struct Request {
        pub bytes: u32, // ~ requested number of bytes in the reponse
        pub delay: u32, // ~ requested number of millis to delay the response
        pub echo: u8, // ~ the byte to echo back to the client in the response
    }

    impl RequestParser {
        // ~ tries to incrementaly parse the given data as of data_start
        // and incrementally produce a request structure.  if all bytes
        // necessary for a request are consumed it is returned along with
        // the index where consuming the given bytes have stopped and may
        // be continued.
        // ~ returns `Some(..)` if a complete request was parsed,
        // otherwise None
        pub fn try_parse<R: TryRead>(&mut self, r: &mut R) -> Result<Option<Request>> {
            loop {
                match try!(r.try_read(&mut self.buf[self.pos as usize ..])) {
                    None => return Ok(None),
                    Some(0) => return Ok(None),
                    Some(len) => {
                        self.pos += len as u32;
                        if self.pos == self.buf.len() as u32 {
                            break;
                        }
                    }
                }
            }
            // ~ at this point if have all the bytes for parsing the request
            self.pos = 0;
            Ok(Some(try!(parse_request_buffer(&self.buf[..]))))
        }
    }

    fn parse_request_buffer(data: &[u8]) -> Result<Request> {
        // ~ first 6 digits represent the requested number of bytes
        // ~ the last 2 digits represent the requested number of seconds of delay
        Ok(Request {
            delay: try!(parse_digits(&data[0..2], 1000)),
            bytes: try!(parse_digits(&data[2..8], 1)),
            echo: data[8],
        })
    }

    fn parse_digits(data: &[u8], scale: u32) -> Result<u32> {
        let mut n = 0u32;
        for &b in data {
            if b >= b'0' && b <= b'9' {
                n = (n * 10) + (b - b'0') as u32;
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Not a number: {:?}", data)));
            }
        }
        Ok(n * scale)
    }
}

// --------------------------------------------------------------------

mod client {
    use std::cmp;
    use std::fmt;
    use std::io;

    use mio::{self, EventLoop, EventSet, Handler, PollOpt, Token, TryWrite};
    use mio::tcp::{TcpStream, Shutdown};

    use super::parser::{Request, RequestParser};
    use super::Timeout;

    #[derive(Default, Debug)]
    struct ResponseWriter {
        req: Request, // ~ the request which this response is being generated for
        bytes_out: u32, // ~ the number of bytes produced to the client
    }

    impl ResponseWriter {
        // ~ tries to write out the rest of the response
        // ~ returns 'true' if the reponse was fully written out
        // to the client otherwise 'false'
        fn write<W: TryWrite>(&mut self, w: &mut W) -> io::Result<bool> {
            // ~ first ensure we dump the number of bytes of the payload
            if self.bytes_out < 6 {
                let mut buf = [0; 6];
                {
                    use std::io::Write;
                    let mut obuf = io::Cursor::new(&mut buf[..]);
                    let _ = write!(obuf, "{:>06}", self.req.bytes);
                }
                while self.bytes_out < 6 {
                    match try!(w.try_write(&buf[self.bytes_out as usize ..])) {
                        None => return Ok(false),
                        Some(0) => return Ok(false),
                        Some(len) => self.bytes_out += len as u32,
                    }
                }
            }

            // ~ now produce the requested payload
            let mut pending_bytes = (self.req.bytes - (self.bytes_out - 6)) as usize;
            let buf = [self.req.echo; 512];
            while pending_bytes > 0 {
                match try!(w.try_write(&buf[..cmp::min(buf.len(), pending_bytes)])) {
                    None => return Ok(false),
                    Some(0) => return Ok(false),
                    Some(len) => {
                        pending_bytes -= len;
                        self.bytes_out += len as u32;
                    }
                }
            }
            Ok(true)
        }
    }

    pub struct Client {
        socket: TcpStream, // ~ the client socket
        req_parser: RequestParser, // ~ the request parser (stateful)
        resp_writer: ResponseWriter, // ~ the state of the generating a response for a request
        interests: EventSet, // ~ set of events this client is interested in
        timer: Option<mio::Timeout>, // ~ a potentially pending timer for this client
    }

    impl fmt::Debug for Client {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Client {{ peer_addr: {:?}, local_addr: {:?}, iterests: {:?}, resp_writer: {:?} }}",
                   self.socket.peer_addr(), self.socket.local_addr(), self.interests, self.resp_writer)
        }
    }

    impl Client {
        pub fn new(socket: TcpStream) -> Self {
            Client {
                socket: socket,
                req_parser: RequestParser::default(),
                // ~ initially a client is interested only in reading (a request)
                interests: EventSet::all() - EventSet::writable(),
                resp_writer: ResponseWriter::default(),
                timer: None,
            }
        }

        pub fn on_accepted<S: Handler>(&self, evt_loop: &mut EventLoop<S>, token: Token)
                                       -> io::Result<()>
        {
            evt_loop.register(
                &self.socket, token, self.interests, PollOpt::edge() | PollOpt::oneshot())
        }

        pub fn on_readable<S: Handler<Timeout=Timeout>>(&mut self, evt_loop: &mut EventLoop<S>, token: Token)
                                       -> io::Result<()>
        {
            match try!(self.req_parser.try_parse(&mut self.socket)) {
                None => {}, // ~ keep reading
                Some(req) => {
                    // ~ switch from reading to writing
                    self.resp_writer = ResponseWriter { req: req, bytes_out: 0 };
                    trace!("Parsed request: (token: {:?}) (client: {:?})", token, self);


                    if self.resp_writer.req.delay == 0 {
                        // ~ stop listening for read events
                        self.interests =
                            (self.interests - EventSet::readable()) | EventSet::writable();
                    } else {
                        match evt_loop.timeout_ms(Timeout::StartResponding(token),
                                                  self.resp_writer.req.delay as u64)
                        {
                            Err(e) => return Err(io::Error::new(
                                io::ErrorKind::Other, format!("Timer scheduling failure: {:?}", e))),
                            Ok(to) => self.timer = Some(to),
                        }
                        // ~ stop listening for read events
                        self.interests = self.interests - EventSet::readable();
                    }
                }
            }
            evt_loop.reregister(
                &self.socket, token, self.interests, PollOpt::edge() | PollOpt::oneshot())
        }

        pub fn on_timeout_start_responding<S: Handler>(
            &mut self, evt_loop: &mut EventLoop<S>, token: Token)
            -> io::Result<()>
        {
            self.interests = self.interests | EventSet::writable();
            self.timer = None;
            evt_loop.reregister(&self.socket, token, self.interests, PollOpt::edge() | PollOpt::oneshot())
        }

        pub fn on_writable<S: Handler>(&mut self, evt_loop: &mut EventLoop<S>, token: Token)
                                       -> io::Result<()>
        {
            if try!(self.resp_writer.write(&mut self.socket)) {
                trace!("Writing out reponse (token: {:?}) (client: {:?})", token, self);

                // ~ let's accept new requests from the client again
                self.interests =
                    (self.interests | EventSet::readable()) - EventSet::writable();
            }
            evt_loop.reregister(
                &self.socket, token, self.interests, PollOpt::edge() | PollOpt::oneshot())
        }

        pub fn shutdown<S: Handler>(&mut self, evt_loop: &mut EventLoop<S>) {
            let _ = self.socket.shutdown(Shutdown::Both);
            if let Some(to) = self.timer.take() {
                let _ = evt_loop.clear_timeout(to);
            }
        }
    }
}

// --------------------------------------------------------------------

const LISTENER_TOKEN: Token = Token(0);

struct ServerHandler {
    listener: TcpListener, // the tcp listener associated with `LISTENER_TOKEN`
    clients: Slab<client::Client>, // accepted clients
}

#[derive(Debug)]
pub enum Timeout {
    StartResponding(Token),
}

impl ServerHandler {

    fn on_accepted(&mut self, evt_loop: &mut EventLoop<Self>, csocket: TcpStream)
                   -> io::Result<()>
    {
        let _ = csocket.set_nodelay(true);

        // ~ remember the client connection for later reference
        let ctoken = match self.clients.insert(client::Client::new(csocket)) {
            Err(mut client) => {
                client.shutdown(evt_loop);
                let e = io::Error::new(io::ErrorKind::ConnectionRefused, "Too many clients!");
                return Err(e);
            }
            Ok(t) => t,
        };
        // ~ register the client connection with this event loop
        let client = &self.clients[ctoken];
        debug!("Accepted client: (token: {:?}) (client: {:?})",
                ctoken, client);
        client.on_accepted(evt_loop, ctoken)
    }

    fn on_readable(&mut self, evt_loop: &mut EventLoop<Self>, token: Token) -> bool {
        self.handle(evt_loop, token, client::Client::on_readable, "Read")
    }

    fn on_writable(&mut self, evt_loop: &mut EventLoop<Self>, token: Token) -> bool {
        self.handle(evt_loop, token, client::Client::on_writable, "Write")
    }

    // ~ return true if the corresponding client got closed and no
    // more events are to be processed on its behalf
    fn handle<F>(&mut self, evt_loop: &mut EventLoop<Self>, token: Token, handler: F, handler_name: &str) -> bool
        where F: Fn(&mut client::Client, &mut EventLoop<Self>, Token) -> io::Result<()>
    {
        if let Err(e) = handler(&mut self.clients[token], evt_loop, token) {
            debug!("{} error => Disconnecting: (token: {:?}) (client: {:?}): {:?}",
                    handler_name, token, &self.clients[token], e);
            if let Some(mut client) = self.clients.remove(token) {
                client.shutdown(evt_loop);
            }
            return true;
        }
        false
    }

    fn on_hup(&mut self, evt_loop: &mut EventLoop<Self>, token: Token) {
        debug!("Hup => Disconnecting: (token: {:?}) (client: {:?})",
                token, &self.clients[token]);
        if let Some(mut client) = self.clients.remove(token) {
            client.shutdown(evt_loop);
        }
    }

    fn on_timeout(&mut self, evt_loop: &mut EventLoop<Self>, payload: Timeout) {
        trace!("on_timeout: {:?}", payload);
        match payload {
            Timeout::StartResponding(token) => {
                self.handle(evt_loop, token, client::Client::on_timeout_start_responding, "Timeout");
            }
        }
    }
}

// ~ XXX read timeouts
// ~ XXX write timeouts
impl Handler for ServerHandler {
    type Timeout = Timeout;
    type Message = ();

    fn ready(&mut self, evt_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        match token {
            LISTENER_TOKEN => {
                // ~ try accepting the client connections
                loop {
                    let csocket = match self.listener.accept() {
                        Err(e) => {
                            debug!("Accept error: {}", e);
                            return;
                        }
                        Ok(None) => break,
                        Ok(Some((socket, _))) => socket,
                    };
                    if let Err(e) = self.on_accepted(evt_loop, csocket) {
                        debug!("Error accepting client: {:?}", e);
                    }
                }
            }
            _ => {
                trace!("Client events (token: {:?}) (client: {:?}) (events: {:?})",
                        token, &self.clients[token], events);
                if events.is_readable() && self.on_readable(evt_loop, token) {
                    return;
                }
                if events.is_writable() && self.on_writable(evt_loop, token) {
                    return;
                }
                if events.is_hup() {
                    self.on_hup(evt_loop, token);
                    return;
                }
            }
        }
    }

    fn timeout(&mut self, evt_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {
        self.on_timeout(evt_loop, timeout)
    }
}

// --------------------------------------------------------------------

fn main() {
    env_logger::init().unwrap();

    let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:10001".into());
    let addr = match addr.parse() {
        Ok(addr) => addr,
        Err(_) => {
            println!("Not an address: {}", addr);
            return;
        }
    };
    let server = match TcpListener::bind(&addr) {
        Ok(server) => server,
        Err(e) => {
            println!("Failed to bind to {}: {}", addr, e);
            return;
        }
    };

    let mut evt_loop = EventLoop::<ServerHandler>::new().unwrap();
    evt_loop.register(&server, LISTENER_TOKEN, EventSet::readable(), PollOpt::edge()).unwrap();
    println!("Starting event loop on {:?}", addr);
    evt_loop.run(&mut ServerHandler{
        listener: server,
        clients: Slab::new_starting_at(Token(1), 1000),
    }).unwrap();
}
