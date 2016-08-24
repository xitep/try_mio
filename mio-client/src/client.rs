use std::ascii;
use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::hash::BuildHasherDefault;
use std::io::{self, Result};
use std::mem;
use std::net::SocketAddr;
use std::str;
use std::sync::mpsc;
use std::thread;
use std::u32;

use fnv::FnvHasher;

use mio::{self, EventLoop, EventLoopConfig, EventSet, Handler, PollOpt, Token, TryRead, TryWrite};
use mio::tcp::TcpStream;


pub struct Response<T> {
    pub client: usize, // ~ the client (e.g. "host:port") which responsed
    pub req: ProtocolRequest<T>, // ~ the originating protocol request
    pub resp: Result<Vec<u8>>, // ~ the (protocol) response data
}

pub struct ProtocolRequest<T> {
    pub bytes: u32, // ~ the number of bytes to request
    pub delay: u8, // ~ the delay in seconds to request
    pub echo: u8, // ~ the byte to be echoed (`bytes` times repeated)
    pub loopback_data: T, // ~ user provided data to be returned with the response
}

impl<T> fmt::Debug for ProtocolRequest<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ProtocolRequest {{ bytes: {}, delay: {}, echo: {}",
               self.bytes, self.delay, self.echo)
    }
}

impl<T> ProtocolRequest<T> {
    pub fn new(bytes: u32, delay: u8, echo: u8, loopback_data: T) -> Self {
        if bytes > 999_999 {
            panic!("Invalid bytes (bytes > 999999): {}", bytes);
        }
        if delay > 99 {
            panic!("Invalid delay (delay > 99): {}", delay);
        }
        ProtocolRequest { bytes: bytes, delay: delay, echo: echo, loopback_data: loopback_data }
    }
}

// Internal structure representing a client's protocol request for the
// event loop handler
struct HandlerRequest<T> {
    client: usize, // ~ which client (connect) to use; might be host:port in future
    reply: mpsc::Sender<Response<T>>, // ~ where to send the response to
    req: ProtocolRequest<T>, // ~ the actual protocol request
}

impl<T> fmt::Debug for HandlerRequest<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HandlerRequest {{ client: {}, req: {:?} }}", self.client, self.req)
    }
}

struct ProtocolRequestWriter {
    pos: u32,
    buf: [u8; 9]
}

impl fmt::Debug for ProtocolRequestWriter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r#"ProtocolRequestWriter {{ pos: {}, buf: "{}" }}"#,
               self.pos, AsAscii(&self.buf[..]))
    }
}

struct AsAscii<'a>(&'a [u8]);

impl<'a> fmt::Display for AsAscii<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::fmt::Write;

        for b in self.0.iter().flat_map(|&b| ascii::escape_default(b)) {
            try!(f.write_char(b as char));
        }
        Ok(())
    }
}

impl ProtocolRequestWriter {
    fn new() -> Self {
        ProtocolRequestWriter {
            pos: u32::MAX,
            buf: [0; 9],
        }
    }

    fn reset<T>(&mut self, req: &ProtocolRequest<T>) {
        use std::io::Write;
        self.pos = 0;
        write!(&mut self.buf[..2], "{:02}", req.delay).unwrap();
        write!(&mut self.buf[2..8], "{:06}", req.bytes).unwrap();
        self.buf[8] = req.echo;
    }

    // ~ tries to write out the rest of the request - if any
    // ~ returns 'true' if the request was fully written out
    fn write<W: TryWrite>(&mut self, w: &mut W) -> Result<bool> {
        while (self.pos as usize) < self.buf.len() {
            match try!(w.try_write(&self.buf[self.pos as usize ..])) {
                None => return Ok(false),
                Some(0) => return Ok(false),
                Some(len) => {
                    self.pos += len as u32;
                }
            }
        }
        Ok(true)
    }
}

#[derive(Default)]
struct ResponsePreemble {
    pos: u16, // ~ current write position into buf
    buf: [u8; 6], // ~ the raw, serialized preemble data
}

struct ProtocolResponseReader {
    preemble: ResponsePreemble,
    // ~ the number of bytes expected (originating from the
    // corresponding request); used for verification purposes only
    expected_bytes: u32, 
    // ~ the number of bytes awaiting to read (as determined from the
    // preemble)
    awaiting_bytes: u32, 
    // ~ the actually response data payload
    data: Vec<u8>, 
}

impl ProtocolResponseReader {
    fn new(expected_bytes: u32) -> Self {
        ProtocolResponseReader {
            preemble: Default::default(),
            expected_bytes: expected_bytes,
            awaiting_bytes: 0,
            data: Vec::new(),
        }
    }

    fn reset(&mut self, expected_bytes: u32) {
        self.preemble = Default::default();
        self.expected_bytes = expected_bytes;
        self.awaiting_bytes = 0;
        self.data.clear();
    }

    fn take(&mut self) -> Vec<u8> {
        self.preemble = Default::default();
        self.expected_bytes = 0;
        self.awaiting_bytes = 0;
        mem::replace(&mut self.data, Vec::new())
    }

    fn read<R: TryRead>(&mut self, r: &mut R) -> Result<bool> {
        // ~ try parsing the preemble if neccessary
        if (self.preemble.pos as usize) < self.preemble.buf.len() {
            loop {
                match try!(r.try_read(&mut self.preemble.buf[self.preemble.pos as usize ..])) {
                    None => return Ok(false),
                    Some(0) => return Ok(false),
                    Some(n) => {
                        self.preemble.pos += n as u16;
                        if self.preemble.pos as usize == self.preemble.buf.len() {
                            let s = str::from_utf8(&self.preemble.buf).unwrap();
                            self.awaiting_bytes = s.parse::<u32>().unwrap();
                            assert_eq!(self.expected_bytes, self.awaiting_bytes);
                            break;
                        }
                    }
                }
            }
        }
        // ~ now parse the response body
        if self.awaiting_bytes == 0 {
            return Ok(true);
        }
        if self.data.capacity() == 0 {
            self.data.reserve_exact(self.awaiting_bytes as usize);
        }
        loop {
            match try!(r.try_read_buf(&mut self.data)) {
                None => return Ok(false),
                Some(0) => return Ok(false),
                Some(_) =>  {
                    if self.data.len() == self.awaiting_bytes as usize {
                        return Ok(true);
                    }
                }
            }
        }
    }
}

// --------------------------------------------------------------------

struct Connection<T> {
    socket: TcpStream, // ~ the client socket
    interests: EventSet, // ~ set of events this client is interested in

    curr_req: Option<HandlerRequest<T>>, // ~ the currently handled request
    writer: ProtocolRequestWriter, // ~ a writer of the current request
    reader: ProtocolResponseReader, // ~ a reader for the response of the current request
}

impl<T> fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection {{ peer_addr: {:?}, local_addr: {:?}, interests: {:?} }}",
               self.socket.peer_addr(), self.socket.local_addr(), self.interests)
    }
}

impl<T> Connection<T> {
    fn new(socket: TcpStream) -> Self {
        Connection {
            socket: socket,
            interests: EventSet::all() - EventSet::readable() - EventSet::writable(),
            curr_req: None,
            writer: ProtocolRequestWriter::new(),
            reader: ProtocolResponseReader::new(0),
        }
    }

    /// ~ panics if there currently is a handler-request registered
    /// with this connection
    fn handle(&mut self, hr: HandlerRequest<T>) {
        assert!(self.curr_req.is_none());
        self.writer.reset(&hr.req);
        self.reader.reset(hr.req.bytes);
        self.curr_req = Some(hr);
        self.interests = (self.interests | EventSet::writable()) - EventSet::readable();
    }

    fn register<H: Handler>(&self, evt_loop: &mut EventLoop<H>, token: Token) -> Result<()> {
        evt_loop.register(&self.socket, token, self.interests, PollOpt::oneshot())
    }

    fn reregister<H: Handler>(&self, evt_loop: &mut EventLoop<H>, token: Token) -> Result<()> {
        evt_loop.reregister(&self.socket, token, self.interests, PollOpt::oneshot())
    }

    fn on_writable<H: Handler>(&mut self, evt_loop: &mut EventLoop<H>, token: Token)
                               -> Result<bool>
    {
        assert!(self.curr_req.is_some());
        if try!(self.writer.write(&mut self.socket)) {
            // ~ switch from "write-" to "read-mode"
            self.interests = (self.interests | EventSet::readable()) - EventSet::writable();
        }
        // ~ otherwise stay in "write-mode"
        try!(evt_loop.reregister(&self.socket, token, self.interests, PollOpt::oneshot()));
        Ok(false)
    }

    fn on_readable<H: Handler>(&mut self, evt_loop: &mut EventLoop<H>, token: Token)
                               -> Result<bool>
    {
        assert!(self.curr_req.is_some());
        if try!(self.reader.read(&mut self.socket)) {
            // ~ we're done. stop reading.
            self.interests = self.interests - EventSet::readable();

            let curr_req = self.curr_req.take().unwrap();
            let _ = curr_req.reply.send(Response {
                client: curr_req.client,
                req: curr_req.req,
                resp: Ok(self.reader.take()),
            });
        }
        try!(evt_loop.reregister(&self.socket, token, self.interests, PollOpt::oneshot()));
        Ok(false)
    }

    fn shutdown<H: Handler>(&mut self, _: &mut EventLoop<H>, err: String) {
        if let Some(curr_req) = self.curr_req.take() {
            let _ = curr_req.reply.send(Response {
                client: curr_req.client,
                req: curr_req.req,
                resp: Err(io::Error::new(io::ErrorKind::Other, err)),
            });
        }
    }
}

// --------------------------------------------------------------------

struct NetworkClientHandler<T> {
    // XXX usize => Token
    conns: HashMap<usize, Connection<T>, BuildHasherDefault<FnvHasher>>,
    addr: SocketAddr, // ~ addr connections are going to connect to
}

impl<T> NetworkClientHandler<T> {
    fn on_hup<H: Handler>(&mut self, evt_loop: &mut EventLoop<H>, token: Token) {
        trace!("OnHup (token: {:?}) (client: {:?})", token, self.conns[&token.0]);
        if let Some(mut client) = self.conns.remove(&token.0) {
            client.shutdown(evt_loop, "OnHup: Connection closed".into());
        }
    }

    fn handle<F, H>(&mut self, evt_loop: &mut EventLoop<H>,
                 token: Token, handler: F, handler_name: &str) -> bool
        where F: Fn(&mut Connection<T>, &mut EventLoop<H>, Token) -> Result<bool>,
              H: Handler
    {
        trace!("{} (token: {:?}) (client: {:?})", handler_name, token, self.conns[&token.0]);
        match handler(self.conns.get_mut(&token.0).unwrap(), evt_loop, token) {
            Err(e) => {
                trace!("{} error => Disconnecting: (token: {:?}) (client: {:?}): {:?}",
                        handler_name, token, self.conns[&token.0], e);
                if let Some(mut client) = self.conns.remove(&token.0) {
                    client.shutdown(evt_loop, format!("{} error: {:?}", handler_name, e));
                }
                true
            }
            Ok(b) => b,
        }
    }
}

impl<T: Send> Handler for NetworkClientHandler<T> {
    type Timeout = ();
    type Message = HandlerRequest<T>;

    fn ready(&mut self, evt_loop: &mut EventLoop<Self>, token: Token, events: EventSet) {
        if events.is_hup() {
            self.on_hup(evt_loop, token);
            return;
        }

        macro_rules! handle {
            ($is_event:ident, $handler:expr, $name:expr) => {
                if events.$is_event() && self.handle(evt_loop, token, $handler, $name) {
                    return;
                }
            }
        }
        handle!(is_writable, Connection::on_writable, "OnWrite");
        handle!(is_readable, Connection::on_readable, "OnRead");
    }

    fn notify(&mut self, evt_loop: &mut EventLoop<Self>, msg: Self::Message) {
        let token = Token(msg.client);
        trace!("Notified: {:?}", msg);

        match self.conns.entry(msg.client) {
            Entry::Occupied(mut e) => {
                let conn = e.get_mut();
                // XXX determine whether the client is currently active
                conn.handle(msg);
                conn.reregister(evt_loop, token).unwrap();
            }
            Entry::Vacant(e) => {
                match TcpStream::connect(&self.addr) {
                    Err(e) => {
                        let _ = msg.reply.send(Response {
                            client: msg.client,
                            req: msg.req,
                            resp: Err(e),
                        });
                    }
                    Ok(stream) => {
                        let _ = stream.set_nodelay(true);
                        let mut conn = e.insert(Connection::new(stream));
                        conn.handle(msg);
                        conn.register(evt_loop, token).unwrap();
                    }
                }
            }
        }
    }
}

// --------------------------------------------------------------------

pub struct NetworkClient<T: Send> {
    channel: mio::Sender<<NetworkClientHandler<T> as Handler>::Message>,
    join_handle: thread::JoinHandle<Result<()>>,
}

// XXX 'static is wrong here
impl<T: Send + 'static> NetworkClient<T> {
    pub fn run(clients: usize, addr: SocketAddr) -> Result<NetworkClient<T>> {
        let mut evt_loop_cfg = EventLoopConfig::new();
        evt_loop_cfg.notify_capacity(clients);
        let mut evt_loop = try!(EventLoop::configured(evt_loop_cfg));
        let channel = evt_loop.channel();
        let join_handle = thread::spawn(move || {
            let mut h = NetworkClientHandler {
                conns: HashMap::with_capacity_and_hasher(clients, BuildHasherDefault::default()),
                addr: addr,
            };
            evt_loop.run(&mut h)
        });
        Ok(NetworkClient {
            channel: channel,
            join_handle: join_handle,
        })
    }

    pub fn request_async(
        &self, client: usize, reply: mpsc::Sender<Response<T>>, req: ProtocolRequest<T>)
        -> Result<()>
    {
        let r = HandlerRequest { client: client, reply: reply, req: req };
        self.channel.send(r).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
    }

    // XXX shutdown

    pub fn await(self) -> Result<()> {
        match self.join_handle.join() {
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e))),
            Ok(r) => r,
        }
    }
}
