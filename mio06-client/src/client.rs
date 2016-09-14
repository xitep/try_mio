use std::ascii;
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;
use std::u32;
use std::usize;

use slab::Slab;

use mio::channel;
use mio::tcp::TcpStream;
use mio::{Event, Events, Poll, PollOpt, Ready, Token};

pub struct Request<T> {
    pub bytes: u32, // ~ the number of bytes to request
    pub delay: u8, // ~ the delay in seconds to request
    pub echo: u8, // ~ the byte to be echoed (`bytes` times repeated)
    pub loopback_data: T, // ~ user provided data to be returned with the response
}

pub struct Response<T> {
    pub req: Request<T>, // ~ the originating protocol request
    pub resp: io::Result<Vec<u8>>, // ~ the (protocol) response data
}

impl<T> fmt::Debug for Request<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Request {{ bytes: {}, delay: {}, echo: {} }}",
               self.bytes,
               self.delay,
               self.echo)
    }
}

impl<T: fmt::Debug> fmt::Debug for Response<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Response {{ req: {:?}, resp: {:?} }}",
               self.req,
               self.resp.as_ref().map(|xs| AsAscii(xs.as_slice())))
    }
}

impl<T> Request<T> {
    pub fn new(bytes: u32, delay: u8, echo: u8, loopback_data: T) -> Self {
        if bytes > 999_999 {
            panic!("Invalid bytes (bytes > 999999): {}", bytes);
        }
        if delay > 99 {
            panic!("Invalid delay (delay > 99): {}", delay);
        }
        Request {
            bytes: bytes,
            delay: delay,
            echo: echo,
            loopback_data: loopback_data,
        }
    }
}

struct RequestWriter {
    pos: u32,
    buf: [u8; 9],
}

impl fmt::Debug for RequestWriter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               r#"RequestWriter {{ pos: {}, buf: "{}" }}"#,
               self.pos,
               AsAscii(&self.buf[..]))
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

impl<'a> fmt::Debug for AsAscii<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r#""{}""#, self)
    }
}

impl RequestWriter {
    fn new() -> Self {
        RequestWriter {
            pos: u32::MAX,
            buf: [0; 9],
        }
    }

    /// Assigns new work to this writer.
    ///
    /// Panics if `self.done() == false`.
    fn assign<T>(&mut self, req: &Request<T>) {
        assert!(self.done());
        self.pos = 0;
        write!(&mut self.buf[..2], "{:02}", req.delay).unwrap();
        write!(&mut self.buf[2..8], "{:06}", req.bytes).unwrap();
        self.buf[8] = req.echo;
    }

    /// Determine whether this request writer is finished/done with
    /// writing out its underlying buffer and can be assigned a new
    /// work using `assign`.
    fn done(&self) -> bool {
        (self.pos as usize) >= self.buf.len()
    }

    // ~ tries to write out the rest of the request - if any
    // ~ returns 'true' if the request was fully written out
    fn write<W: Write>(&mut self, w: &mut W) -> io::Result<bool> {
        while (self.pos as usize) < self.buf.len() {
            // ~ try to write out the rest of our buffer
            let n = match w.write(&self.buf[self.pos as usize..]) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(false);
                    } else {
                        return Err(e);
                    }
                }
                Ok(0) => {
                    return Ok(false);
                }
                Ok(n) => n,
            };
            self.pos += n as u32;
        }
        Ok(true)
    }
}

const LEN_DIGITS: u8 = 6;

enum LengthReader {
    Done(u32),
    Pending(u8, u32),
}

impl LengthReader {
    fn new() -> Self {
        LengthReader::Pending(LEN_DIGITS, 0)
    }

    fn read<R: Read>(&mut self, r: &mut R) -> io::Result<u32> {
        match *self {
            LengthReader::Done(len) => Ok(len),
            LengthReader::Pending(mut digits_missing, mut curr) => {
                assert!(digits_missing != 0);
                let mut buf = [0u8; LEN_DIGITS as usize];
                match r.read(&mut buf[..digits_missing as usize]) {
                    Err(e) => return Err(e),
                    Ok(0) => {
                        return Err(io::Error::new(io::ErrorKind::WouldBlock, "read zero bytes"));
                    }
                    Ok(n) => {
                        for i in 0..n {
                            curr *= 10;
                            curr += (buf[i as usize] - b'0') as u32;
                        }
                        digits_missing -= n as u8;
                        if digits_missing == 0 {
                            *self = LengthReader::Done(curr);
                            return Ok(curr);
                        } else {
                            *self = LengthReader::Pending(digits_missing, curr);
                            return Err(io::Error::new(io::ErrorKind::WouldBlock,
                                                      "need more bytes"));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn test_lengthreader_read() {
    let src = b"0123456789";
    let mut c = io::Cursor::new(&src);
    let mut lr = LengthReader::new();
    let n = lr.read(&mut c).unwrap();
    assert_eq!(n, 12345);
    assert_eq!(c.bytes().next().unwrap().unwrap(), b'6');

    let src = b"0123";
    let mut lr = LengthReader::new();
    lr.read(&mut io::Cursor::new(&src)).unwrap_err();
    let src = b"99";
    let n = lr.read(&mut io::Cursor::new(&src)).unwrap();
    assert_eq!(n, 12399);
}

struct Buf {
    data: Vec<u8>,
    pos: u32,
}

impl Buf {
    fn new(size: u32) -> Buf {
        let mut v = Vec::with_capacity(size as usize);
        unsafe { v.set_len(size as usize) };
        Buf {
            data: v,
            pos: 0,
        }
    }

    fn into_vec(self) -> Vec<u8> {
        assert_eq!(self.pos as usize, self.data.len());
        self.data
    }

    fn read<R: Read>(&mut self, r: &mut R) -> io::Result<()> {
        loop {
            match r.read(&mut self.data.as_mut_slice()[self.pos as usize..]) {
                Err(e) => {
                    return Err(e);
                }
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "read zero bytes"));
                }
                Ok(n) => {
                    self.pos = self.pos + n as u32;
                    if self.pos as usize == self.data.len() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[test]
fn test_buf_read() {
    let src1 = [1u8, 2, 3, 4, 5];
    let src2 = [6u8, 7, 8, 9, 10, 11, 12];

    let mut b = Buf::new(10);
    let e = b.read(&mut io::Cursor::new(&src1)).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::WouldBlock);

    let mut c2 = &mut io::Cursor::new(&src2);
    b.read(&mut c2).unwrap();
    assert_eq!(c2.bytes().next().unwrap().unwrap(), 11u8);

    let v = b.into_vec();
    assert_eq!(v.as_slice(), &[1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

enum ResponseReader {
    Length(LengthReader),
    Body(Buf),
}

impl ResponseReader {
    fn new() -> Self {
        ResponseReader::Length(LengthReader::new())
    }

    fn read<R: Read>(&mut self, r: &mut R) -> io::Result<Vec<u8>> {
        // ~ first handle the length reader
        let body = if let ResponseReader::Length(ref mut lreader) = *self {
            let len = try!(lreader.read(r));
            if len == 0 {
                *lreader = LengthReader::new();
                return Ok(Vec::new());
            } else {
                Some(ResponseReader::Body(Buf::new(len)))
            }
        } else {
            None
        };
        // ~ switch state if necessary
        if let Some(body) = body {
            *self = body;
        }
        // ~ here we assert we're in the "body" state
        {
            let mut buf = if let ResponseReader::Body(ref mut buf) = *self {
                buf
            } else {
                panic!("not in BODY state!");
            };
            try!(buf.read(r));
        }
        // ~ here we change state again and deliver the response
        let body = mem::replace(self, ResponseReader::Length(LengthReader::new()));
        let v = match body {
            ResponseReader::Body(buf) => buf.into_vec(),
            _ => panic!("not in BODY state!"),
        };
        Ok(v)
    }
}

#[test]
fn test_responsereader_read_inonechunk() {
    let src = b"000010abcdefghilmnopqrstuvwxyz";
    let mut rr = ResponseReader::new();
    let xs = rr.read(&mut io::Cursor::new(&src)).unwrap();
    assert_eq!(xs.as_slice(), &src[6..16]);
}

#[test]
fn test_responsereader_read_intwochunks() {
    let src = b"000010abcdefghilmnopqrstuvwxyz";

    let mut rr = ResponseReader::new();
    let e = rr.read(&mut io::Cursor::new(&src[..10])).unwrap_err();
    assert_eq!(e.kind(), io::ErrorKind::WouldBlock);
    let xs = rr.read(&mut io::Cursor::new(&src[10..])).unwrap();
    assert_eq!(xs.as_slice(), &src[6..16]);
}


// --------------------------------------------------------------------

/// The event loop side implementation of a handling events in a
/// particular connection.
struct Conn<T> {
    socket: TcpStream, // ~ the client socket
    interests: Ready, // ~ set of events this client is interested in

    // ~ the queue of outstanding requests
    pending_reqs: VecDeque<ConnReq<T>>,
    // ~ the queue of requests written out and waiting for responses
    awaiting_reqs: VecDeque<ConnReq<T>>,

    // ~ helper to render a particular request on the wire
    req_writer: RequestWriter,
    // ~ helper to read a particular response from the wire
    resp_reader: ResponseReader,
}

/// A user request and the channel where to post the response to once
/// the request is handled.
struct ConnReq<T> {
    req: Request<T>,
    reply: mpsc::Sender<Response<T>>,
}

impl<T> fmt::Debug for Conn<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Conn {{ peer_addr: {:?}, local_addr: {:?}, \
                interests: {:?}, pending_reqs.len: {}, \
                awaiting_reqs.len(): {} }}",
               self.socket.peer_addr(),
               self.socket.local_addr(),
               self.interests,
               self.pending_reqs.len(),
               self.awaiting_reqs.len())
    }
}

impl<T> Conn<T> {
    fn new(socket: TcpStream) -> Self {
        Conn {
            socket: socket,
            interests: Ready::all(),
            pending_reqs: VecDeque::new(),
            awaiting_reqs: VecDeque::new(),
            req_writer: RequestWriter::new(),
            resp_reader: ResponseReader::new(),
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        poll.deregister(&self.socket)
    }

    fn register(&self, poll: &Poll, token: Token) -> io::Result<()> {
        poll.register(&self.socket, token, self.interests, PollOpt::oneshot())
    }

    fn reregister(&self, poll: &Poll, token: Token) -> io::Result<()> {
        poll.reregister(&self.socket, token, self.interests, PollOpt::oneshot())
    }

    fn push_request(&mut self, r: ConnReq<T>) -> Result<(), (ConnReq<T>, io::Error)> {
        self.pending_reqs.push_back(r);
        self.interests = self.interests | Ready::writable();
        Ok(())
    }

    /// Handle an I/O event for this connection.
    /// Return `true` if this connection is to be droped/closed, `false` otherwise.
    fn handle_event(&mut self, poll: &Poll, token: Token, evt: &Event) -> io::Result<Progress> {
        if evt.kind() == Ready::writable() {
            loop {
                if self.req_writer.done() {
                    if self.pending_reqs.is_empty() {
                        // ~ nothing to write and no new pending requests
                        self.interests = self.interests - Ready::writable();
                        try!(self.reregister(poll, token));
                        return Ok(Progress::Continue);
                    } else {
                        // ~ nothing to write but a pending request open
                        // ~ grab the head of the pending reqs and prepare
                        // it for being written out
                        // ~ @unwrap: here we are guaranteed
                        // `pending_reqs` is not empty
                        self.req_writer.assign(&self.pending_reqs.front().unwrap().req);
                    }
                }

                // XXX do not fail here ... instead handle the error by
                // replying to the requestor `self.pending_reqs.get(0).reply`
                if try!(self.req_writer.write(&mut self.socket)) {
                    // @unwrap(): `pending_reqs` still contains the req
                    // the writer just finished writing; now move it to
                    // the `awaiting_reqs`
                    self.awaiting_reqs.push_back(self.pending_reqs.pop_front().unwrap());
                    self.interests = self.interests | Ready::readable();
                    debug!("Fully written out request: {:?}", self);
                } else {
                    self.interests = self.interests | Ready::writable();
                    debug!("Could not fully write out request: {:?}", self);
                    break;
                }
            }
            try!(self.reregister(poll, token));
            return Ok(Progress::Continue);
        }

        if evt.kind() == Ready::readable() {
            let data = match self.resp_reader.read(&mut self.socket) {
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        self.interests = self.interests | Ready::readable();
                        try!(self.reregister(poll, token));
                        return Ok(Progress::Continue);
                    }
                    // XXX do not simply fail here ... instead notify
                    // the clients on the awaiting_reqs
                    debug!("Stopping conn upon error: {:?} => {:?}", self, e);
                    return Ok(Progress::Stop);
                }
                Ok(data) => data,
            };
            assert!(!self.awaiting_reqs.is_empty());
            let req = self.awaiting_reqs.pop_front().unwrap();
            // ~ do not fail if the send failed; the client might not
            // be listening anymore
            // ~ XXX we could cancel all pending_reqs (and maybe
            // waiting_reqs) for this reply channel
            let _ = req.reply.send(Response {
                req: req.req,
                resp: Ok(data),
            });

            if !self.awaiting_reqs.is_empty() {
                self.interests = self.interests | Ready::readable();
                try!(self.reregister(poll, token));
            }
            return Ok(Progress::Continue);
        }

        // XXX handle error and hang-ups
        debug!("Handling event ({:?}) is not yet implemented: {:?}", evt, self);

        // try!(poll.reregister(&self.socket, token, self.interests, PollOpt::oneshot()));
        Ok(Progress::Continue)
    }
}


/// Starts a new client. This will spawn a new background thread running
/// the I/O event loop to which the return `Client` will communicate to.
// note: the event loop is started immediately in a background thread
// such that when communicating with it through the client we can
// assume two distinct threads participating in the communication.
pub fn new<T: Send + 'static>(max_conns: usize) -> io::Result<Client<T>> {
    let poll = try!(Poll::new());
    let (tx, rx) = channel::channel();
    try!(poll.register(&rx, EVT_LOOP_RX_TOKEN, Ready::all(), PollOpt::oneshot()));
    let th = thread::spawn(move || {
        let evt_loop = EventLoop {
            conns: Slab::with_capacity(max_conns),
            poll: poll,
            rx: rx,
        };
        evt_loop.run()
    });
    Ok(Client { tx: tx, th: th })
}

/// An asynchronous client for the `try_mio` servers/protocol.
pub struct Client<T> {
    tx: channel::Sender<Cmd<T>>,
    th: thread::JoinHandle<io::Result<()>>,
}

/// The client side handle to an opened connection.
pub struct Connection<T> {
    conn_token: Token,
    tx: channel::Sender<Cmd<T>>,
}

impl<T> Drop for Connection<T> {
    fn drop(&mut self) {
        if self.conn_token != CONN_DISCONNECTED {
            let _ = self.tx.send(Cmd::DisconnectSilently(self.conn_token));
        }
    }
}

impl<T> Connection<T> {
    pub fn request(&self, req: Request<T>, tx: mpsc::Sender<Response<T>>) -> io::Result<()> {
        send_cmd(&self.tx, Cmd::Request(self.conn_token, req, tx))
    }

    pub fn close(mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        try!(send_cmd(&self.tx, Cmd::Disconnect(self.conn_token, tx)));
        self.conn_token = CONN_DISCONNECTED;
        match rx.recv().map_err(|_| end_of_evtloop()) {
            Err(e) => Err(e),
            Ok(r) => r,
        }
    }
}

impl<T> Client<T> {
    pub fn shutdown(self) -> io::Result<()> {
        try!(self.cmd(Cmd::Shutdown));
        self.th.join().unwrap()
    }

    pub fn connect(&self, addr: &SocketAddr) -> io::Result<Connection<T>> {
        let (tx, rx) = mpsc::sync_channel(1);
        try!(self.cmd(Cmd::Connect(addr.clone(), tx)));
        try!(rx.recv().map_err(|_| end_of_evtloop())).map(|tok| {
            Connection {
                conn_token: tok,
                tx: self.tx.clone(),
            }
        })
    }

    fn cmd(&self, cmd: Cmd<T>) -> io::Result<()> {
        send_cmd(&self.tx, cmd)
    }
}

fn send_cmd<T>(tx: &channel::Sender<Cmd<T>>, cmd: Cmd<T>) -> io::Result<()> {
    match tx.send(cmd) {
        Err(channel::SendError::Io(e)) => Err(e),
        // XXX ideally foward the error - if any - of the finished run loop
        Err(channel::SendError::Disconnected(_)) => Err(end_of_evtloop()),
        Ok(_) => Ok(()),
    }
}

fn end_of_evtloop() -> io::Error {
    io::Error::new(io::ErrorKind::Other, "event loop not running?!")
}

const EVT_LOOP_RX_TOKEN: Token = Token(usize::MAX - 1);
const CONN_DISCONNECTED: Token = Token(usize::MAX - 2);

struct EventLoop<T> {
    conns: Slab<Conn<T>, Token>,
    poll: Poll,

    rx: channel::Receiver<Cmd<T>>,
}

enum Cmd<T> {
    Shutdown,
    Connect(SocketAddr, mpsc::SyncSender<io::Result<Token>>),
    Request(Token, Request<T>, mpsc::Sender<Response<T>>),
    Disconnect(Token, mpsc::SyncSender<io::Result<()>>),
    DisconnectSilently(Token),
}

enum Progress {
    Continue,
    Stop,
}

impl<T> EventLoop<T> {
    fn run(self) -> io::Result<()> {
        let r = self.run_();
        warn!("Leaving run loop: {:?}", r);
        r
    }

    fn run_(mut self) -> io::Result<()> {
        let mut evts = Events::with_capacity(1024);
        'runloop: loop {
            try!(self.poll.poll(&mut evts, None));
            for e in &evts {
                debug!("Received event: {:?}", e);
                match e.token() {
                    EVT_LOOP_RX_TOKEN => {
                        match try!(self.handle_cmd(&e)) {
                            Progress::Continue => {
                                try!(self.poll.reregister(&self.rx,
                                                          EVT_LOOP_RX_TOKEN,
                                                          Ready::all(),
                                                          PollOpt::oneshot()));
                            }
                            Progress::Stop => {
                                break 'runloop;
                            }
                        }
                    }
                    tok => {
                        match {
                            let mut conn = &mut self.conns[tok];
                            try!(conn.handle_event(&self.poll, tok, &e))
                        } {
                            Progress::Continue => {}
                            Progress::Stop => {
                                if let Some(conn) = self.conns.remove(tok) {
                                    try!(self.poll.deregister(&conn.socket));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Process an event arriving at the command receiver `self.rx`.
    /// Return `true` if the event loop is to be stopped, `false`
    /// otherwise.
    fn handle_cmd(&mut self, _: &Event) -> io::Result<Progress> {
        let cmd = match self.rx.try_recv() {
            Err(mpsc::TryRecvError::Disconnected) => {
                trace!("No clients anymore. Stopping event loop.");
                return Ok(Progress::Stop);
            }
            Err(mpsc::TryRecvError::Empty) => {
                return Ok(Progress::Continue);
            }
            Ok(cmd) => cmd,
        };
        match cmd {
            Cmd::Shutdown => {
                // XXX close all open connections
                trace!("cmd::shutdown: Stopping event loop.");
                // XXX might want to make this rather a graceful shutdown
                return Ok(Progress::Stop);
            }
            Cmd::Connect(addr, tx) => {
                match self.conns.vacant_entry() {
                    None => {
                        tx.send(Err(io::Error::new(io::ErrorKind::Other,
                                                     "too many connections!")))
                            .expect("client stopped waiting for answer?");
                    }
                    Some(e) => {
                        match TcpStream::connect(&addr) {
                            Ok(stream) => {
                                let conn = Conn::new(stream);
                                try!(conn.register(&self.poll, e.index()));
                                let tok = e.insert(conn).index();
                                tx.send(Ok(tok)).expect("client stopped waiting for answer?");
                            }
                            Err(e) => {
                                tx.send(Err(e)).expect("client stopped waiting for answer?");
                            }
                        }
                    }
                }
            }
            Cmd::Request(conn_token, req, tx) => {
                match self.conns.get_mut(conn_token) {
                    None => {
                        // ~ don't expect the message to be actually
                        // delivered, the receiver is not under our
                        // control
                        let _ = tx.send(Response {
                            req: req,
                            resp: Err(io::Error::new(io::ErrorKind::Other, "no such connection")),
                        });
                    }
                    Some(c) => {
                        debug!("Pushing request: {:?} on conn {:?}", req, conn_token);
                        match c.push_request(ConnReq {
                            req: req,
                            reply: tx,
                        }) {
                            Ok(_) => {
                                try!(c.reregister(&self.poll, conn_token));
                            }
                            Err((creq, e)) => {
                                // ~ don't expect the message to be actually
                                // delivered, the receiver is not under our control
                                let _ = creq.reply.send(Response {
                                    req: creq.req,
                                    resp: Err(e),
                                });
                            }
                        }
                    }
                }
            }
            Cmd::Disconnect(token, tx) => {
                match self.conns.remove(token) {
                    None => {
                        tx.send(Ok(())).expect("client stopped waiting for answer?");
                    }
                    Some(conn) => {
                        let _ = conn.deregister(&self.poll);
                        tx.send(Ok(())).expect("client stopped waiting for answer?");
                    }
                };
            }
            Cmd::DisconnectSilently(token) => {
                if let Some(conn) = self.conns.remove(token) {
                    let _ = conn.deregister(&self.poll);
                }
            }
        }
        Ok(Progress::Continue)
    }
}
