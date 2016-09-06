use std::ascii;
use std::fmt;
use std::io::{self, Write};
use std::u32;
use std::net::SocketAddr;
use std::usize;
use std::thread;
use std::sync::mpsc;

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

impl<T: fmt::Debug> fmt::Debug for Request<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Request {{ bytes: {}, delay: {}, echo: {}, loopback_data: {:?} }}",
               self.bytes,
               self.delay,
               self.echo,
               self.loopback_data)
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

    fn reset<T>(&mut self, req: &Request<T>) {
        self.pos = 0;
        write!(&mut self.buf[..2], "{:02}", req.delay).unwrap();
        write!(&mut self.buf[2..8], "{:06}", req.bytes).unwrap();
        self.buf[8] = req.echo;
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

// --------------------------------------------------------------------

struct Conn {
    socket: TcpStream, // ~ the client socket
    interests: Ready, // ~ set of events this client is interested in
}

impl fmt::Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Conn {{ peer_addr: {:?}, local_addr: {:?}, interests: {:?} }}",
               self.socket.peer_addr(),
               self.socket.local_addr(),
               self.interests)
    }
}

impl Conn {
    fn new(socket: TcpStream) -> Self {
        Conn {
            socket: socket,
            interests: Ready::all(), // XXX register only writeable at the beginning
        }
    }

    fn register(&self, poll: &Poll, token: Token) -> io::Result<()> {
        debug!("registering: {:?}", self);
        poll.register(&self.socket, token, self.interests, PollOpt::oneshot())
    }

    /// Handle an I/O event for this connection.
    /// Return `true` if this connection is to be droped/closed, `false` otherwise.
    fn handle_event(&mut self, poll: &Poll, token: Token, _: &Event) -> io::Result<Progress> {
        // try!(poll.reregister(&self.socket, token, self.interests, PollOpt::oneshot()));
        Ok(Progress::Continue)
    }
}


/// Starts a new client. This will spawn a new background thread running
/// the I/O event loop to which the return `Client` will communicate to.
// note: the event loop is started immediately in a background thread
// such that when communicating with it through the client we can
// assume two distinct threads participating in the communication.
pub fn new<T: Send + 'static>() -> io::Result<Client<T>> {
    let poll = try!(Poll::new());
    let (tx, rx) = channel::channel();
    try!(poll.register(&rx, EVT_LOOP_RX_TOKEN, Ready::all(), PollOpt::oneshot()));
    let th = thread::spawn(move || {
        let evt_loop = EventLoop {
            conns: Slab::with_capacity(16),
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

// XXX implement drop to post a "close the connection" event to the event-loop
pub struct Connection<T> {
    conn_token: Token,
    tx: channel::Sender<Cmd<T>>,
}

impl<T> Connection<T> {
    pub fn request(&self, req: Request<T>, tx: mpsc::Sender<Response<T>>) -> io::Result<()> {
        send_cmd(&self.tx, Cmd::Request(req, tx))
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

struct EventLoop<T> {
    conns: Slab<Conn, Token>,
    poll: Poll,

    rx: channel::Receiver<Cmd<T>>,
}

enum Cmd<T> {
    Shutdown,
    Connect(SocketAddr, mpsc::SyncSender<io::Result<Token>>),
    Request(Request<T>, mpsc::Sender<Response<T>>),
}

enum Progress {
    Continue,
    Stop,
}

impl<T> EventLoop<T> {
    fn run(self) -> io::Result<()> {
        let r = self.run_();
        debug!("Leaving run loop: {:?}", r);
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
            Cmd::Request(req, tx) => {
                // XXX
            }
        }
        Ok(Progress::Continue)
    }
}
