- `mio-server` - a tcp server (using [raw mio](https://github.com/carllerche/mio)) with a simple protocol
- `mio-client` - a client (using [raw mio 0.5](https://github.com/carllerche/mio)) to make N parallel requests against mio-server
- `mio06-client` - a client (using [raw mio 0.6](https://github.com/carllerche/mio)) to make N parallel requests against multiple mio-servers
- `blocking-client` - a client (using `std::net::TcpStream`) to make N serial requests against mio-server
- `futures-client` - a client (using [futures-rs](https://github.com/alexcrichton/futures-rs)) to make N parallel requests against mio-server
- `tokio-client` - a client (using [tokio-rs](https://github.com/tokio-rs/tokio)) to make N serial requests against mio-server
