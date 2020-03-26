#[cfg(unix)]
extern crate libc;
#[cfg(unix)]
extern crate unix_socket;
#[macro_use(log, sendlog)]
extern crate logger;
extern crate command;
extern crate config;
extern crate database;
extern crate net2;
extern crate parser;
extern crate response;
extern crate util;

use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
use libc::funcs::c95::stdlib::exit;
#[cfg(unix)]
use libc::funcs::posix88::unistd::fork;
#[cfg(unix)]
use libc::funcs::posix88::unistd::getpid;
use net2::{TcpBuilder, TcpStreamExt};
#[cfg(unix)]
use unix_socket::{UnixListener, UnixStream};

use config::Config;
use database::Database;
use logger::Level;
use parser::{OwnedParsedCommand, ParseError, Parser};
use response::{Response, ResponseError};

/// A stream connection.
#[cfg(unix)]
enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

#[cfg(not(unix))]
enum Stream {
    Tcp(TcpStream),
}

#[cfg(unix)]
impl Stream {
    /// Creates a new independently owned handle to the underlying socket.
    fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Stream::Tcp(ref s) => Ok(Stream::Tcp(s.try_clone()?)),
            Stream::Unix(ref s) => Ok(Stream::Unix(s.try_clone()?)),
        }
    }

    /// Write a buffer into this object, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
            Stream::Unix(ref mut s) => s.write(buf),
        }
    }

    /// Sets the keepalive timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => TcpStreamExt::set_keepalive(s, duration),
            Stream::Unix(_) => Ok(()),
        }
    }

    /// Sets the write timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_write_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }

    /// Sets the read timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_read_timeout(dur),
            // TODO: couldn't figure out how to enable this in unix_socket
            Stream::Unix(_) => Ok(()),
        }
    }
}

#[cfg(unix)]
impl Read for Stream {
    /// Pull some bytes from this source into the specified buffer,
    /// returning how many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
            Stream::Unix(ref mut s) => s.read(buf),
        }
    }
}

#[cfg(not(unix))]
impl Stream {
    /// Creates a new independently owned handle to the underlying socket.
    fn try_clone(&self) -> io::Result<Stream> {
        match *self {
            Stream::Tcp(ref s) => Ok(Stream::Tcp(s.try_clone()?)),
        }
    }

    /// Write a buffer into this object, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.write(buf),
        }
    }

    /// Sets the keepalive timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => TcpStreamExt::set_keepalive(s, duration),
        }
    }

    /// Sets the write timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_write_timeout(dur),
        }
    }

    /// Sets the read timeout to the timeout specified.
    /// It fails silently for UNIX sockets.
    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match *self {
            Stream::Tcp(ref s) => s.set_read_timeout(dur),
        }
    }
}

#[cfg(not(unix))]
impl Read for Stream {
    /// Pull some bytes from this source into the specified buffer,
    /// returning how many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Stream::Tcp(ref mut s) => s.read(buf),
        }
    }
}

/// A client connection
struct Client {
    /// The socket connection
    stream: Stream,
    /// A reference to the database
    db: Arc<Mutex<Database>>,
    /// The client unique identifier
    id: usize,
}

/// The database server
pub struct Server {
    /// A reference to the database
    db: Arc<Mutex<Database>>,
    /// A list of channels listening for incoming connections
    listener_channels: Vec<Sender<u8>>,
    /// A list of threads listening for incoming connections
    listener_threads: Vec<thread::JoinHandle<()>>,
    /// An incremental id for new clients
    pub next_id: Arc<Mutex<usize>>,
    /// Sender to signal hz thread to stop
    hz_stop: Option<Sender<()>>,
}

impl Client {
    /// Creates a new TCP socket client
    pub fn tcp(stream: TcpStream, db: Arc<Mutex<Database>>, id: usize) -> Client {
        return Client {
            stream: Stream::Tcp(stream),
            db: db,
            id: id,
        };
    }

    /// Creates a new UNIX socket client
    #[cfg(unix)]
    pub fn unix(stream: UnixStream, db: Arc<Mutex<Database>>, id: usize) -> Client {
        return Client {
            stream: Stream::Unix(stream),
            db: db,
            id: id,
        };
    }

    /// Creates a thread that writes into the client stream each response received
    fn create_writer_thread(
        &self,
        sender: Sender<(Level, String)>,
        rx: Receiver<Option<Response>>,
    ) {
        let mut stream = self.stream.try_clone().unwrap();
        thread::spawn(move || loop {
            match rx.recv() {
                Ok(m) => match m {
                    Some(msg) => match stream.write(&*msg.as_bytes()) {
                        Ok(_) => (),
                        Err(e) => {
                            sendlog!(sender, Warning, "Error writing to client: {:?}", e).unwrap()
                        }
                    },
                    None => break,
                },
                Err(_) => break,
            };
        });
    }

    pub async fn process(mut stream: tokio::net::TcpStream, db: Arc<Mutex<Database>>, id: usize) {
        use tokio::sync::mpsc::unbounded_channel;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (stream_tx, mut rx) = unbounded_channel::<Option<Response>>();
        let (mut reader, mut writer) = tokio::io::split(stream);

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(m) => match m {
                        Some(msg) => match writer.write(&*msg.as_bytes()).await {
                            Ok(_) => (),
                            Err(e) => {
                                // sendlog!(sender, Warning, "Error writing to client: {:?}", e).unwrap()
                            }
                        },
                        None => break,
                    },
                    _ => break,
                }
            }
        });

        let mut client = command::Client::new(stream_tx.clone(), id);
        let mut parser = Parser::new();
        let mut this_command: Option<OwnedParsedCommand>;
        let mut next_command: Option<OwnedParsedCommand> = None;
        let sender = db.lock().unwrap().config.logger.sender();

        loop {
            if next_command.is_none() && parser.is_incomplete() {
                parser.allocate();
                let len = {
                    let pos = parser.written;
                    let mut buffer = parser.get_mut();

                    // read socket
                    match reader.read(&mut buffer[pos..]).await {
                        Ok(r) => r,
                        Err(err) => {
                            // sendlog!(sender, Verbose, "Reading from client: {:?}", err);
                            break;
                        }
                    }
                };
                parser.written += len;

                // client closed connection
                if len == 0 {
                    // sendlog!(sender, Verbose, "Client closed connection");
                    break;
                }
            }

            // was there an error during the execution?
            let mut error = false;

            this_command = next_command;
            next_command = None;

            // try to parse received command
            let parsed_command = match this_command {
                Some(ref c) => c.get_command(),
                None => {
                    match parser.next() {
                        Ok(p) => p,
                        Err(err) => {
                            match err {
                                // if it's incomplete, keep adding to the buffer
                                ParseError::Incomplete => {
                                    continue;
                                }
                                ParseError::BadProtocol(s) => {
                                    let _ = stream_tx.send(Some(Response::Error(s)));
                                    break;
                                }
                                _ => {
                                    sendlog!(
                                        sender,
                                        Verbose,
                                        "Protocol error from client: {:?}",
                                        err
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            };

            println!("parsed_command {:?}", parsed_command);

            let r = {
                let mut db = match db.lock() {
                    Ok(db) => db,
                    Err(_) => break,
                };
                command::command(parsed_command, &mut *db, &mut client)
            };

            // check out the response
            match r {
                // received a response, send it to the client
                Ok(response) => {
                    match stream_tx.send(Some(response)) {
                        Ok(_) => (),
                        Err(_) => error = true,
                    };
                }
                // no response
                Err(err) => {
                    match err {
                        // There is no reply to send, that's ok
                        ResponseError::NoReply => (),
                        // We have to wait until a sender signals us back and then retry
                        // (Repeating the same command is actually wrong because of the timeout)
                        ResponseError::Wait(mut receiver) => {
                            // if we receive a None, send a nil, otherwise execute the command
                            match receiver.recv().await.unwrap() {
                                Some(cmd) => next_command = Some(cmd),
                                None => match stream_tx.send(Some(Response::Nil)) {
                                    Ok(_) => (),
                                    Err(_) => error = true,
                                },
                            }
                        }
                    }
                }
            }

            // if something failed, let's shut down the client
            if error {
                // kill threads
                stream_tx.send(None);
                client.rawsender.send(None);
                break;
            }
        }
    }

}

impl Server {
    /// Creates a new server
    pub fn new(config: Config) -> Server {
        let db = Database::new(config);
        return Server {
            db: Arc::new(Mutex::new(db)),
            listener_channels: Vec::new(),
            listener_threads: Vec::new(),
            next_id: Arc::new(Mutex::new(0)),
            hz_stop: None,
        };
    }

    pub fn get_mut_db<'a>(&'a self) -> MutexGuard<'a, database::Database> {
        self.db.lock().unwrap()
    }

    /// Runs the server. If `config.daemonize` is true, it forks and exits.
    #[cfg(unix)]
    pub fn run(&mut self) {
        let (daemonize, pidfile) = {
            let db = self.db.lock().unwrap();
            (db.config.daemonize.clone(), db.config.pidfile.clone())
        };
        if daemonize {
            unsafe {
                match fork() {
                    -1 => panic!("Fork failed"),
                    0 => {
                        if let Ok(mut fp) = File::create(Path::new(&*pidfile)) {
                            match write!(fp, "{}", getpid()) {
                                Ok(_) => (),
                                Err(e) => {
                                    let db = self.db.lock().unwrap();
                                    log!(db.config.logger, Warning, "Error writing pid: {}", e);
                                }
                            }
                        }
                        self.start();
                        self.join();
                    }
                    _ => exit(0),
                };
            }
        } else {
            self.start();
            self.join();
        }
    }

    #[cfg(not(unix))]
    pub fn run(&mut self) {
        let daemonize = {
            let db = self.db.lock().unwrap();
            db.config.daemonize
        };
        if daemonize {
            panic!("Cannot daemonize in non-unix");
        } else {
            self.start();
            self.join();
        }
    }

    #[cfg(windows)]
    fn reuse_address(&self, _: &TcpBuilder) -> io::Result<()> {
        Ok(())
    }

    #[cfg(not(windows))]
    fn reuse_address(&self, builder: &TcpBuilder) -> io::Result<()> {
        builder.reuse_address(true)?;
        Ok(())
    }

    /// Join the listener threads.
    pub fn join(&mut self) {
        #![allow(unused_must_use)]
        while self.listener_threads.len() > 0 {
            self.listener_threads.pop().unwrap().join();
        }
    }

    async fn start_listen(addr: &str, db: Arc<Mutex<Database>>, next_id: Arc<Mutex<usize>>) -> Result<(), Box<dyn std::error::Error>>  {
        use tokio::net::{ TcpStream, TcpListener};
        use tokio::runtime::Runtime;
        use tokio::sync::mpsc::{unbounded_channel};

        let sender = db.lock().unwrap().config.logger.sender();
        // let next_id = $server.next_id.clone();

        let mut listener = TcpListener::bind(addr).await?;

        println!("Listening on {:?}", addr);

        loop {
            let (mut socket, addr) = listener.accept().await?;
            // let (tx, rx) = unbounded_channel();
            let id = {
                let mut nid = next_id.lock().unwrap();
                *nid += 1;
                *nid - 1
            };
            tokio::spawn(Client::process(socket, db.clone(), id));
        }

        Ok(())
    }

    /// Starts threads listening to new connections.
    pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        use tokio::net::{ TcpStream, TcpListener};
        use tokio::runtime::Runtime;

        let (tcp_keepalive, timeout, addresses, tcp_backlog) = {
            let db = self.db.lock().unwrap();
            (
                db.config.tcp_keepalive.clone(),
                db.config.timeout.clone(),
                db.config.addresses().clone(),
                db.config.tcp_backlog.clone(),
            )
        };

        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let mut threads = vec![];
            for (host, port) in addresses {
                let addr = format!("{}:{}", host, port);
                let db = self.db.clone();
                let next_id = self.next_id.clone();
                threads.push(tokio::spawn(async move {
                    Self::start_listen(&addr, db, next_id).await;
                }));
            }

            for thread in threads {
                thread.await;
            }
        });

        Ok(())
    }

    #[cfg(not(unix))]
    fn handle_unixsocket(&mut self) {
        let db = self.db.lock().unwrap();
        if db.config.unixsocket.is_some() {
            let _ = writeln!(
                &mut std::io::stderr(),
                "Ignoring unixsocket in non unix environment\n"
            );
        }
    }

    /// Sends a kill signal to the listeners and connects to the incoming
    /// connections to break the listening loop.
    pub fn stop(&mut self) {
        #![allow(unused_must_use)]
        for sender in self.listener_channels.iter() {
            sender.send(0);
            let db = self.db.lock().unwrap();
            for (host, port) in db.config.addresses() {
                for addrs in (&host[..], port).to_socket_addrs().unwrap() {
                    TcpStream::connect(addrs);
                }
            }
        }
        match self.hz_stop {
            Some(ref t) => {
                let _ = t.send(());
            }
            None => (),
        }
        self.join();
    }
}
