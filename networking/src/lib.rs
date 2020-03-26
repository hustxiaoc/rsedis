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
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::{ TcpStream, TcpListener};
use tokio::sync::{
    Mutex, MutexGuard,
    mpsc::{unbounded_channel}
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::sync::{Arc};
use std::thread;
use std::time::Duration;

use config::Config;
use database::Database;
use logger::Level;
use parser::{OwnedParsedCommand, ParseError, Parser};
use response::{Response, ResponseError};

/// The database server
pub struct Server {
    /// A reference to the database
    db: Arc<Mutex<Database>>,
    /// A list of channels listening for incoming connections
    /// A list of threads listening for incoming connections
    /// An incremental id for new clients
    pub next_id: Arc<Mutex<usize>>,
}

async fn process_client_request(mut stream: TcpStream, db: Arc<Mutex<Database>>, id: usize) {

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
    let sender = db.lock().await.config.logger.sender();

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

        let mut db = db.lock().await;
        let r = command::command(parsed_command, &mut *db, &mut client);

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

impl Server {
    /// Creates a new server
    pub fn new(config: Config) -> Server {
        let db = Database::new(config);
        return Server {
            db: Arc::new(Mutex::new(db)),
            next_id: Arc::new(Mutex::new(0)),
            // hz_stop: None,
        };
    }

    pub async fn get_mut_db<'a>(&'a self) -> MutexGuard<'a, database::Database> {
        self.db.lock().await
    }

    async fn start_listen(addr: &str, db: Arc<Mutex<Database>>, next_id: Arc<Mutex<usize>>) -> Result<(), Box<dyn std::error::Error>>  {

        let sender = db.lock().await.config.logger.sender();

        let mut listener = TcpListener::bind(addr).await?;

        println!("Listening on {:?}", addr);

        loop {
            let (mut socket, addr) = listener.accept().await?;
            let id = {
                let mut nid = next_id.lock().await;
                *nid += 1;
                *nid - 1
            };
            tokio::spawn(process_client_request(socket, db.clone(), id));
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        use tokio::net::{ TcpStream, TcpListener};

        let (tcp_keepalive, timeout, addresses, tcp_backlog) = {
            let db = self.db.lock().await;
            (
                db.config.tcp_keepalive.clone(),
                db.config.timeout.clone(),
                db.config.addresses().clone(),
                db.config.tcp_backlog.clone(),
            )
        };

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

        Ok(())
    }

}
