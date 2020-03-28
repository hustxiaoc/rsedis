#[macro_use(log, sendlog)]
extern crate logger;
extern crate command;
extern crate config;
extern crate database;
extern crate net2;
extern crate parser;
extern crate response;
extern crate util;

use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::{ TcpStream, TcpListener};
use tokio::sync::{
    oneshot,
    Mutex, MutexGuard,
    mpsc::{unbounded_channel}
};
use tokio::sync::oneshot::{Sender, Receiver};
use tokio::sync::mpsc::{ UnboundedReceiver, UnboundedSender};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime;

use std::sync::{Arc};
use std::time::Duration;
use std::thread;
use std::sync::mpsc::channel;

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

enum WorkerMessage {
    ClientRegister(command::Client),
    ClientDeRegister(usize),
    Request(usize, OwnedParsedCommand),
}

async fn process_client_request(mut stream: TcpStream, db: Arc<Mutex<Database>>, id: usize, tx: UnboundedSender<WorkerMessage>) {

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

    tx.send(WorkerMessage::ClientRegister(client));

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
                        0
                    }
                }
            };

            // client closed connection
            if len == 0 {
                stream_tx.send(None);
                tx.send(WorkerMessage::ClientDeRegister(id));
                // sendlog!(sender, Verbose, "Client closed connection");
                break;
            }

            parser.written += len;
        }

        // was there an error during the execution?
        let mut error = false;

        this_command = next_command;
        next_command = None;

        // println!("parsed_command {:?}", parsed_command);

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

        tx.send(WorkerMessage::Request((id), parsed_command.into_owned()));
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

    async fn start_listen(addr: &str, db: Arc<Mutex<Database>>, next_id: Arc<Mutex<usize>>, tx: UnboundedSender<WorkerMessage>) -> Result<(), Box<dyn std::error::Error>>  {

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
            tokio::spawn(process_client_request(socket, db.clone(), id, tx.clone()));
        }

        Ok(())
    }

    pub async fn start(&mut self, config: Config) -> Result<(), Box<dyn std::error::Error>>  {

        let (tx, mut rx) = unbounded_channel::<WorkerMessage>();
        // 开启db线程
        thread::Builder::new().name("db_thread".to_string()).spawn(move || {
            let mut rt = runtime::Builder::new().basic_scheduler().build().unwrap();
            rt.block_on(async move {
                let mut db = Database::new(config);
                let mut clients = HashMap::new();
                loop {
                    match rx.recv().await {
                        Some(WorkerMessage::ClientRegister(client)) => {
                            println!("client register");
                            clients.insert(client.id, client);
                        },

                        Some(WorkerMessage::ClientDeRegister(id)) => {
                            println!("client deregister");
                            clients.remove(&id);
                        },

                        Some(WorkerMessage::Request(id, cmd)) => {
                            let mut client = clients.get_mut(&id).unwrap();
                            let r = command::command(cmd.get_command(), &mut db, &mut client);
                            // let r = command::command1(cmd.get_command(), &mut db);

                            match r {
                                Ok(response) => {
                                    client.rawsender.send(Some(response));
                                },
                                _ => {

                                }
                            }
                        },
                        None => {

                        }
                    }
                }
            });
        });

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
            let txx = tx.clone();
            threads.push(tokio::spawn(async move {
                Self::start_listen(&addr, db, next_id, txx).await;
            }));
        }

        for thread in threads {
            thread.await;
        }

        Ok(())
    }

}
