use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::{ TcpStream, TcpListener};
use tokio::sync::{
    Mutex, MutexGuard,
    mpsc::{unbounded_channel}
};
use tokio::sync::mpsc::{ UnboundedReceiver, UnboundedSender};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime;
use tokio::time::delay_for;
use tokio::time::{timeout, Duration};

use std::sync::{Arc};
use std::thread;

use config::Config;
use database::Database;
use logger::Level;
use parser::{OwnedParsedCommand, ParseError, Parser};
use response::{Response, ResponseError};

/// The database server
pub struct Server {
    /// An incremental id for new clients
    pub next_id: Arc<Mutex<usize>>,
}

enum WorkerMessage {
    ClientRegister(command::Client),
    ClientDeRegister(usize),
    Request(usize, OwnedParsedCommand),
    CheckExpire
}

async fn process_client_request(mut stream: TcpStream, id: usize, tx: UnboundedSender<WorkerMessage>) {

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
    // let sender = db.lock().await.config.logger.sender();

    tx.send(WorkerMessage::ClientRegister(client));

    loop {
        println!("client thread {:?}", thread::current().id());
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
                                // sendlog!(
                                //     sender,
                                //     Verbose,
                                //     "Protocol error from client: {:?}",
                                //     err
                                // );
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
        return Server {
            next_id: Arc::new(Mutex::new(0)),
            // hz_stop: None,
        };
    }

    async fn start_listen(addr: &str, next_id: Arc<Mutex<usize>>, tx: UnboundedSender<WorkerMessage>) -> Result<(), Box<dyn std::error::Error>>  {

        let mut listener = TcpListener::bind(addr).await?;

        println!("Listening on {:?}", addr);

        loop {
            let (mut socket, addr) = listener.accept().await?;
            let id = {
                let mut nid = next_id.lock().await;
                *nid += 1;
                *nid - 1
            };
            tokio::spawn(process_client_request(socket, id, tx.clone()));
        }

        Ok(())
    }

    pub async fn start(&mut self, config: Config) -> Result<(), Box<dyn std::error::Error>>  {

        let (tx, mut rx) = unbounded_channel::<WorkerMessage>();

        let addresses = config.addresses();
        println!("main thread {:?}", thread::current().id());

        let time_tx = tx.clone();

        tokio::spawn(async move {
            println!("time thread {:?}", thread::current().id());

            loop {
                delay_for(Duration::from_millis(1000 * 3)).await;
                time_tx.send(WorkerMessage::CheckExpire);
            }
        });
        
        tokio::spawn(async move {
            let mut clients = HashMap::new();
            let mut db = Database::new(config);
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

                    Some(WorkerMessage::CheckExpire) => {
                        // println!("check expire");
                    },

                    Some(WorkerMessage::Request(id, cmd)) => {
                        println!("db thread {:?}", thread::current().id());

                        let mut client = clients.get_mut(&id).unwrap();
                        let r = command::command(cmd.get_command(), &mut db, &mut client);

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

        // thread::Builder::new().name("db_thread".to_string()).spawn(move || {
        //     let mut rt = runtime::Builder::new().basic_scheduler().enable_time().build().unwrap();
        //
        //     rt.spawn(async move {
        //
        //     });
        //
        //     rt.block_on(async move {
        //
        //     });
        // });

        let mut list = vec![];
        for (host, port) in addresses {
            let addr = format!("{}:{}", host, port);
            let next_id = self.next_id.clone();
            let tx = tx.clone();
            list.push(tokio::spawn(async move {
                Self::start_listen(&addr, next_id, tx).await;
            }));
        }

        for handle in list {
            handle.await;
        }

        Ok(())
    }

}
