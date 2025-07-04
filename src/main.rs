#![allow(unused_imports)]
use std::io::Read;
use std::io::Write;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
//use std::net::TcpListener;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    
    loop {
        
        let stream = listener.accept().await; 
        match stream {
           
            Ok((mut stream,_)) => {
                println!("accepted new connection plz, handle it plzzz !");
                
                tokio::spawn(async move {
               
                let mut buf = [0; 512];
                loop {
                    let read_count = stream.read(&mut buf).await.unwrap();
                    if read_count == 0 {
                        break;
                    }
                
                    stream.write(b"+PONG\r\n").await.unwrap();
                }       
            });
            } 
            Err(e) => {
                println!("error: {}", e);
             }
        }
    }
}
