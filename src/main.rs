use std::error::Error;
use std::future::Future;
use std::time::Duration;
use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").await
        .expect("Failed to bind to the port");
    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                eprintln!("failed to accept connection: {err}");
                continue;
            }
        };
        tokio::spawn(async move {
            let res = handle_connection(stream).await;
            match res {
                Ok(_) => {}
                Err(err) => eprintln!("{err:?}")
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    // echo -e '\x00\x00\x00\x02\x00\x11' | nc 127.0.0.1 9092
    let length = timeout(stream.read_u32()).await
        .context("Failed to read message length")?;
    if length > 1024 * 1024 * 1024 {
        anyhow::bail!("Message length {length} is too large, skipping");
    }
    let length = length as usize;

    let mut message = vec![0; length].into_boxed_slice();
    let read_len = timeout(stream.read_exact(&mut message)).await
        .context(format!("Failed to read message with expected size {length}"))?;
    assert_eq!(read_len, length);

    //println!("success {:?}", &message[..]);

    send_v0_message(&mut stream, 7, &[]).await?;

    Ok(())
}

async fn timeout<T: Sized, E: Error + Sized + Send + Sync + 'static>(future: impl Future<Output = Result<T, E>> + Sized) -> anyhow::Result<T> {
    let action = tokio::time::timeout(Duration::from_millis(1500), future);
    let result = action.await??;
    Ok(result)
}

async fn send_v0_message(stream: &mut TcpStream, correlation_id: i32, data: &[u8]) -> anyhow::Result<()> {
    let message_length = (data.len() + correlation_id.to_be_bytes().len()) as u32;
    let test = async move {
        stream.write_u32(message_length).await?;
        stream.write_i32(correlation_id).await?;
        stream.write(data).await?;
        Ok::<_, std::io::Error>(())
    };
    timeout(test).await?;
    Ok(())
}
