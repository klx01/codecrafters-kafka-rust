use std::future::Future;
use std::time::Duration;
use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct RequestHeader<'a> {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&'a str>,
}

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
            let mut stream = stream;
            let res = handle_connection(&mut stream).await;
            match res {
                Ok(_) => {}
                Err(err) => eprintln!("{err:?}")
            }
        });
    }
}

async fn handle_connection(stream: &mut TcpStream) -> anyhow::Result<()> {
    // echo -e '\x00\x00\x00\x23\x00\x12\x00\x04\x46\xFD\xAD\x22\x00\x09\x6B\x61\x66\x6B\x61\x2D\x63\x6C\x69\x00\x0A\x6B\x61\x66\x6B\x61\x2D\x63\x6C\x69\x04\x30\x2E\x31\x00' | nc 127.0.0.1 9092
    let request_data = timeout(read_request(stream)).await
        .context("failed to read request")?;

    //println!("request bytes len {:02X?}", (request_data.len() as i32).to_be_bytes());
    //println!("request bytes {:02X?}", request_data);

    let request = parse_request(&request_data)
        .context("failed to parse request")?;

    //println!("request header {:?}", request);

    timeout(send_v0_message(stream, request.correlation_id, &[])).await
        .context("failed to write response")?;

    Ok(())
}

fn parse_request(tail: &[u8]) -> anyhow::Result<RequestHeader> {
    let (api_key, tail) = split_i16(tail)
        .context("failed to parse api_key")?;
    let (api_version, tail) = split_i16(tail)
        .context("failed to parse api_version")?;
    let (correlation_id, tail) = split_i32(tail)
        .context("failed to parse correlation_id")?;
    let (id_length, tail) = split_i16(tail)
        .context("failed to parse id_length")?;
    let (client_id, tail) = if id_length > 0 {
        let (client_id, tail) = split_utf8(tail, id_length as usize)
            .context("failed to parse client_id")?;
        (Some(client_id), tail)
    } else {
        (None, tail)
    };

    let header = RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    };
    Ok(header)
}

fn split_i16(tail: &[u8]) -> anyhow::Result<(i16, &[u8])> {
    let (data, tail) = split_bytes(tail, 2)
        .context("failed to read i16")?;
    Ok((i16::from_be_bytes(data.try_into().unwrap()), tail))
}

fn split_i32(tail: &[u8]) -> anyhow::Result<(i32, &[u8])> {
    let (data, tail) = split_bytes(tail, 4)
        .context("failed to read i32")?;
    Ok((i32::from_be_bytes(data.try_into().unwrap()), tail))
}

fn split_bytes(tail: &[u8], length: usize) -> anyhow::Result<(&[u8], &[u8])> {
    if tail.len() < length {
        anyhow::bail!("failed to read {length} bytes, data too short");
    }
    let (data, tail) = tail.split_at(length);
    Ok((data, tail))
}

fn split_utf8(tail: &[u8], byte_length: usize) -> anyhow::Result<(&str, &[u8])> {
    let (data, tail) = split_bytes(tail, byte_length)?;
    let data = std::str::from_utf8(data)?;
    Ok((data, tail))
}

async fn read_request(stream: &mut TcpStream) -> anyhow::Result<Box<[u8]>> {
    let length = stream.read_i32().await
        .context("Failed to read message length")?;
    if length > 1024 * 1024 * 1024 {
        anyhow::bail!("Message length {length} is too large, skipping");
    }
    let length = length as usize;

    let mut message = vec![0; length].into_boxed_slice();
    let read_len = stream.read_exact(&mut message).await
        .context(format!("Failed to read message with expected size {length}"))?;
    assert_eq!(read_len, length);
    Ok(message)
}

async fn timeout<T: Sized>(future: impl Future<Output = anyhow::Result<T>> + Sized) -> anyhow::Result<T> {
    let action = tokio::time::timeout(Duration::from_millis(1500), future);
    let result = action.await??;
    Ok(result)
}

async fn send_v0_message(stream: &mut TcpStream, correlation_id: i32, data: &[u8]) -> anyhow::Result<()> {
    let message_length = (data.len() + correlation_id.to_be_bytes().len()) as u32;
    stream.write_u32(message_length).await?;
    stream.write_i32(correlation_id).await?;
    stream.write(data).await?;
    Ok(())
}
