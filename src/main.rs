use std::future::Future;
use std::time::Duration;
use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct RequestHeader<'a> {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&'a str>,
}

#[derive(Debug)]
enum ErrorCode {
    Unknown = -1,
    None = 0,
    MessageTooLarge = 10,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
}
impl TryFrom<i16> for ErrorCode {
    type Error = ();
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == (Self::Unknown as i16) => Ok(Self::Unknown),
            x if x == (Self::None as i16) => Ok(Self::None),
            x if x == (Self::MessageTooLarge as i16) => Ok(Self::MessageTooLarge),
            x if x == (Self::UnsupportedVersion as i16) => Ok(Self::UnsupportedVersion),
            x if x == (Self::InvalidRequest as i16) => Ok(Self::InvalidRequest),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    ApiVersions = 18,
}
impl TryFrom<i16> for ApiKey {
    type Error = ();
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == (Self::Produce as i16) => Ok(Self::Produce),
            x if x == (Self::Fetch as i16) => Ok(Self::Fetch),
            x if x == (Self::ListOffsets as i16) => Ok(Self::ListOffsets),
            x if x == (Self::Metadata as i16) => Ok(Self::Metadata),
            x if x == (Self::LeaderAndIsr as i16) => Ok(Self::LeaderAndIsr),
            x if x == (Self::ApiVersions as i16) => Ok(Self::ApiVersions),
            _ => Err(()),
        }
    }
}

const ALLOWED_VERSIONS: [i16; 5] = [0, 1, 2, 3, 4];

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

    let request = match parse_request(&request_data) {
        Ok(x) => x,
        Err(err) => match err {
            None => anyhow::bail!("failed to parse request before getting correlation id, can't respond"),
            Some((code, correlation_id)) => {
                timeout(send_v0_message(stream, correlation_id, (code as i16).to_be_bytes().as_slice())).await
                    .context("failed to write response")?;
                return Ok(());
            },
        }
    };

    //println!("request header {:?}", request);

    timeout(send_v0_message(stream, request.correlation_id, &[])).await
        .context("failed to write response")?;

    Ok(())
}

fn parse_request(tail: &[u8]) -> Result<RequestHeader, Option<(ErrorCode, i32)>> {
    let (api_key, tail) = split_i16(tail)
        .ok_or(None)?;
    let (api_version, tail) = split_i16(tail)
        .ok_or(None)?;
    let (correlation_id, tail) = split_i32(tail)
        .ok_or(None)?;

    let api_key = ApiKey::try_from(api_key)
        .ok().ok_or((ErrorCode::InvalidRequest, correlation_id))?;
    if !ALLOWED_VERSIONS.contains(&api_version) {
        return Err(Some((ErrorCode::UnsupportedVersion, correlation_id)));
    }

    let (id_length, tail) = split_i16(tail)
        .ok_or((ErrorCode::InvalidRequest, correlation_id))?;
    let (client_id, tail) = if id_length > 0 {
        let (client_id, tail) = split_utf8(tail, id_length as usize)
            .ok_or((ErrorCode::InvalidRequest, correlation_id))?;
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

fn split_i16(tail: &[u8]) -> Option<(i16, &[u8])> {
    let (data, tail) = split_bytes(tail, 2)?;
    Some((i16::from_be_bytes(data.try_into().unwrap()), tail))
}

fn split_i32(tail: &[u8]) -> Option<(i32, &[u8])> {
    let (data, tail) = split_bytes(tail, 4)?;
    Some((i32::from_be_bytes(data.try_into().unwrap()), tail))
}

fn split_bytes(tail: &[u8], length: usize) -> Option<(&[u8], &[u8])> {
    if tail.len() < length {
        return None;
    }
    let (data, tail) = tail.split_at(length);
    Some((data, tail))
}

fn split_utf8(tail: &[u8], byte_length: usize) -> Option<(&str, &[u8])> {
    let (data, tail) = split_bytes(tail, byte_length)?;
    let data = std::str::from_utf8(data).ok()?;
    Some((data, tail))
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
