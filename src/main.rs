use std::future::Future;
use std::time::Duration;
use anyhow::Context;
use bytes::{BufMut, BytesMut};
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
    ApiVersions = 18,
}
impl TryFrom<i16> for ApiKey {
    type Error = ();
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == (Self::ApiVersions as i16) => Ok(Self::ApiVersions),
            _ => Err(()),
        }
    }
}

const MIN_VERSION: i16 = 0;
const MAX_VERSION: i16 = 4;

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
    loop {
        let request_data = timeout(read_request(stream)).await
            .context("failed to read request")?;
        //println!("request bytes len {:02X?}", (request_data.len() as i32).to_be_bytes());
        //println!("request bytes {:02X?}", request_data);

        let (header, request_data) = match parse_request_header(&request_data) {
            Ok(x) => x,
            Err(err) => match err {
                None => anyhow::bail!("failed to parse request before getting correlation id, can't respond"),
                Some((code, correlation_id)) => {
                    respond_error(stream, correlation_id, code, false).await?;
                    continue;
                },
            }
        };
        //println!("request header {:?}", request);

        match header.api_key {
            ApiKey::ApiVersions => handle_api_versions(stream, header, request_data).await?,
        }
    }
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

fn parse_request_header(tail: &[u8]) -> Result<(RequestHeader, &[u8]), Option<(ErrorCode, i32)>> {
    /*
Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING
     */
    let (api_key, tail) = split_i16(tail)
        .ok_or(None)?;
    let (api_version, tail) = split_i16(tail)
        .ok_or(None)?;
    let (correlation_id, tail) = split_i32(tail)
        .ok_or(None)?;

    let api_key = ApiKey::try_from(api_key)
        .ok().ok_or((ErrorCode::InvalidRequest, correlation_id))?;

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
    Ok((header, tail))
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

async fn handle_api_versions<'a, 'b>(stream: &'a mut TcpStream, header: RequestHeader<'b>, tail: &'b [u8]) -> anyhow::Result<()> {
    /*
ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER 
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER 
      api_key => INT16
      min_version => INT16
      max_version => INT16
    throttle_time_ms => INT32
     */
    if (header.api_version < 3) || (header.api_version > MAX_VERSION) {
        return respond_error(stream, header.correlation_id, ErrorCode::UnsupportedVersion, true).await;
    }

    let mut message = BytesMut::with_capacity(8);
    message.put_i16(ErrorCode::None as i16);

    put_compact_array_len(&mut message, Some(1));
    message.put_i16(ApiKey::ApiVersions as i16);
    message.put_i16(MIN_VERSION);
    message.put_i16(MAX_VERSION);

    put_compact_array_len(&mut message, None); // tag buffer after api keys

    message.put_i32(0); // throttle_time_ms

    put_compact_array_len(&mut message, None); // tag buffer in the end
    
    timeout(send_response(stream, header.correlation_id, &message, true)).await
        .context("failed to write response")?;
    Ok(())
}

fn put_compact_array_len(buf: &mut BytesMut, len: Option<u8>) {
    let value = match len {
        None => 0,
        Some(len) => len + 1,
    };
    put_unsigned_varint(buf, value);
}

fn put_unsigned_varint(buf: &mut BytesMut, value: u8) {
    assert!(value < 128, "unsigned varint is not properly implemented yet!");
    buf.put_u8(value);
}

async fn respond_error(stream: &mut TcpStream, correlation_id: i32, code: ErrorCode, is_header_v0: bool) -> anyhow::Result<()> {
    timeout(send_response(stream, correlation_id, (code as i16).to_be_bytes().as_slice(), is_header_v0)).await
        .context("failed to write response")?;
    Ok(())
}

async fn send_response(stream: &mut TcpStream, correlation_id: i32, data: &[u8], is_header_v0: bool) -> anyhow::Result<()> {
    /*
Response Header v0 => correlation_id 
  correlation_id => INT32

Response Header v1 => correlation_id TAG_BUFFER 
  correlation_id => INT32
     */
    let message_length = data.len() + correlation_id.to_be_bytes().len();
    stream.write_i32(message_length as i32).await?;
    stream.write_i32(correlation_id).await?;
    if !is_header_v0 {
        stream.write_u8(0).await?; // empty tag buffer, using unsigned varint
    }
    stream.write(data).await?;

    /*let mut debug = BytesMut::new();
    debug.put_i32(message_length as i32);
    debug.put_i32(correlation_id);
    if !is_header_v0 {
        debug.put_u8(0);
    }
    debug.put(data);
    println!("response bytes {:02X?}", debug);*/

    Ok(())
}

async fn timeout<T: Sized>(future: impl Future<Output = anyhow::Result<T>> + Sized) -> anyhow::Result<T> {
    let action = tokio::time::timeout(Duration::from_millis(1500), future);
    let result = action.await??;
    Ok(result)
}
