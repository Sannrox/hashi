//! Replication-mode wire session. tokio-postgres has no CopyBoth or
//! `replication=database`, so snapshot SQL stays on that client and the
//! slot, start, and WAL stream use this session.

use super::Config;
use crate::Error;
use bytes::{Bytes, BytesMut};
use postgres_protocol::authentication::{self, sasl};
use postgres_protocol::message::frontend;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

enum Conn {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

pub(super) struct Replication {
    conn: Conn,
    buf: BytesMut,
}

impl Replication {
    pub(super) async fn connect(config: &Config) -> Result<Self, Error> {
        let mut tcp = TcpStream::connect((config.host.as_str(), config.port))
            .await
            .map_err(|_| Error::new("PostgreSQL replication connection failed"))?;
        tcp.set_nodelay(true)
            .map_err(|_| Error::new("PostgreSQL replication connection failed"))?;
        let conn = if config.allow_insecure_lab {
            Conn::Plain(tcp)
        } else {
            let mut ssl = BytesMut::new();
            frontend::ssl_request(&mut ssl);
            tcp.write_all(&ssl)
                .await
                .map_err(|_| Error::new("PostgreSQL replication TLS request failed"))?;
            let mut reply = [0u8; 1];
            tcp.read_exact(&mut reply)
                .await
                .map_err(|_| Error::new("PostgreSQL replication TLS request failed"))?;
            if reply[0] != b'S' {
                return Err(Error::new("PostgreSQL replication requires TLS"));
            }
            let connector = tokio_rustls::TlsConnector::from(Arc::new(rustls_config()?));
            let name = rustls::pki_types::ServerName::try_from(config.host.clone())
                .map_err(|_| Error::new("PostgreSQL TLS server name is invalid"))?;
            let tls = connector
                .connect(name, tcp)
                .await
                .map_err(|_| Error::new("PostgreSQL replication TLS handshake failed"))?;
            Conn::Tls(Box::new(tls))
        };
        let mut session = Self {
            conn,
            buf: BytesMut::new(),
        };
        session.startup(config).await?;
        session.authenticate(config).await?;
        session.ready().await?;
        Ok(session)
    }

    pub(super) async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<BTreeMap<String, Option<String>>>, Error> {
        self.send_query(sql).await?;
        let mut names = Vec::new();
        let mut rows = Vec::new();
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'T' => names = column_names(&payload)?,
                b'D' => rows.push(data_row(&payload, &names)?),
                b'C' | b'I' | b'N' | b'S' => {}
                b'Z' => return Ok(rows),
                b'E' => return Err(query_error(&payload)),
                _ => {
                    return Err(Error::new(
                        "PostgreSQL replication query returned an unexpected message",
                    ));
                }
            }
        }
    }

    pub(super) async fn start_copy(&mut self, sql: &str) -> Result<(), Error> {
        self.send_query(sql).await?;
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'W' => return Ok(()),
                b'T' | b'D' | b'C' | b'I' | b'N' | b'S' => {}
                b'E' => {
                    let text = field_m(&payload);
                    if text.contains("removed")
                        || text.contains("recycled")
                        || text.contains("no longer")
                    {
                        return Err(Error::new(
                            "PostgreSQL WAL past the checkpoint was recycled; an explicit new snapshot and consumer recovery are required",
                        ));
                    }
                    return Err(Error::new("PostgreSQL logical stream failed to start"));
                }
                _ => {
                    return Err(Error::new(
                        "PostgreSQL did not enter CopyBoth for START_REPLICATION",
                    ));
                }
            }
        }
    }

    pub(super) async fn next_copy(&mut self) -> Result<Option<Bytes>, Error> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'd' => return Ok(Some(payload)),
                b'c' | b'C' => return Ok(None),
                b'N' | b'S' => {}
                b'E' => {
                    return Err(Error::new(
                        "PostgreSQL replication stream failed; WAL may have been recycled",
                    ));
                }
                _ => {
                    return Err(Error::new(
                        "PostgreSQL replication stream returned an unexpected message",
                    ));
                }
            }
        }
    }

    pub(super) async fn send_copy(&mut self, payload: Bytes) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        frontend::CopyData::new(payload)
            .map_err(|_| Error::new("PostgreSQL standby status update failed"))?
            .write(&mut buf);
        self.write_all(&buf).await
    }

    async fn startup(&mut self, config: &Config) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        frontend::startup_message(
            [
                ("user", config.user.as_str()),
                ("database", config.database.as_str()),
                ("replication", "database"),
                ("client_encoding", "UTF8"),
                ("application_name", "hashi"),
            ],
            &mut buf,
        )
        .map_err(|_| Error::new("PostgreSQL replication startup failed"))?;
        self.write_all(&buf).await
    }

    async fn authenticate(&mut self, config: &Config) -> Result<(), Error> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'R' => match auth_kind(&payload)? {
                    0 => return Ok(()),
                    3 => {
                        self.send_password(config.password.as_bytes()).await?;
                    }
                    5 => {
                        if payload.len() < 8 {
                            return Err(Error::new("PostgreSQL MD5 authentication is truncated"));
                        }
                        let salt = [payload[4], payload[5], payload[6], payload[7]];
                        let hash = authentication::md5_hash(
                            config.user.as_bytes(),
                            config.password.as_bytes(),
                            salt,
                        );
                        self.send_password(hash.as_bytes()).await?;
                    }
                    10 => {
                        self.scram(config, &payload[4..]).await?;
                        return Ok(());
                    }
                    _ => {
                        return Err(Error::new(
                            "PostgreSQL replication authentication method is unsupported",
                        ));
                    }
                },
                b'N' | b'S' => {}
                b'E' => return Err(query_error(&payload)),
                _ => {
                    return Err(Error::new(
                        "PostgreSQL replication authentication returned an unexpected message",
                    ));
                }
            }
        }
    }

    async fn scram(&mut self, config: &Config, mechanisms: &[u8]) -> Result<(), Error> {
        let mut offered = Vec::new();
        let mut rest = mechanisms;
        while let Some(name) = take_cstr(&mut rest)? {
            if !name.is_empty() {
                offered.push(name.to_owned());
            }
        }
        if !offered.iter().any(|m| m == sasl::SCRAM_SHA_256) {
            return Err(Error::new(
                "PostgreSQL replication authentication method is unsupported",
            ));
        }
        let mut scram = sasl::ScramSha256::new(
            config.password.as_bytes(),
            sasl::ChannelBinding::unsupported(),
        );
        let mut buf = BytesMut::new();
        frontend::sasl_initial_response(sasl::SCRAM_SHA_256, scram.message(), &mut buf)
            .map_err(|_| Error::new("PostgreSQL SCRAM authentication failed"))?;
        self.write_all(&buf).await?;
        let continue_payload = self.expect_auth(11).await?;
        scram
            .update(&continue_payload)
            .map_err(|_| Error::new("PostgreSQL SCRAM authentication failed"))?;
        buf.clear();
        frontend::sasl_response(scram.message(), &mut buf)
            .map_err(|_| Error::new("PostgreSQL SCRAM authentication failed"))?;
        self.write_all(&buf).await?;
        let final_payload = self.expect_auth(12).await?;
        scram
            .finish(&final_payload)
            .map_err(|_| Error::new("PostgreSQL SCRAM authentication failed"))?;
        let (tag, payload) = self.read_message().await?;
        match tag {
            b'R' if auth_kind(&payload)? == 0 => Ok(()),
            b'E' => Err(query_error(&payload)),
            _ => Err(Error::new("PostgreSQL SCRAM authentication failed")),
        }
    }

    async fn expect_auth(&mut self, kind: i32) -> Result<Bytes, Error> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'R' if auth_kind(&payload)? == kind => return Ok(payload.slice(4..)),
                b'N' | b'S' => {}
                b'E' => return Err(query_error(&payload)),
                _ => return Err(Error::new("PostgreSQL SCRAM authentication failed")),
            }
        }
    }

    async fn ready(&mut self) -> Result<(), Error> {
        loop {
            let (tag, payload) = self.read_message().await?;
            match tag {
                b'Z' => return Ok(()),
                b'S' | b'K' | b'N' => {}
                b'E' => return Err(query_error(&payload)),
                _ => {
                    return Err(Error::new(
                        "PostgreSQL replication startup returned an unexpected message",
                    ));
                }
            }
        }
    }

    async fn send_password(&mut self, password: &[u8]) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        frontend::password_message(password, &mut buf)
            .map_err(|_| Error::new("PostgreSQL password authentication failed"))?;
        self.write_all(&buf).await
    }

    async fn send_query(&mut self, sql: &str) -> Result<(), Error> {
        let mut buf = BytesMut::new();
        frontend::query(sql, &mut buf)
            .map_err(|_| Error::new("PostgreSQL replication query failed"))?;
        self.write_all(&buf).await
    }

    async fn read_message(&mut self) -> Result<(u8, Bytes), Error> {
        self.fill(5).await?;
        let tag = self.buf[0];
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
        if len < 4 {
            return Err(Error::new("PostgreSQL replication message is invalid"));
        }
        self.fill(1 + len).await?;
        let _ = self.buf.split_to(5);
        Ok((tag, self.buf.split_to(len - 4).freeze()))
    }

    async fn fill(&mut self, n: usize) -> Result<(), Error> {
        while self.buf.len() < n {
            let mut tmp = [0u8; 8192];
            let read = self
                .conn
                .read(&mut tmp)
                .await
                .map_err(|_| Error::new("PostgreSQL replication connection closed"))?;
            if read == 0 {
                return Err(Error::new("PostgreSQL replication connection closed"));
            }
            self.buf.extend_from_slice(&tmp[..read]);
        }
        Ok(())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.conn
            .write_all(buf)
            .await
            .map_err(|_| Error::new("PostgreSQL replication write failed"))?;
        self.conn
            .flush()
            .await
            .map_err(|_| Error::new("PostgreSQL replication write failed"))
    }
}

impl Conn {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Conn::Plain(stream) => stream.read(buf).await,
            Conn::Tls(stream) => stream.read(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Conn::Plain(stream) => stream.write_all(buf).await,
            Conn::Tls(stream) => stream.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Conn::Plain(stream) => stream.flush().await,
            Conn::Tls(stream) => stream.flush().await,
        }
    }
}

fn rustls_config() -> Result<rustls::ClientConfig, Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn auth_kind(payload: &[u8]) -> Result<i32, Error> {
    if payload.len() < 4 {
        return Err(Error::new("PostgreSQL authentication message is truncated"));
    }
    Ok(i32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

fn query_error(_payload: &[u8]) -> Error {
    Error::new("PostgreSQL source operation failed")
}

fn field_m(payload: &[u8]) -> String {
    let mut rest = payload;
    while rest.len() >= 2 {
        let kind = rest[0];
        rest = &rest[1..];
        if kind == 0 {
            break;
        }
        let Some(end) = rest.iter().position(|b| *b == 0) else {
            break;
        };
        if kind == b'M' {
            return String::from_utf8_lossy(&rest[..end]).into_owned();
        }
        rest = &rest[end + 1..];
    }
    String::new()
}

fn column_names(payload: &[u8]) -> Result<Vec<String>, Error> {
    if payload.len() < 2 {
        return Err(Error::new("PostgreSQL row description is truncated"));
    }
    let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut rest = &payload[2..];
    let mut names = Vec::with_capacity(n);
    for _ in 0..n {
        let name = take_cstr(&mut rest)?.unwrap_or("").to_owned();
        if rest.len() < 18 {
            return Err(Error::new("PostgreSQL row description is truncated"));
        }
        rest = &rest[18..];
        names.push(name);
    }
    Ok(names)
}

fn data_row(payload: &[u8], names: &[String]) -> Result<BTreeMap<String, Option<String>>, Error> {
    if payload.len() < 2 {
        return Err(Error::new("PostgreSQL data row is truncated"));
    }
    let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if n != names.len() {
        return Err(Error::new("PostgreSQL data row does not match description"));
    }
    let mut rest = &payload[2..];
    let mut row = BTreeMap::new();
    for name in names {
        if rest.len() < 4 {
            return Err(Error::new("PostgreSQL data row is truncated"));
        }
        let len = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        rest = &rest[4..];
        if len < 0 {
            row.insert(name.clone(), None);
            continue;
        }
        let len = len as usize;
        if rest.len() < len {
            return Err(Error::new("PostgreSQL data row is truncated"));
        }
        let text = std::str::from_utf8(&rest[..len])
            .map_err(|_| Error::new("PostgreSQL data row is not UTF-8"))?
            .to_owned();
        rest = &rest[len..];
        row.insert(name.clone(), Some(text));
    }
    Ok(row)
}

fn take_cstr<'a>(buf: &mut &'a [u8]) -> Result<Option<&'a str>, Error> {
    if buf.is_empty() {
        return Ok(None);
    }
    let end = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| Error::new("PostgreSQL string is truncated"))?;
    let text = std::str::from_utf8(&buf[..end])
        .map_err(|_| Error::new("PostgreSQL identifier is not UTF-8"))?;
    *buf = &buf[end + 1..];
    Ok(Some(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_m_reads_message() {
        let mut payload = Vec::new();
        payload.push(b'C');
        payload.extend(b"XX000\0");
        payload.push(b'M');
        payload.extend(b"requested WAL segment has already been recycled\0");
        payload.push(0);
        assert!(field_m(&payload).contains("recycled"));
    }
}
