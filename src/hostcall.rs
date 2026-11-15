//! The host call: a harness's line to the program that runs this `ree` as its engine (`Context.HostCall`).
//!
//! A program that drives a run from outside (rodeo's `rodeo run --ree`, which answers a script's `@rodeo/*`
//! calls: files, streams, processes) listens on a loopback port and starts `ree` with [`ADDRESS`] naming it
//! (`127.0.0.1:<port>`) and [`TOKEN`] holding the word it expects first. The harness then has
//! `Context.HostCall(Request: string): string`: one frame goes out, the engine waits, and the reply's bytes come back.
//!
//! The engine does nothing else while it waits: no thread resumes, no frame steps and the virtual clock stands, so what a
//! run does is the same however long its host takes (a file read, a person typing on stdin). The wait is not script work
//! either: the script timeout's host-time backstop does not count it ([`crate::watchdog::forgive`]).
//!
//! Without [`ADDRESS`] in the environment there is no `HostCall` and nothing here opens a socket, as before; with it,
//! only a loopback address is dialed. Scripts get no socket of their own either way: the harness decides what its
//! `HostCall` carries.
//!
//! Frames, both ways: a little-endian u32 length, then that many bytes.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Instant,
};

/// The environment variable naming the host's loopback address (`127.0.0.1:<port>`)
pub const ADDRESS: &str = "REE_HOST_CALL";

/// The environment variable holding the token sent as the first frame, so the host knows the process it started
pub const TOKEN: &str = "REE_HOST_TOKEN";

/// The largest frame either side sends (a length past it is a broken stream, not a message)
pub const MAX_FRAME: usize = 1 << 30;

/// The address the environment names, when it names one.
pub fn configured() -> Option<String> {
    std::env::var(ADDRESS).ok().filter(|address| !address.trim().is_empty())
}

/// The connection to the host.
pub struct Host {
    stream: TcpStream,
}

impl Host {
    /// Dials the host [`ADDRESS`] names and sends [`TOKEN`]'s word (`None` without an address).
    pub fn from_env() -> Result<Option<Host>, String> {
        let Some(address) = configured() else { return Ok(None) };
        Host::connect(address.trim(), &std::env::var(TOKEN).unwrap_or_default()).map(Some)
    }

    /// Dials `address`, a loopback `ip:port`, and sends `token` as the first frame.
    pub fn connect(address: &str, token: &str) -> Result<Host, String> {
        let parsed: SocketAddr = address.parse().map_err(|error| format!("{ADDRESS}: {address:?} is not an ip:port address ({error})"))?;
        if !parsed.ip().is_loopback() {
            return Err(format!("{ADDRESS}: {address} is not a loopback address: the host is the program that started this run"));
        }
        let stream = TcpStream::connect(parsed).map_err(|error| format!("{ADDRESS}: cannot reach the host at {address}: {error}"))?;
        // (a call is a small frame each way: without this the reply waits on the peer's delayed acknowledgement)
        let _ = stream.set_nodelay(true);
        let mut host = Host { stream };
        host.send(token.as_bytes())?;
        Ok(host)
    }

    /// One call: `request` goes out as a frame and the reply's bytes come back. The host time it takes is forgiven to the
    /// script timeout.
    pub fn call(&mut self, request: &[u8]) -> Result<Vec<u8>, String> {
        let started = Instant::now();
        let reply = self.send(request).and_then(|()| self.receive());
        crate::watchdog::forgive(started.elapsed());
        reply
    }

    fn send(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > MAX_FRAME {
            return Err(format!("a host call of {} bytes is past the frame limit ({MAX_FRAME})", bytes.len()));
        }
        let gone = |error: std::io::Error| format!("the host is gone ({error})");
        self.stream.write_all(&(bytes.len() as u32).to_le_bytes()).map_err(gone)?;
        self.stream.write_all(bytes).map_err(gone)?;
        self.stream.flush().map_err(gone)
    }

    fn receive(&mut self) -> Result<Vec<u8>, String> {
        let gone = |error: std::io::Error| format!("the host is gone ({error})");
        let mut length = [0u8; 4];
        self.stream.read_exact(&mut length).map_err(gone)?;
        let length = u32::from_le_bytes(length) as usize;
        if length > MAX_FRAME {
            return Err(format!("the host's reply says {length} bytes, past the frame limit ({MAX_FRAME})"));
        }
        let mut bytes = vec![0u8; length];
        self.stream.read_exact(&mut bytes).map_err(gone)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn read(stream: &mut TcpStream) -> Vec<u8> {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).unwrap();
        let mut bytes = vec![0u8; u32::from_le_bytes(length) as usize];
        stream.read_exact(&mut bytes).unwrap();
        bytes
    }

    fn write(stream: &mut TcpStream, bytes: &[u8]) {
        stream.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
        stream.write_all(bytes).unwrap();
    }

    #[test]
    fn a_call_sends_the_token_first_and_returns_the_reply_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let host = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let token = read(&mut stream);
            let first = read(&mut stream);
            write(&mut stream, &[b"reply to ", &first[..]].concat());
            // an empty frame and a large one both arrive whole
            let second = read(&mut stream);
            write(&mut stream, &[]);
            let third = read(&mut stream);
            write(&mut stream, &third);
            (token, second.len(), third.len())
        });
        let mut client = Host::connect(&address, "the-token").unwrap();
        assert_eq!(client.call(b"ping").unwrap(), b"reply to ping");
        assert_eq!(client.call(b"").unwrap(), b"");
        let large = vec![7u8; 3 * 1024 * 1024];
        assert_eq!(client.call(&large).unwrap(), large);
        assert_eq!(host.join().unwrap(), (b"the-token".to_vec(), 0, large.len()));
    }

    #[test]
    fn only_a_loopback_address_is_dialed() {
        assert!(Host::connect("192.0.2.1:9", "").err().unwrap().contains("not a loopback address"));
        assert!(Host::connect("example.com:80", "").err().unwrap().contains("not an ip:port address"));
    }

    #[test]
    fn a_host_that_hangs_up_is_an_error_not_a_wait() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let host = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _token = read(&mut stream);
            let _request = read(&mut stream);
            // (dropped without a reply)
        });
        let mut client = Host::connect(&address, "").unwrap();
        assert!(client.call(b"ping").err().unwrap().contains("the host is gone"));
        host.join().unwrap();
    }
}
