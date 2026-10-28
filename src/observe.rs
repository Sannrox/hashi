//! Health and Prometheus metrics for `hashi run --listen <addr>`.
//!
//! The run loop records every pass into [`Metrics`]; [`serve`] answers
//! `GET /healthz` and `GET /metrics` from it on a plain HTTP listener. Nothing
//! here opens an outbound connection, so an air-gapped site scrapes it locally.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::RunReport;

#[derive(Debug, Default)]
pub struct Metrics {
    instances: BTreeMap<String, InstanceMetrics>,
}

#[derive(Debug, Default)]
struct InstanceMetrics {
    delivered_total: u64,
    quarantined_total: u64,
    errors_total: u64,
    records_per_second: f64,
    checkpoint: u64,
    checkpoint_advanced_ms: Option<i64>,
    last_source_ts_ms: Option<i64>,
    healthy: bool,
    recorded_ms: Option<i64>,
    buffer: Option<crate::buffer::BufferStats>,
}

impl Metrics {
    /// Record one instance's pass, finished at wall time `now_ms`. `progress`
    /// is what the pass committed, also when it then failed.
    pub fn record(&mut self, instance: &str, progress: &RunReport, failed: bool, now_ms: i64) {
        let entry = self.instances.entry(instance.to_string()).or_default();
        let since_ms = entry.recorded_ms.replace(now_ms);
        let records = progress.delivered + progress.quarantined;
        entry.delivered_total += progress.delivered;
        entry.quarantined_total += progress.quarantined;
        entry.records_per_second = match since_ms.map(|since| now_ms - since) {
            Some(ms) if ms > 0 => records as f64 * 1000.0 / ms as f64,
            _ => 0.0,
        };
        // A pass that failed before loading state reports position 0; keep the last known one.
        if records > 0 || !failed {
            entry.checkpoint = progress.checkpoint;
        }
        if records > 0 {
            entry.checkpoint_advanced_ms = Some(now_ms);
        }
        entry.last_source_ts_ms = progress.last_source_ts_ms.or(entry.last_source_ts_ms);
        entry.errors_total += u64::from(failed);
        entry.healthy = !failed;
    }

    /// Record an instance's buffer after a pass; `None` when it has no
    /// buffer or the buffer could not be read.
    pub fn record_buffer(&mut self, instance: &str, buffer: Option<crate::buffer::BufferStats>) {
        self.instances
            .entry(instance.to_string())
            .or_default()
            .buffer = buffer;
    }

    /// Healthy once every instance has passed and none failed its last pass.
    pub fn healthy(&self) -> bool {
        !self.instances.is_empty() && self.instances.values().all(|i| i.healthy)
    }

    /// Prometheus text exposition, one series per instance.
    pub fn render(&self, now_ms: i64) -> String {
        let mut out = String::new();
        let mut family =
            |name: &str,
             kind: &str,
             help: &str,
             value: &dyn Fn(&InstanceMetrics) -> Option<f64>| {
                let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
                for (instance, metrics) in &self.instances {
                    if let Some(v) = value(metrics) {
                        let _ = writeln!(out, "{name}{{instance=\"{}\"}} {v}", escape(instance));
                    }
                }
            };
        let age = |at: Option<i64>| at.map(|ms| (now_ms - ms).max(0) as f64 / 1000.0);
        family(
            "hashi_records_delivered_total",
            "counter",
            "Records acknowledged by the sink.",
            &|m| Some(m.delivered_total as f64),
        );
        family(
            "hashi_records_quarantined_total",
            "counter",
            "Records moved to quarantine.",
            &|m| Some(m.quarantined_total as f64),
        );
        family(
            "hashi_pass_errors_total",
            "counter",
            "Passes that ended in an error.",
            &|m| Some(m.errors_total as f64),
        );
        family(
            "hashi_records_per_second",
            "gauge",
            "Records committed per second since the previous pass.",
            &|m| Some(m.records_per_second),
        );
        family(
            "hashi_checkpoint_position",
            "gauge",
            "Last committed position.",
            &|m| Some(m.checkpoint as f64),
        );
        family(
            "hashi_checkpoint_age_seconds",
            "gauge",
            "Seconds since this process last advanced the checkpoint.",
            &|m| age(m.checkpoint_advanced_ms),
        );
        family(
            "hashi_source_lag_seconds",
            "gauge",
            "Age of the source timestamp of the newest committed record.",
            &|m| age(m.last_source_ts_ms),
        );
        family(
            "hashi_buffer_bytes",
            "gauge",
            "Bytes waiting in the store-and-forward buffer.",
            &|m| m.buffer.map(|b| b.bytes as f64),
        );
        family(
            "hashi_buffer_records",
            "gauge",
            "Records waiting in the store-and-forward buffer.",
            &|m| m.buffer.map(|b| b.records as f64),
        );
        family(
            "hashi_buffer_oldest_age_seconds",
            "gauge",
            "Age of the oldest buffered record's ingest time; absent when the buffer is empty.",
            &|m| age(m.buffer.and_then(|b| b.oldest_ingest_ts_ms)),
        );
        family(
            "hashi_buffer_drained_total",
            "counter",
            "Records this process delivered out of the buffer.",
            &|m| m.buffer.map(|b| b.drained as f64),
        );
        family(
            "hashi_buffer_dropped_total",
            "counter",
            "Records a full drop_oldest buffer dropped and logged as a gap; kept across restarts.",
            &|m| m.buffer.map(|b| b.dropped as f64),
        );
        family(
            "hashi_up",
            "gauge",
            "1 when the last pass succeeded.",
            &|m| Some(if m.healthy { 1.0 } else { 0.0 }),
        );
        out
    }
}

fn escape(label: &str) -> String {
    label
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Serve `/healthz` and `/metrics` on a background thread until the process exits.
/// Requests are answered one at a time, each bounded in size and time.
pub fn serve(
    listener: TcpListener,
    metrics: Arc<Mutex<Metrics>>,
    now_ms: fn() -> i64,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _ = answer(stream, &metrics, now_ms());
        }
    })
}

fn answer(mut stream: TcpStream, metrics: &Mutex<Metrics>, now_ms: i64) -> std::io::Result<()> {
    let path = request_path(&mut stream)?;
    let (status, body) = {
        let metrics = metrics.lock().unwrap_or_else(|poison| poison.into_inner());
        match path.as_str() {
            "/metrics" => ("200 OK", metrics.render(now_ms)),
            "/healthz" if metrics.healthy() => ("200 OK", "ok\n".into()),
            "/healthz" => ("503 Service Unavailable", "unhealthy\n".into()),
            _ => ("404 Not Found", "not found\n".into()),
        }
    };
    stream.set_write_timeout(Some(REQUEST_DEADLINE))?;
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

const REQUEST_DEADLINE: Duration = Duration::from_secs(2);
const MAX_REQUEST_LINE: usize = 8 * 1024;

/// The request target of the first line. Requests are served one at a time,
/// so a client gets at most `MAX_REQUEST_LINE` bytes and `REQUEST_DEADLINE` in total.
fn request_path(stream: &mut TcpStream) -> std::io::Result<String> {
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let mut line = Vec::new();
    let mut chunk = [0u8; 512];
    while !line.contains(&b'\n') && line.len() < MAX_REQUEST_LINE {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(&mut chunk)? {
            0 => break,
            n => line.extend_from_slice(&chunk[..n]),
        }
    }
    let line = String::from_utf8_lossy(&line);
    Ok(line.split_whitespace().nth(1).unwrap_or("").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(delivered: u64, checkpoint: u64) -> RunReport {
        RunReport {
            discovered: 1,
            delivered,
            quarantined: 0,
            checkpoint,
            last_source_ts_ms: Some(1_000),
        }
    }

    /// Buffer series appear only for an instance with a buffer; the oldest
    /// age only while something is buffered.
    #[test]
    fn buffer_metrics_report_fill_age_and_counters() {
        let mut metrics = Metrics::default();
        metrics.record("direct", &report(1, 1), false, 1_000);
        metrics.record("line-1", &report(1, 1), false, 1_000);
        metrics.record_buffer(
            "line-1",
            Some(crate::buffer::BufferStats {
                bytes: 300,
                records: 3,
                oldest_ingest_ts_ms: Some(4_000),
                drained: 5,
                dropped: 2,
            }),
        );
        let text = metrics.render(10_000);
        for series in [
            "hashi_buffer_bytes{instance=\"line-1\"} 300\n",
            "hashi_buffer_records{instance=\"line-1\"} 3\n",
            "hashi_buffer_oldest_age_seconds{instance=\"line-1\"} 6\n",
            "hashi_buffer_drained_total{instance=\"line-1\"} 5\n",
            "hashi_buffer_dropped_total{instance=\"line-1\"} 2\n",
        ] {
            assert!(text.contains(series), "{series} missing from {text}");
        }
        assert!(!text.contains("hashi_buffer_bytes{instance=\"direct\"}"));
    }

    #[test]
    fn a_failed_pass_turns_health_off_and_counts_an_error() {
        let mut metrics = Metrics::default();
        assert!(!metrics.healthy());
        metrics.record("line-1", &report(0, 0), false, 3_000);
        metrics.record("line-1", &report(4, 4), false, 5_000);
        assert!(metrics.healthy());
        let text = metrics.render(7_000);
        assert!(text.contains("hashi_records_delivered_total{instance=\"line-1\"} 4\n"));
        assert!(text.contains("hashi_records_per_second{instance=\"line-1\"} 2\n"));
        assert!(text.contains("hashi_checkpoint_age_seconds{instance=\"line-1\"} 2\n"));
        assert!(text.contains("hashi_source_lag_seconds{instance=\"line-1\"} 6\n"));
        // A pass that commits two more records and then fails still counts them.
        metrics.record("line-1", &report(2, 6), true, 8_000);
        assert!(!metrics.healthy());
        let text = metrics.render(8_000);
        assert!(text.contains("hashi_pass_errors_total{instance=\"line-1\"} 1\n"));
        assert!(text.contains("hashi_up{instance=\"line-1\"} 0\n"));
        assert!(text.contains("hashi_records_delivered_total{instance=\"line-1\"} 6\n"));
        assert!(text.contains("hashi_checkpoint_position{instance=\"line-1\"} 6\n"));
        // One that fails before reading state keeps the last known position.
        metrics.record("line-1", &RunReport::default(), true, 9_000);
        assert!(
            metrics
                .render(9_000)
                .contains("hashi_checkpoint_position{instance=\"line-1\"} 6\n")
        );
    }

    #[test]
    fn a_silent_or_oversized_client_cannot_block_the_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        serve(listener, Arc::new(Mutex::new(Metrics::default())), || 0);
        let _silent = TcpStream::connect(addr).unwrap();
        let mut flood = TcpStream::connect(addr).unwrap();
        let _ = flood.write_all(&[b'a'; 64 * 1024]);
        let started = Instant::now();
        let mut probe = TcpStream::connect(addr).unwrap();
        probe.write_all(b"GET /healthz HTTP/1.1\r\n\r\n").unwrap();
        let mut response = String::new();
        probe.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 503"), "{response}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
