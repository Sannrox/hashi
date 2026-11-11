//! `hashi` prints its version; `hashi run --config <file> [--once]
//! [--interval-ms <n>] [--listen <addr>]` runs the configured instances, once
//! or until stopped, optionally serving `/healthz` and `/metrics` on `<addr>`;
//! `hashi diagnostics --config <file>` prints a support bundle as JSON;
//! `hashi discover --config <file> [--instance <name>]` prints discovered
//! kinds; `hashi trust list|approve|revoke` reviews OPC UA server trust;
//! `hashi outbox` runs MySQL change capture once for a sandboxed host, from
//! the files its `CONNECTOR_*` variables name (see `hashi::outbox`).

use std::net::TcpListener;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hashi::observe::{Metrics, serve};
use hashi::{Config, DiscoverOutcome, Service};

const USAGE: &str = "usage: hashi run --config <file> [--once] [--interval-ms <n>] [--listen <addr>]\n       hashi diagnostics --config <file>\n       hashi discover --config <file> [--instance <name>]\n       hashi trust list --config <file> [--instance <name>]\n       hashi trust approve --config <file> [--instance <name>] <sha256>\n       hashi trust revoke --config <file> [--instance <name>] <sha256>\n       hashi outbox";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        println!(
            "hashi {} contract/{}",
            env!("CARGO_PKG_VERSION"),
            hashi::CONTRACT_VERSION
        );
        return ExitCode::SUCCESS;
    }
    let outcome = match args[0].as_str() {
        "run" => run(&args[1..]),
        "diagnostics" => diagnostics(&args[1..]),
        "discover" => discover(&args[1..]),
        "trust" => trust(&args[1..]),
        "outbox" => outbox(&args[1..]),
        _ => Err(USAGE.into()),
    };
    match outcome {
        Ok(code) => code,
        Err(message) => {
            eprintln!("hashi: {message}");
            ExitCode::from(2)
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// One run into the host's outbox; the pages written go to stderr.
fn outbox(args: &[String]) -> Result<ExitCode, String> {
    if !args.is_empty() {
        return Err(USAGE.into());
    }
    #[cfg(feature = "mysql")]
    {
        let files = hashi::outbox::HostFiles::from_env().map_err(|error| error.message)?;
        match hashi::outbox::run_mysql_once(&files) {
            Ok(pages) => {
                eprintln!("hashi: wrote {pages} pages");
                Ok(ExitCode::SUCCESS)
            }
            Err(error) => {
                eprintln!("hashi: {}", error.message);
                Ok(ExitCode::FAILURE)
            }
        }
    }
    #[cfg(not(feature = "mysql"))]
    Err("hashi outbox needs the mysql feature".into())
}

fn diagnostics(args: &[String]) -> Result<ExitCode, String> {
    let [flag, path] = args else {
        return Err(USAGE.into());
    };
    if flag != "--config" {
        return Err(USAGE.into());
    }
    let config = Config::load(path.as_ref()).map_err(|error| error.message)?;
    let bundle = serde_json::to_string_pretty(&hashi::diagnostics::collect(&config))
        .map_err(|error| error.to_string())?;
    println!("{bundle}");
    Ok(ExitCode::SUCCESS)
}

fn operator_args(args: &[String]) -> Result<(String, Option<String>, Option<String>), String> {
    let mut config_path = None;
    let mut instance = None;
    let mut rest = Vec::new();
    let mut values = args.iter();
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--config" => config_path = Some(values.next().ok_or(USAGE)?.clone()),
            "--instance" => instance = Some(values.next().ok_or(USAGE)?.clone()),
            value if !value.starts_with('-') => rest.push(value.to_string()),
            _ => return Err(USAGE.into()),
        }
    }
    let config_path = config_path.ok_or(USAGE)?;
    if rest.len() > 1 {
        return Err(USAGE.into());
    }
    Ok((config_path, instance, rest.pop()))
}

fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn discover(args: &[String]) -> Result<ExitCode, String> {
    let (path, instance, extra) = operator_args(args)?;
    if extra.is_some() {
        return Err(USAGE.into());
    }
    let config = Config::load(path.as_ref()).map_err(|error| error.message)?;
    match hashi::discover(&config, instance.as_deref()).map_err(|error| error.message)? {
        DiscoverOutcome::Ok(document) => {
            print_json(&document)?;
            Ok(ExitCode::SUCCESS)
        }
        DiscoverOutcome::Untrusted(document) => {
            print_json(&document)?;
            Ok(ExitCode::FAILURE)
        }
    }
}

fn trust(args: &[String]) -> Result<ExitCode, String> {
    let Some((action, rest)) = args.split_first() else {
        return Err(USAGE.into());
    };
    let (path, instance, fingerprint) = operator_args(rest)?;
    let config = Config::load(path.as_ref()).map_err(|error| error.message)?;
    let document = match action.as_str() {
        "list" if fingerprint.is_none() => {
            hashi::trust_list(&config, instance.as_deref()).map_err(|error| error.message)?
        }
        "approve" => {
            let sha256 = fingerprint.ok_or(USAGE)?;
            hashi::trust_approve(&config, instance.as_deref(), &sha256)
                .map_err(|error| error.message)?
        }
        "revoke" => {
            let sha256 = fingerprint.ok_or(USAGE)?;
            hashi::trust_revoke(&config, instance.as_deref(), &sha256)
                .map_err(|error| error.message)?
        }
        _ => return Err(USAGE.into()),
    };
    print_json(&document)?;
    Ok(ExitCode::SUCCESS)
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let mut config_path = None;
    let mut once = false;
    let mut interval = Duration::from_secs(1);
    let mut listen = None;
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--config" => config_path = rest.next().cloned(),
            "--once" => once = true,
            "--interval-ms" => {
                let value = rest.next().ok_or(USAGE)?;
                interval = Duration::from_millis(value.parse().map_err(|_| USAGE)?);
            }
            "--listen" => listen = Some(rest.next().ok_or(USAGE)?.clone()),
            _ => return Err(USAGE.into()),
        }
    }
    let config_path = config_path.ok_or(USAGE)?;
    let config = Config::load(config_path.as_ref()).map_err(|error| error.message)?;
    let mut service = Service::from_config(&config).map_err(|error| error.message)?;
    let metrics = Arc::new(Mutex::new(Metrics::default()));
    if let Some(addr) = listen {
        let listener =
            TcpListener::bind(&addr).map_err(|error| format!("--listen {addr}: {error}"))?;
        let bound = listener.local_addr().map_err(|error| error.to_string())?;
        eprintln!("hashi: listening on {bound}");
        serve(listener, metrics.clone(), now_ms);
    }
    loop {
        let mut failed = false;
        let passes = service.pass_with_progress();
        let mut metrics = metrics.lock().unwrap_or_else(|poison| poison.into_inner());
        for pass in &passes {
            let name = &pass.instance;
            match &pass.outcome {
                Ok(report) => eprintln!(
                    "{name}: delivered {} quarantined {} checkpoint {}",
                    report.delivered, report.quarantined, report.checkpoint
                ),
                Err(_error) if pass.role == 0 => {
                    if once {
                        failed = true;
                    }
                    eprintln!("{name}: standby, waiting for lease");
                }
                Err(error) => {
                    failed = true;
                    eprintln!("{name}: {}", error.message);
                }
            }
            let pass_failed = pass.outcome.is_err() && pass.role != 0;
            metrics.record(name, &pass.progress, pass_failed, now_ms());
            metrics.record_lease(
                name,
                pass.role,
                pass.fencing_token,
                pass.takeovers,
                pass.lease_acquired_ms,
            );
            metrics.record_buffer(name, pass.buffer);
        }
        drop(metrics);
        if once {
            return Ok(if failed {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            });
        }
        std::thread::sleep(interval);
    }
}
