use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::convert::TryInto;
use std::{thread, time};
use tokio::io::AsyncRead;
use tokio::sync::SemaphorePermit;
mod null_client;

#[derive(Parser)]
#[clap(name = "nulldb")]
#[clap(about = "A fictional versioning CLI", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Put {
        key: String,
        value: String,
        #[clap(long, default_value = "localhost")]
        host: String,
        #[clap(long, default_value = "8080")]
        port: String,
    },

    Get {
        key: String,
        #[clap(long, default_value = "localhost")]
        host: String,
        #[clap(long, default_value = "8080")]
        port: String,
    },

    Test {
        #[clap(long, default_value = "localhost")]
        host: String,
        #[clap(long, default_value = "8080")]
        port: String,
    },

    Delete {
        key: String,
        #[clap(long, default_value = "localhost")]
        host: String,
    },

    Bench {
        #[clap(long, default_value_t = 100)]
        records: i32,
        #[clap(long, default_value_t = 10)]
        duration: i32,
        #[clap(long, default_value = "localhost")]
        host: String,
    },

    BenchDisk {
        #[clap(long, default_value = "localhost")]
        host: String,
        #[clap(long, default_value = "8001")]
        port: String,
    },

    Compact {
        #[clap(long, default_value = "localhost")]
        host: String,
        #[clap(long, default_value = "8080")]
        port: String,
    },
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "localhost")]
    host: String,
    #[clap(short, long, default_value = "data")]
    data: String,
    #[clap(short, long, default_value = "key")]
    key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    match &args.command {
        Commands::Put {
            key,
            value,
            host,
            port,
        } => {
            println!("putting data {}", value);
            println!(
                "connecting at: {}",
                format!("http://{}:{}/{}/{}", host, port, "v1/data", key)
            );
            let client = reqwest::Client::new();
            let data = value.clone();
            let resp = client
                .post(format!("http://{}:{}/{}/{}", host, port, "v1/data", key))
                .body(data)
                .send()
                .await?
                .text()
                .await?;

            println!("{}", resp)
        }

        Commands::Test { host, port } => {
            println!("testing data");
            let client = reqwest::Client::new();
            let resp = client
                .get(format!("http://{}:{}/", host, port))
                .send()
                .await?
                .text()
                .await?;

            println!("{}", resp)
        }

        Commands::Get { key, host, port } => {
            let then = time::Instant::now();
            println!("getting data for key {}", key);
            let resp = reqwest::get(format!("http://{}:{}/{}/{}\n", host, port, "v1/data", key))
                .await?
                .text()
                .await?;
            println!("key {}:{}", key, resp);
            let dur: u64 = ((time::Instant::now() - then).as_millis())
                .try_into()
                .unwrap();
            println!("duration: {}", dur);
        }

        Commands::Delete { key, host } => {
            println!("deleting data for key {}", key);
            let client = reqwest::Client::new();
            let _resp = client
                .delete(format!("http://{}:8080/{}/{}\n", host, "v1/data", key))
                .send()
                .await?
                .text()
                .await?;
        }

        Commands::Bench {
            records,
            duration,
            host,
        } => {
            println!("benchmarking database");
            benchmark(*records, *duration, host.to_string()).await;
        }

        Commands::BenchDisk { host, port } => {
            println!("benchmarking disk");
            load_disks(1_000, 32, host.to_string(), port.to_string()).await;
        }

        Commands::Compact { host, port } => {
            println!("Making a compation request!");
            let _resp = reqwest::get(format!(
                "http://{}:{}/{}\n",
                host, port, "v1/management/compact"
            ))
            .await?
            .text()
            .await?;
        }
    }

    Ok(())
}

async fn benchmark(records: i32, duration: i32, host: String) -> Option<()> {
    let start = time::Instant::now();
    let client = null_client::NullClient::new(format!("http://{}:8080/v1/data", host).to_string());

    println!("Countdown");
    let mut rng = thread_rng();
    for _i in 0..duration {
        let then = time::Instant::now();

        // This loop will run once every second
        for _ in 0..records {
            let ret = client
                .post(
                    get_random_string(rng.gen_range(1..10)),
                    get_random_string(rng.gen_range(50..500)),
                )
                .await;
            if ret.is_err() {
                println!("failed: {:?}", ret)
            }
        }

        let dur: u64 = ((time::Instant::now() - then).as_millis())
            .try_into()
            .unwrap();
        let sleep_time: u64 = 1000 - dur;

        if sleep_time > 0 {
            let sleep_dura = time::Duration::from_nanos(sleep_time);
            thread::sleep(sleep_dura);
        }
    }

    let end: u64 = ((time::Instant::now() - start).as_millis())
        .try_into()
        .unwrap();
    println!(
        "Benchmarking took:{} \nTotal Records Saved: {} \nRecords per MS:{}",
        end,
        duration * records,
        (duration * records) as f64 / end as f64
    );
    return Some(());
}

async fn load_disks(records: u32, threads: u32, host: String, port: String) {
    let client = null_client::NullClient::new(format!("http://{host}:{port}/v1/data").to_string());

    let _ = futures::stream::iter(0..records)
        .map(|_| {
            let c = client.clone();
            tokio::spawn(async move {
                let _ = c.post(
                "testdata".to_string(),
                "This is the value of our record, it's awesome, not super short but kind of short."
                    .to_string()).await;
            })
        })
        .buffered(threads as usize)
        .count()
        .await;
}
fn get_random_string(length: usize) -> String {
    let chars: Vec<u8> = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .collect();
    let s = std::str::from_utf8(&chars).unwrap().to_string();
    return s;
}
