//! Example usage:
//!
//! ```bash
//! wget -qO- "https://data.binance.vision/data/futures/um/daily/aggTrades/1000BTTCUSDT/1000BTTCUSDT-aggTrades-2022-04-08.zip"
//! | zcat
//! | csv_parser -c trade_id:i64 \
//!     -c price:f32 \
//!     -c qty:f32 \
//!     -c first_id:i64 \
//!     -c last_id:i64 \
//!     -c timestamp:i64 \
//!     -c is_bid:bool \
//!     -s timestamp \
//!     -s price \
//!     -s qty \
//!     -s is_bid \
//!     --sort timestamp \
//!     /tmp/test.pq
//! ```

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use clap::Parser;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;

pub struct StdinReader {
    stdin: io::Stdin,
}

impl Read for StdinReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stdin.read(buf)
    }
}

impl Seek for StdinReader {
    fn seek(&mut self, _pos: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(io::ErrorKind::Other, "Seek not supported"))
    }
}

unsafe impl Send for StdinReader {}
unsafe impl Sync for StdinReader {}
impl MmapBytesReader for StdinReader {}

#[derive(clap::Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[clap(short, long)]
    columns: Vec<String>,
    #[clap(long)]
    skip_rows: usize,
    #[clap(short, long)]
    select: Vec<String>,
    #[clap(long)]
    sort: Vec<String>,
    output: String,
}

fn parse_column(col: &str) -> (String, DataType) {
    let mut split = col.split(':');
    let name = split.next().unwrap();
    let dtype = split.next().unwrap();
    let dtype = match dtype {
        "i64" => DataType::Int64,
        "i32" => DataType::Int32,
        "f32" => DataType::Float32,
        "f64" => DataType::Float64,
        "bool" => DataType::Boolean,
        // "timestamp[ms]" => DataType::Datetime(TimeUnit::Millisecond, None),
        _ => panic!("Unsupported data type: {}", dtype),
    };
    (name.to_string(), dtype)
}

fn main() {
    let cli = Cli::parse();
    let mut reader = StdinReader { stdin: io::stdin() };

    let columns = cli
        .columns
        .iter()
        .map(|c| parse_column(c))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::from_iter(
        columns
            .iter()
            .map(|(name, dtype)| Field::new(name.into(), dtype.clone())),
    ));
    let csv_reader = CsvReadOptions::default()
        .with_has_header(true)
        .with_skip_rows(cli.skip_rows)
        .with_schema(Some(schema))
        .with_columns(Some(columns.iter().map(|(name, _)| name.into()).collect()))
        .into_reader_with_file_handle(&mut reader);

    let df = csv_reader.finish().unwrap();

    let mut df = df.select(&cli.select).unwrap();
    let mut df = df
        .sort_in_place(
            &cli.sort,
            SortMultipleOptions::new().with_maintain_order(true),
        )
        .unwrap();

    let f = File::create(&cli.output).expect("create failed");

    ParquetWriter::new(f)
        .set_parallel(false)
        .finish(&mut df)
        .expect("write failed");
}
