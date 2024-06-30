use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Result, Seek, SeekFrom, Take},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

type StationsMap = BTreeMap<String, StationMetrics>;
type ChunkReader = Take<BufReader<File>>;

fn main() {
    /*
    The release build is executed in around 17.96 seconds on SER5 PRO MAX:
       - CPU: AMD Ryzen 7 5800H with Radeon Graphics (16) @ 3.200GHz
       - GPU: AMD ATI Radeon Vega Series / Radeon Vega Mobile Series
       - Memory: 28993MiB
    */
    let start = Instant::now();

    let n_threads: usize = std::thread::available_parallelism().unwrap().into();

    let file_path = Path::new("./data/weather_stations.csv");
    let file_size = fs::metadata(&file_path).unwrap().size();
    let mut reader = get_file_reader(file_path).unwrap();

    let intervals = get_file_intervals_for_cpus(n_threads, file_size, &mut reader);
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for interval in intervals {
        let results = Arc::clone(&results);
        let handle = thread::spawn(move || {
            let station_to_metrics = process_chunk(file_path, interval);
            results.lock().unwrap().push(station_to_metrics);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let result = results
        .lock()
        .unwrap()
        .iter()
        .fold(StationsMap::default(), |a, b| merge_maps(a, &b));

    print_metrics(&result);
    println!("\n Execution time: {:?}", start.elapsed());
}

fn get_file_reader(path: &Path) -> Result<BufReader<File>> {
    let file: File = File::open(path)?;
    Ok(BufReader::new(file))
}

/// Splits the file into intervals based on the number of CPUs.
/// Each interval is determined by dividing the file size by the number of CPUs
/// and adjusting the intervals to ensure lines are not split between chunks.
///
/// Example:
///
/// Suppose the file size is 1000 bytes and `cpus` is 4.
/// The file will be divided into 4 chunks, and the intervals might be as follows:
///
/// Interval { start: 0, end: 249 }
/// Interval { start: 250, end: 499 }
/// Interval { start: 500, end: 749 }
/// Interval { start: 750, end: 999 }
/// ```
fn get_file_intervals_for_cpus(
    cpus: usize,
    file_size: u64,
    reader: &mut BufReader<File>,
) -> Vec<Interval> {
    let chunk_size = file_size / (cpus as u64);
    let mut intervals = Vec::new();
    let mut start = 0;
    let mut buf = String::new();

    for _ in 0..cpus {
        let mut end: u64 = (start + chunk_size).min(file_size);
        _ = reader.seek(SeekFrom::Start(end));
        let bytes_until_end_of_line = reader.read_line(&mut buf).unwrap();
        end = end + (bytes_until_end_of_line as u64) - 1; // -1 because read_line() also reads the /n

        intervals.push(Interval { start, end });

        start = end + 1;
        buf.clear();
    }
    intervals
}

fn process_chunk(file_path: &Path, interval: Interval) -> StationsMap {
    let mut reader = get_file_reader(file_path).unwrap();
    _ = reader.seek(SeekFrom::Start(interval.start));
    let chunk_reader = reader.take(interval.end - interval.start);
    build_map(chunk_reader).unwrap()
}

fn build_map(chunk_reader: ChunkReader) -> Result<BTreeMap<String, StationMetrics>> {
    let mut station_to_metrics = StationsMap::new();
    for line in chunk_reader.lines() {
        let line = line?;
        let (city, temperature) = line.split_once(';').unwrap();
        let temperature: f32 = temperature.parse().expect("Incorrect temperature");
        station_to_metrics
            .entry(city.to_string())
            .or_default()
            .update(temperature);
    }
    Ok(station_to_metrics)
}

fn merge_maps(a: StationsMap, b: &StationsMap) -> StationsMap {
    let mut merged_map = a;
    for (k, v) in b {
        merged_map.entry(k.into()).or_default().merge(v);
    }
    merged_map
}

fn print_metrics(station_to_metrics: &StationsMap) {
    // No need to sort as BTreeMap already sorts keys in ascending order.
    for (i, (name, state)) in station_to_metrics.into_iter().enumerate() {
        if i == 0 {
            print!("{name}={state}");
        } else {
            print!(", {name}={state}");
        }
    }
}

#[derive(Debug)]
struct StationMetrics {
    sum_temperature: f64,
    num_records: u32,
    min_temperature: f32,
    max_temperature: f32,
}

impl StationMetrics {
    fn update(&mut self, temperature: f32) {
        self.max_temperature = self.max_temperature.max(temperature);
        self.min_temperature = self.min_temperature.min(temperature);
        self.num_records += 1;
        self.sum_temperature += temperature as f64;
    }

    fn merge(&mut self, other: &Self) {
        self.max_temperature = self.max_temperature.max(other.max_temperature);
        self.min_temperature = self.min_temperature.min(other.min_temperature);
        self.num_records += other.num_records;
        self.sum_temperature += other.sum_temperature;
    }
}

impl Default for StationMetrics {
    fn default() -> Self {
        StationMetrics {
            sum_temperature: 0.0,
            num_records: 0,
            min_temperature: f32::MAX,
            max_temperature: f32::MIN,
        }
    }
}

impl Display for StationMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg_temperature = self.sum_temperature / (self.num_records as f64);
        write!(
            f,
            "{:.1}/{avg_temperature:.1}/{:.1}",
            self.min_temperature, self.max_temperature
        )
    }
}

#[derive(Debug)]
struct Interval {
    start: u64,
    end: u64,
}
