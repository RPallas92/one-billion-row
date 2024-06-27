use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::File,
    io::{BufRead, BufReader, Result},
    time::Instant,
};

fn main() {
    /*
    The release build is executed in around 90 seconds on SER5 PRO MAX:
       - CPU: AMD Ryzen 7 5800H with Radeon Graphics (16) @ 3.200GHz
       - GPU: AMD ATI Radeon Vega Series / Radeon Vega Mobile Series
       - Memory: 28993MiB
    */
    let start = Instant::now();

    let reader = get_file_reader().unwrap();
    let station_to_metrics = build_map(reader).unwrap();
    print_metrics(station_to_metrics);

    let duration = start.elapsed();
    println!("\n Execution time: {:?}", duration);
}

fn get_file_reader() -> Result<BufReader<File>> {
    let file: File = File::open("./data/weather_stations.csv")?;
    Ok(BufReader::new(file))
}

fn build_map(file_reader: BufReader<File>) -> Result<BTreeMap<String, StationMetrics>> {
    let mut station_to_metrics = BTreeMap::<String, StationMetrics>::new();
    for line in file_reader.lines() {
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

// BTreeMap already sorts keys in ascending order.
fn print_metrics(station_to_metrics: BTreeMap<String, StationMetrics>) {
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
