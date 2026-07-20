#![allow(clippy::cast_precision_loss)]

use std::{collections::BTreeMap, fs, io, path::Path};

use spark_schema::{MetricPoint, Quality};

#[derive(Debug, Clone, Copy)]
struct CpuTimes {
    total: u64,
    idle: u64,
}

#[derive(Debug, Default)]
pub struct SystemCollector {
    previous_cpu: Option<CpuTimes>,
}

impl SystemCollector {
    #[must_use]
    pub fn collect(&mut self) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        self.collect_cpu(&mut points);
        collect_uptime(&mut points);
        collect_memory(&mut points);
        collect_pressure("cpu", &mut points);
        collect_pressure("memory", &mut points);
        points
    }

    fn collect_cpu(&mut self, points: &mut Vec<MetricPoint>) {
        match fs::read_to_string("/proc/stat").and_then(|raw| parse_cpu_times(&raw)) {
            Ok(current) => {
                let utilization = self.previous_cpu.and_then(|previous| {
                    let total = current.total.saturating_sub(previous.total);
                    let idle = current.idle.saturating_sub(previous.idle);
                    (total > 0).then(|| (total.saturating_sub(idle) as f64) / (total as f64))
                });
                self.previous_cpu = Some(current);
                points.push(utilization.map_or_else(
                    || {
                        MetricPoint::unavailable(
                            "system.cpu.utilization",
                            "1",
                            Quality::Stale,
                            "procfs",
                            "BASELINE_INITIALIZING",
                        )
                    },
                    |value| {
                        MetricPoint::gauge(
                            "system.cpu.utilization",
                            value,
                            "1",
                            Quality::Derived,
                            "procfs",
                        )
                    },
                ));
            }
            Err(_) => points.push(MetricPoint::unavailable(
                "system.cpu.utilization",
                "1",
                Quality::Error,
                "procfs",
                "PROC_STAT_READ_FAILED",
            )),
        }

        match fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|raw| raw.split_whitespace().next()?.parse::<f64>().ok())
        {
            Some(value) => points.push(MetricPoint::gauge(
                "system.cpu.load_average.1m",
                value,
                "1",
                Quality::Measured,
                "procfs",
            )),
            None => points.push(MetricPoint::unavailable(
                "system.cpu.load_average.1m",
                "1",
                Quality::Error,
                "procfs",
                "PROC_LOADAVG_READ_FAILED",
            )),
        }
    }
}

fn collect_uptime(points: &mut Vec<MetricPoint>) {
    match fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|raw| raw.split_whitespace().next()?.parse::<f64>().ok())
    {
        Some(value) => points.push(MetricPoint::gauge(
            "system.uptime",
            value,
            "s",
            Quality::Measured,
            "procfs",
        )),
        None => points.push(MetricPoint::unavailable(
            "system.uptime",
            "s",
            Quality::Error,
            "procfs",
            "PROC_UPTIME_READ_FAILED",
        )),
    }
}

fn collect_memory(points: &mut Vec<MetricPoint>) {
    let Ok(raw) = fs::read_to_string("/proc/meminfo") else {
        for (name, unit) in memory_metrics() {
            points.push(MetricPoint::unavailable(
                name,
                unit,
                Quality::Error,
                "procfs",
                "PROC_MEMINFO_READ_FAILED",
            ));
        }
        return;
    };
    let values = parse_meminfo(&raw);
    let mappings = [
        ("MemTotal", "system.memory.linux.total"),
        ("MemAvailable", "system.memory.linux.available"),
        ("MemFree", "system.memory.linux.free"),
        ("Cached", "system.memory.cached"),
        ("Buffers", "system.memory.buffers"),
        ("SwapTotal", "system.memory.swap.total"),
        ("SwapFree", "system.memory.swap.free"),
    ];
    for (key, name) in mappings {
        points.push(values.get(key).map_or_else(
            || {
                MetricPoint::unavailable(
                    name,
                    "By",
                    Quality::Error,
                    "procfs",
                    "MEMINFO_FIELD_MISSING",
                )
            },
            |value| MetricPoint::gauge(name, *value as f64, "By", Quality::Measured, "procfs"),
        ));
    }
    let available = values.get("MemAvailable").copied();
    let swap_free = values.get("SwapFree").copied();
    points.push(available.map_or_else(
        || {
            MetricPoint::unavailable(
                "spark.uma.allocatable_without_swap",
                "By",
                Quality::Error,
                "procfs",
                "MEMINFO_FIELD_MISSING",
            )
        },
        |value| {
            MetricPoint::gauge(
                "spark.uma.allocatable_without_swap",
                value as f64,
                "By",
                Quality::Derived,
                "procfs",
            )
        },
    ));
    points.push(available.zip(swap_free).map_or_else(
        || {
            MetricPoint::unavailable(
                "spark.uma.allocatable_with_swap",
                "By",
                Quality::Error,
                "procfs",
                "MEMINFO_FIELD_MISSING",
            )
        },
        |(memory, swap)| {
            MetricPoint::gauge(
                "spark.uma.allocatable_with_swap",
                memory.saturating_add(swap) as f64,
                "By",
                Quality::Derived,
                "procfs",
            )
        },
    ));
}

fn collect_pressure(domain: &str, points: &mut Vec<MetricPoint>) {
    let path = format!("/proc/pressure/{domain}");
    let names: &[(&str, &str)] = if domain == "cpu" {
        &[("some", "spark.pressure.cpu.some")]
    } else {
        &[
            ("some", "spark.pressure.memory.some"),
            ("full", "spark.pressure.memory.full"),
        ]
    };
    match fs::read_to_string(Path::new(&path)) {
        Ok(raw) => {
            let parsed = parse_pressure(&raw);
            for (class, name) in names {
                points.push(parsed.get(*class).map_or_else(
                    || {
                        MetricPoint::unavailable(
                            name,
                            "%",
                            Quality::Error,
                            "procfs",
                            "PSI_FIELD_MISSING",
                        )
                    },
                    |value| MetricPoint::gauge(name, *value, "%", Quality::Measured, "procfs"),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            for (_, name) in names {
                points.push(MetricPoint::unavailable(
                    name,
                    "%",
                    Quality::Unsupported,
                    "procfs",
                    "PSI_UNSUPPORTED",
                ));
            }
        }
        Err(_) => {
            for (_, name) in names {
                points.push(MetricPoint::unavailable(
                    name,
                    "%",
                    Quality::Error,
                    "procfs",
                    "PSI_READ_FAILED",
                ));
            }
        }
    }
}

fn memory_metrics() -> &'static [(&'static str, &'static str)] {
    &[
        ("system.memory.linux.total", "By"),
        ("system.memory.linux.available", "By"),
        ("system.memory.linux.free", "By"),
        ("system.memory.cached", "By"),
        ("system.memory.buffers", "By"),
        ("system.memory.swap.total", "By"),
        ("system.memory.swap.free", "By"),
        ("spark.uma.allocatable_without_swap", "By"),
        ("spark.uma.allocatable_with_swap", "By"),
    ]
}

fn parse_cpu_times(raw: &str) -> io::Result<CpuTimes> {
    let line = raw
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "aggregate cpu row missing"))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if values.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "aggregate cpu row too short",
        ));
    }
    Ok(CpuTimes {
        total: values.iter().sum(),
        idle: values[3].saturating_add(values.get(4).copied().unwrap_or(0)),
    })
}

fn parse_meminfo(raw: &str) -> BTreeMap<&str, u64> {
    raw.lines()
        .filter_map(|line| {
            let (key, rest) = line.split_once(':')?;
            let kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            Some((key, kib.saturating_mul(1024)))
        })
        .collect()
}

fn parse_pressure(raw: &str) -> BTreeMap<&str, f64> {
    raw.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let class = fields.next()?;
            let avg10 = fields
                .find_map(|field| field.strip_prefix("avg10="))?
                .parse()
                .ok()?;
            Some((class, avg10))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_counters() {
        let times = parse_cpu_times("cpu  10 2 3 80 5 0 0 0\n").unwrap();
        assert_eq!(times.total, 100);
        assert_eq!(times.idle, 85);
    }

    #[test]
    fn parses_meminfo_as_bytes() {
        let values = parse_meminfo("MemTotal:       100 kB\nMemAvailable: 40 kB\n");
        assert_eq!(values["MemTotal"], 102_400);
        assert_eq!(values["MemAvailable"], 40_960);
    }

    #[test]
    fn parses_psi_average() {
        let values = parse_pressure(
            "some avg10=0.25 avg60=0.10 avg300=0.01 total=12\nfull avg10=0.02 avg60=0.01 avg300=0.00 total=2\n",
        );
        assert!((values["some"] - 0.25).abs() < f64::EPSILON);
        assert!((values["full"] - 0.02).abs() < f64::EPSILON);
    }
}
