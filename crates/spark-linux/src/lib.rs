#![allow(clippy::cast_precision_loss)]

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use nix::sys::statvfs::{FsFlags, statvfs};
use nix::unistd::{SysconfVar, sysconf};
use spark_schema::{MetricPoint, Quality};

/// Discovers stable Linux hardware and sensor inventory attributes.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn inventory() -> BTreeMap<String, String> {
    let mut inventory = BTreeMap::new();
    inventory_hwmon(&mut inventory);
    inventory_thermal_zones(&mut inventory);
    inventory_block_devices(&mut inventory);
    inventory_network(&mut inventory);
    if let Ok(raw) = fs::read_to_string("/proc/meminfo")
        && let Some(total) = parse_meminfo(&raw).get("MemTotal")
    {
        inventory.insert("host.memory.total_bytes".to_owned(), total.to_string());
    }
    for (key, path) in [
        ("host.os.release", "/etc/dgx-release"),
        ("host.os.pretty_name", "/etc/os-release"),
        ("host.cpu.online", "/sys/devices/system/cpu/online"),
    ] {
        if let Ok(raw) = fs::read_to_string(path) {
            let value = if key == "host.os.pretty_name" {
                raw.lines()
                    .find_map(|line| line.strip_prefix("PRETTY_NAME="))
                    .map(|value| value.trim_matches('"').to_owned())
            } else {
                Some(raw.trim().to_owned())
            };
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                inventory.insert(key.to_owned(), value);
            }
        }
    }
    inventory
}

#[derive(Debug, Clone, Copy)]
struct CpuTimes {
    total: u64,
    idle: u64,
    context_switches: u64,
    runnable: u64,
    blocked: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct NetworkCounters {
    rx_bytes: u64,
    rx_packets: u64,
    rx_errors: u64,
    rx_dropped: u64,
    tx_bytes: u64,
    tx_packets: u64,
    tx_errors: u64,
    tx_dropped: u64,
    carrier_changes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct BlockCounters {
    reads: u64,
    sectors_read: u64,
    read_ms: u64,
    writes: u64,
    sectors_written: u64,
    write_ms: u64,
    busy_ms: u64,
    queue_ms: u64,
}

#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
pub struct SystemCollector {
    previous_cpu: Option<CpuTimes>,
    previous_network: BTreeMap<String, NetworkCounters>,
    previous_block: BTreeMap<String, BlockCounters>,
    previous_vmstat: BTreeMap<String, u64>,
}

impl SystemCollector {
    #[must_use]
    pub fn collect(&mut self) -> Vec<MetricPoint> {
        let mut points = self.collect_core();
        points.extend(self.collect_network_points());
        points.extend(self.collect_storage());
        points
    }

    #[must_use]
    pub fn collect_core(&mut self) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        self.collect_cpu(&mut points);
        collect_uptime(&mut points);
        collect_memory(&mut points);
        collect_pressure("cpu", &mut points);
        collect_pressure("memory", &mut points);
        collect_pressure("io", &mut points);
        self.collect_vmstat(&mut points);
        collect_temperatures(&mut points);
        points
    }

    #[must_use]
    pub fn collect_network_points(&mut self) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        self.collect_network(&mut points);
        points
    }

    #[must_use]
    pub fn collect_storage(&mut self) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        self.collect_block(&mut points);
        collect_filesystem(&mut points);
        points
    }

    fn collect_cpu(&mut self, points: &mut Vec<MetricPoint>) {
        #[allow(clippy::single_match_else)]
        match fs::read_to_string("/proc/stat").and_then(|raw| parse_cpu_times(&raw)) {
            Ok(current) => {
                let previous = self.previous_cpu.replace(current);
                points.push(previous.map_or_else(
                    || {
                        unavailable(
                            "system.cpu.utilization",
                            "1",
                            Quality::Stale,
                            "BASELINE_INITIALIZING",
                        )
                    },
                    |old| {
                        let total = current.total.saturating_sub(old.total);
                        let idle = current.idle.saturating_sub(old.idle);
                        if total == 0 {
                            unavailable(
                                "system.cpu.utilization",
                                "1",
                                Quality::Stale,
                                "ZERO_INTERVAL",
                            )
                        } else {
                            gauge(
                                "system.cpu.utilization",
                                total.saturating_sub(idle) as f64 / total as f64,
                                "1",
                                Quality::Derived,
                            )
                        }
                    },
                ));
                points.push(gauge(
                    "system.cpu.tasks.runnable",
                    current.runnable as f64,
                    "{task}",
                    Quality::Measured,
                ));
                points.push(gauge(
                    "system.cpu.tasks.blocked",
                    current.blocked as f64,
                    "{task}",
                    Quality::Measured,
                ));
                points.push(previous.map_or_else(
                    || {
                        unavailable(
                            "system.cpu.context_switches",
                            "{context_switch}",
                            Quality::Stale,
                            "BASELINE_INITIALIZING",
                        )
                    },
                    |old| {
                        MetricPoint::counter_delta(
                            "system.cpu.context_switches",
                            current
                                .context_switches
                                .saturating_sub(old.context_switches)
                                as f64,
                            "{context_switch}",
                            "procfs",
                        )
                    },
                ));
            }
            Err(_) => {
                points.push(unavailable(
                    "system.cpu.utilization",
                    "1",
                    Quality::Error,
                    "PROC_STAT_READ_FAILED",
                ));
                points.push(unavailable(
                    "system.cpu.context_switches",
                    "{context_switch}",
                    Quality::Error,
                    "PROC_STAT_READ_FAILED",
                ));
            }
        }

        match read_first_f64("/proc/loadavg") {
            Some(value) => points.push(gauge(
                "system.cpu.load_average.1m",
                value,
                "1",
                Quality::Measured,
            )),
            None => points.push(unavailable(
                "system.cpu.load_average.1m",
                "1",
                Quality::Error,
                "PROC_LOADAVG_READ_FAILED",
            )),
        }
        collect_cpu_sysfs(points);
    }

    fn collect_vmstat(&mut self, points: &mut Vec<MetricPoint>) {
        let Ok(raw) = fs::read_to_string("/proc/vmstat") else {
            for (name, unit) in [
                ("system.memory.page_faults.major", "{fault}"),
                ("system.memory.paging", "By"),
                ("system.memory.oom_kills", "{event}"),
            ] {
                points.push(unavailable(
                    name,
                    unit,
                    Quality::Error,
                    "PROC_VMSTAT_READ_FAILED",
                ));
            }
            return;
        };
        let mut current = parse_key_values(&raw);
        let reclaim_pages = ["pgscan_kswapd", "pgscan_direct"]
            .into_iter()
            .filter_map(|key| current.get(key))
            .copied()
            .sum();
        current.insert("spark_reclaim_pages", reclaim_pages);
        for (key, name, unit, multiplier, direction) in [
            (
                "pgmajfault",
                "system.memory.page_faults.major",
                "{fault}",
                1_u64,
                None,
            ),
            (
                "pswpin",
                "system.memory.paging",
                "By",
                page_size(),
                Some("in"),
            ),
            (
                "pswpout",
                "system.memory.paging",
                "By",
                page_size(),
                Some("out"),
            ),
            (
                "oom_kill",
                "system.memory.oom_kills",
                "{event}",
                1_u64,
                None,
            ),
            (
                "spark_reclaim_pages",
                "system.memory.reclaim",
                "By",
                page_size(),
                None,
            ),
        ] {
            let point = current.get(key).copied().map_or_else(
                || unavailable(name, unit, Quality::Unsupported, "VMSTAT_FIELD_MISSING"),
                |value| {
                    self.previous_vmstat.get(key).copied().map_or_else(
                        || unavailable(name, unit, Quality::Stale, "BASELINE_INITIALIZING"),
                        |old| {
                            MetricPoint::counter_delta(
                                name,
                                value.saturating_sub(old).saturating_mul(multiplier) as f64,
                                unit,
                                "procfs",
                            )
                        },
                    )
                },
            );
            points.push(direction.map_or(point.clone(), |value| {
                point.with_attribute("direction", value)
            }));
        }
        self.previous_vmstat = current
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect();
    }

    #[allow(clippy::too_many_lines)]
    fn collect_network(&mut self, points: &mut Vec<MetricPoint>) {
        let Ok(raw) = fs::read_to_string("/proc/net/dev") else {
            return;
        };
        let mut current = parse_network(&raw);
        for (interface, counters) in &mut current {
            counters.carrier_changes = read_trimmed(
                Path::new("/sys/class/net")
                    .join(interface)
                    .join("carrier_changes"),
            )
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        }
        for (interface, counters) in &current {
            if interface == "lo"
                || !Path::new("/sys/class/net")
                    .join(interface)
                    .join("device")
                    .exists()
            {
                continue;
            }
            let old = self.previous_network.get(interface);
            for (name, direction, value, previous, unit) in [
                (
                    "system.network.io",
                    "receive",
                    counters.rx_bytes,
                    old.map(|v| v.rx_bytes),
                    "By",
                ),
                (
                    "system.network.io",
                    "transmit",
                    counters.tx_bytes,
                    old.map(|v| v.tx_bytes),
                    "By",
                ),
                (
                    "system.network.packet.count",
                    "receive",
                    counters.rx_packets,
                    old.map(|v| v.rx_packets),
                    "{packet}",
                ),
                (
                    "system.network.packet.count",
                    "transmit",
                    counters.tx_packets,
                    old.map(|v| v.tx_packets),
                    "{packet}",
                ),
                (
                    "system.network.errors",
                    "receive",
                    counters.rx_errors,
                    old.map(|v| v.rx_errors),
                    "{error}",
                ),
                (
                    "system.network.errors",
                    "transmit",
                    counters.tx_errors,
                    old.map(|v| v.tx_errors),
                    "{error}",
                ),
                (
                    "system.network.packet.dropped",
                    "receive",
                    counters.rx_dropped,
                    old.map(|v| v.rx_dropped),
                    "{packet}",
                ),
                (
                    "system.network.carrier_changes",
                    "bidirectional",
                    counters.carrier_changes,
                    old.map(|v| v.carrier_changes),
                    "{event}",
                ),
                (
                    "system.network.packet.dropped",
                    "transmit",
                    counters.tx_dropped,
                    old.map(|v| v.tx_dropped),
                    "{packet}",
                ),
            ] {
                let point = previous
                    .map_or_else(
                        || unavailable(name, unit, Quality::Stale, "BASELINE_INITIALIZING"),
                        |before| {
                            MetricPoint::counter_delta(
                                name,
                                value.saturating_sub(before) as f64,
                                unit,
                                "procfs",
                            )
                        },
                    )
                    .with_attribute("network.interface.name", interface)
                    .with_attribute("network.io.direction", direction);
                points.push(point);
            }
            let sysfs = Path::new("/sys/class/net").join(interface);
            let up = read_trimmed(sysfs.join("operstate")).is_some_and(|value| value == "up");
            points.push(
                gauge(
                    "system.network.link.up",
                    f64::from(up),
                    "1",
                    Quality::Measured,
                )
                .with_attribute("network.interface.name", interface),
            );
            if let Some(mbps) = read_trimmed(sysfs.join("speed"))
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| *v > 0.0)
            {
                points.push(
                    gauge(
                        "system.network.link.speed",
                        mbps * 1_000_000.0,
                        "bit/s",
                        Quality::Measured,
                    )
                    .with_attribute("network.interface.name", interface),
                );
            }
        }
        self.previous_network = current;
    }

    fn collect_block(&mut self, points: &mut Vec<MetricPoint>) {
        let Ok(entries) = fs::read_dir("/sys/block") else {
            return;
        };
        let mut current = BTreeMap::new();
        for entry in entries.flatten() {
            let device = entry.file_name().to_string_lossy().into_owned();
            if device.starts_with("loop") || device.starts_with("ram") {
                continue;
            }
            let Some(counters) =
                read_trimmed(entry.path().join("stat")).and_then(|raw| parse_block(&raw))
            else {
                continue;
            };
            let old = self.previous_block.get(&device);
            for (name, direction, value, previous, unit) in [
                (
                    "system.disk.io",
                    "read",
                    counters.sectors_read.saturating_mul(512),
                    old.map(|v| v.sectors_read.saturating_mul(512)),
                    "By",
                ),
                (
                    "system.disk.io",
                    "write",
                    counters.sectors_written.saturating_mul(512),
                    old.map(|v| v.sectors_written.saturating_mul(512)),
                    "By",
                ),
                (
                    "system.disk.operation.count",
                    "read",
                    counters.reads,
                    old.map(|v| v.reads),
                    "{operation}",
                ),
                (
                    "system.disk.operation.count",
                    "write",
                    counters.writes,
                    old.map(|v| v.writes),
                    "{operation}",
                ),
                (
                    "system.disk.operation_time",
                    "read",
                    counters.read_ms,
                    old.map(|v| v.read_ms),
                    "ms",
                ),
                (
                    "system.disk.operation_time",
                    "write",
                    counters.write_ms,
                    old.map(|v| v.write_ms),
                    "ms",
                ),
                (
                    "system.disk.operation_time",
                    "busy",
                    counters.busy_ms,
                    old.map(|v| v.busy_ms),
                    "ms",
                ),
                (
                    "system.disk.queue_time",
                    "weighted",
                    counters.queue_ms,
                    old.map(|v| v.queue_ms),
                    "ms",
                ),
            ] {
                points.push(
                    previous
                        .map_or_else(
                            || unavailable(name, unit, Quality::Stale, "BASELINE_INITIALIZING"),
                            |before| {
                                MetricPoint::counter_delta(
                                    name,
                                    value.saturating_sub(before) as f64,
                                    unit,
                                    "sysfs",
                                )
                            },
                        )
                        .with_attribute("device", &device)
                        .with_attribute("direction", direction),
                );
            }
            current.insert(device, counters);
        }
        self.previous_block = current;
    }
}

fn collect_uptime(points: &mut Vec<MetricPoint>) {
    points.push(read_first_f64("/proc/uptime").map_or_else(
        || {
            unavailable(
                "system.uptime",
                "s",
                Quality::Error,
                "PROC_UPTIME_READ_FAILED",
            )
        },
        |value| gauge("system.uptime", value, "s", Quality::Measured),
    ));
}

fn collect_cpu_sysfs(points: &mut Vec<MetricPoint>) {
    let Ok(entries) = fs::read_dir("/sys/devices/system/cpu") else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(id) = name
            .strip_prefix("cpu")
            .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()))
        else {
            continue;
        };
        let online = read_trimmed(entry.path().join("online")).is_none_or(|value| value == "1");
        points.push(
            gauge(
                "system.cpu.online",
                f64::from(online),
                "1",
                Quality::Measured,
            )
            .with_attribute("cpu.logical_number", id),
        );
        if online
            && let Some(khz) = read_trimmed(entry.path().join("cpufreq/scaling_cur_freq"))
                .and_then(|value| value.parse::<f64>().ok())
        {
            points.push(
                gauge(
                    "system.cpu.frequency",
                    khz * 1000.0,
                    "Hz",
                    Quality::Measured,
                )
                .with_attribute("cpu.logical_number", id),
            );
        }
    }
}

fn collect_memory(points: &mut Vec<MetricPoint>) {
    let Ok(raw) = fs::read_to_string("/proc/meminfo") else {
        return;
    };
    let values = parse_meminfo(&raw);
    for (key, name) in [
        ("MemTotal", "system.memory.linux.total"),
        ("MemAvailable", "system.memory.linux.available"),
        ("MemFree", "system.memory.linux.free"),
        ("Cached", "system.memory.cached"),
        ("Buffers", "system.memory.buffers"),
        ("Active", "system.memory.active"),
        ("Inactive", "system.memory.inactive"),
        ("Dirty", "system.memory.dirty"),
        ("Writeback", "system.memory.writeback"),
        ("SReclaimable", "system.memory.slab.reclaimable"),
        ("SUnreclaim", "system.memory.slab.unreclaimable"),
        ("SwapTotal", "system.memory.swap.total"),
        ("SwapFree", "system.memory.swap.free"),
    ] {
        points.push(values.get(key).map_or_else(
            || unavailable(name, "By", Quality::Error, "MEMINFO_FIELD_MISSING"),
            |value| gauge(name, *value as f64, "By", Quality::Measured),
        ));
    }
    let available = values.get("MemAvailable").copied();
    points.push(available.map_or_else(
        || {
            unavailable(
                "spark.uma.allocatable_without_swap",
                "By",
                Quality::Error,
                "MEMINFO_FIELD_MISSING",
            )
        },
        |value| {
            gauge(
                "spark.uma.allocatable_without_swap",
                value as f64,
                "By",
                Quality::Derived,
            )
        },
    ));
    let huge_page_size = values.get("Hugepagesize").copied().unwrap_or(0);
    points.push(allocatable_with_swap(&values).map_or_else(
        || {
            unavailable(
                "spark.uma.allocatable_with_swap",
                "By",
                Quality::Error,
                "MEMINFO_FIELD_MISSING",
            )
        },
        |value| {
            gauge(
                "spark.uma.allocatable_with_swap",
                value as f64,
                "By",
                Quality::Derived,
            )
        },
    ));
    for (key, name) in [
        ("HugePages_Total", "system.memory.hugepages.total"),
        ("HugePages_Free", "system.memory.hugepages.free"),
        ("HugePages_Rsvd", "system.memory.hugepages.reserved"),
    ] {
        points.push(values.get(key).map_or_else(
            || unavailable(name, "By", Quality::Unsupported, "MEMINFO_FIELD_MISSING"),
            |count| {
                gauge(
                    name,
                    count.saturating_mul(huge_page_size) as f64,
                    "By",
                    Quality::Derived,
                )
            },
        ));
    }
}

fn allocatable_with_swap(values: &BTreeMap<&str, u64>) -> Option<u64> {
    let memory = values.get("MemAvailable").copied()?;
    let huge_pages_total = values.get("HugePages_Total").copied().unwrap_or(0);
    let extra = if huge_pages_total > 0 {
        values
            .get("HugePages_Free")
            .copied()
            .unwrap_or(0)
            .saturating_mul(values.get("Hugepagesize").copied().unwrap_or(0))
    } else {
        values.get("SwapFree").copied().unwrap_or(0)
    };
    Some(memory.saturating_add(extra))
}

fn collect_pressure(domain: &str, points: &mut Vec<MetricPoint>) {
    let path = format!("/proc/pressure/{domain}");
    let names: &[(&str, &str)] = match domain {
        "cpu" => &[("some", "spark.pressure.cpu.some")],
        "memory" => &[
            ("some", "spark.pressure.memory.some"),
            ("full", "spark.pressure.memory.full"),
        ],
        "io" => &[
            ("some", "spark.pressure.io.some"),
            ("full", "spark.pressure.io.full"),
        ],
        _ => return,
    };
    match fs::read_to_string(path) {
        Ok(raw) => {
            let parsed = parse_pressure(&raw);
            for (class, name) in names {
                points.push(parsed.get(*class).map_or_else(
                    || unavailable(name, "%", Quality::Error, "PSI_FIELD_MISSING"),
                    |value| gauge(name, *value, "%", Quality::Measured),
                ));
            }
        }
        Err(error) => {
            for (_, name) in names {
                points.push(unavailable(
                    name,
                    "%",
                    if error.kind() == io::ErrorKind::NotFound {
                        Quality::Unsupported
                    } else {
                        Quality::Error
                    },
                    "PSI_READ_FAILED",
                ));
            }
        }
    }
}

fn collect_filesystem(points: &mut Vec<MetricPoint>) {
    let mounts = fs::read_to_string("/proc/self/mountinfo").map_or_else(
        |_| vec![("/".to_owned(), "unknown".to_owned(), "unknown".to_owned())],
        |raw| parse_mountinfo(&raw),
    );
    for (mountpoint, filesystem, device) in mounts {
        collect_one_filesystem(points, &mountpoint, &filesystem, &device);
    }
}

fn collect_one_filesystem(
    points: &mut Vec<MetricPoint>,
    mountpoint: &str,
    filesystem: &str,
    device: &str,
) {
    let Ok(stats) = statvfs(mountpoint) else {
        return;
    };
    let block_size = filesystem_count(stats.fragment_size());
    let total = filesystem_count(stats.blocks()).saturating_mul(block_size);
    let available = filesystem_count(stats.blocks_available()).saturating_mul(block_size);
    let used =
        total.saturating_sub(filesystem_count(stats.blocks_free()).saturating_mul(block_size));
    let attrs = |point: MetricPoint| {
        point
            .with_attribute("mountpoint", mountpoint)
            .with_attribute("filesystem.type", filesystem)
            .with_attribute("filesystem.device", device)
    };
    points.push(attrs(gauge(
        "system.filesystem.limit",
        total as f64,
        "By",
        Quality::Measured,
    )));
    points.push(attrs(
        gauge(
            "system.filesystem.usage",
            used as f64,
            "By",
            Quality::Derived,
        )
        .with_attribute("state", "used"),
    ));
    points.push(attrs(
        gauge(
            "system.filesystem.usage",
            available as f64,
            "By",
            Quality::Measured,
        )
        .with_attribute("state", "available"),
    ));
    points.push(attrs(
        gauge(
            "system.filesystem.inodes",
            filesystem_count(stats.files()) as f64,
            "{inode}",
            Quality::Measured,
        )
        .with_attribute("state", "total"),
    ));
    let files = filesystem_count(stats.files());
    let available_files = filesystem_count(stats.files_free());
    for (state, value, quality) in [
        (
            "used",
            files.saturating_sub(available_files),
            Quality::Derived,
        ),
        ("available", available_files, Quality::Measured),
    ] {
        points.push(attrs(
            gauge("system.filesystem.inodes", value as f64, "{inode}", quality)
                .with_attribute("state", state),
        ));
    }
    points.push(attrs(gauge(
        "system.filesystem.read_only",
        f64::from(stats.flags().contains(FsFlags::ST_RDONLY)),
        "1",
        Quality::Measured,
    )));
}

fn filesystem_count<T: Into<u64>>(value: T) -> u64 {
    value.into()
}

fn parse_mountinfo(raw: &str) -> Vec<(String, String, String)> {
    let mut mounts = BTreeMap::new();
    for line in raw.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let mut prefix = before.split_whitespace();
        let Some(mountpoint) = prefix.nth(4) else {
            continue;
        };
        let mut suffix = after.split_whitespace();
        let (Some(filesystem), Some(device)) = (suffix.next(), suffix.next()) else {
            continue;
        };
        if is_pseudo_filesystem(filesystem) || !is_health_filesystem(filesystem, device) {
            continue;
        }
        mounts.insert(
            decode_mount_field(mountpoint),
            (filesystem.to_owned(), decode_mount_field(device)),
        );
    }
    mounts
        .into_iter()
        .map(|(mountpoint, (filesystem, device))| (mountpoint, filesystem, device))
        .collect()
}

fn is_health_filesystem(filesystem: &str, device: &str) -> bool {
    device.starts_with("/dev/") || matches!(filesystem, "btrfs" | "cifs" | "nfs" | "nfs4" | "zfs")
}

fn decode_mount_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn is_pseudo_filesystem(filesystem: &str) -> bool {
    matches!(
        filesystem,
        "autofs"
            | "bpf"
            | "cgroup"
            | "cgroup2"
            | "configfs"
            | "debugfs"
            | "devpts"
            | "devtmpfs"
            | "fusectl"
            | "hugetlbfs"
            | "mqueue"
            | "proc"
            | "pstore"
            | "securityfs"
            | "squashfs"
            | "sysfs"
            | "tracefs"
    )
}

fn collect_temperatures(points: &mut Vec<MetricPoint>) {
    let Ok(hwmons) = fs::read_dir("/sys/class/hwmon") else {
        return;
    };
    let mut maximum = None::<f64>;
    for hwmon in hwmons.flatten() {
        let name = read_trimmed(hwmon.path().join("name"))
            .unwrap_or_else(|| hwmon.file_name().to_string_lossy().into_owned());
        let Ok(channels) = fs::read_dir(hwmon.path()) else {
            continue;
        };
        for channel in channels.flatten() {
            let filename = channel.file_name().to_string_lossy().into_owned();
            let Some(id) = filename
                .strip_prefix("temp")
                .and_then(|v| v.strip_suffix("_input"))
            else {
                continue;
            };
            let Some(milli_celsius) =
                read_trimmed(channel.path()).and_then(|v| v.parse::<f64>().ok())
            else {
                continue;
            };
            let label = read_trimmed(hwmon.path().join(format!("temp{id}_label")))
                .unwrap_or_else(|| format!("temp{id}"));
            let value = milli_celsius / 1000.0;
            maximum = Some(maximum.map_or(value, |old| old.max(value)));
            let mut point = gauge("system.temperature", value, "Cel", Quality::Measured)
                .with_attribute("sensor", &name)
                .with_attribute("channel", label);
            for (suffix, attribute) in [
                ("max", "temperature.limit.max_celsius"),
                ("crit", "temperature.limit.critical_celsius"),
            ] {
                if let Some(limit) = read_trimmed(hwmon.path().join(format!("temp{id}_{suffix}")))
                    .and_then(|value| value.parse::<f64>().ok())
                {
                    point = point.with_attribute(attribute, (limit / 1000.0).to_string());
                }
            }
            points.push(point);
        }
    }
    if let Some(value) = maximum {
        points.push(
            gauge("system.temperature", value, "Cel", Quality::Derived)
                .with_attribute("aggregation", "maximum"),
        );
    }
}

fn inventory_hwmon(inventory: &mut BTreeMap<String, String>) {
    let Ok(entries) = fs::read_dir("/sys/class/hwmon") else {
        return;
    };
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().into_owned();
        let name = read_trimmed(entry.path().join("name")).unwrap_or_else(|| id.clone());
        inventory.insert(format!("linux.hwmon.{id}.name"), name);
        let Ok(channels) = fs::read_dir(entry.path()) else {
            continue;
        };
        for channel in channels.flatten() {
            let filename = channel.file_name().to_string_lossy().into_owned();
            let Some((temp, property)) = filename
                .strip_prefix("temp")
                .and_then(|value| value.split_once('_'))
            else {
                continue;
            };
            if !temp.bytes().all(|byte| byte.is_ascii_digit())
                || !matches!(
                    property,
                    "label" | "min" | "max" | "crit" | "emergency" | "lcrit"
                )
            {
                continue;
            }
            if let Some(value) = read_trimmed(channel.path()) {
                let value = if property == "label" {
                    value
                } else {
                    value
                        .parse::<f64>()
                        .map_or(value.clone(), |milli| (milli / 1000.0).to_string())
                };
                inventory.insert(format!("linux.hwmon.{id}.temp{temp}.{property}"), value);
            }
        }
    }
}

fn inventory_thermal_zones(inventory: &mut BTreeMap<String, String>) {
    let Ok(entries) = fs::read_dir("/sys/class/thermal") else {
        return;
    };
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().into_owned();
        if !id.starts_with("thermal_zone") {
            continue;
        }
        if let Some(value) = read_trimmed(entry.path().join("type")) {
            inventory.insert(format!("linux.thermal.{id}.type"), value);
        }
    }
}

fn inventory_block_devices(inventory: &mut BTreeMap<String, String>) {
    let Ok(entries) = fs::read_dir("/sys/block") else {
        return;
    };
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().into_owned();
        if id.starts_with("loop") || id.starts_with("ram") {
            continue;
        }
        for (property, path) in [
            ("model", "device/model"),
            ("firmware", "device/firmware_rev"),
        ] {
            if let Some(value) = read_trimmed(entry.path().join(path)) {
                inventory.insert(format!("host.disk.{id}.{property}"), value);
            }
        }
        if let Some(sectors) =
            read_trimmed(entry.path().join("size")).and_then(|value| value.parse::<u64>().ok())
        {
            inventory.insert(
                format!("host.disk.{id}.capacity_bytes"),
                sectors.saturating_mul(512).to_string(),
            );
        }
    }
}

fn inventory_network(inventory: &mut BTreeMap<String, String>) {
    let Ok(entries) = fs::read_dir("/sys/class/net") else {
        return;
    };
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().into_owned();
        if !entry.path().join("device").exists() {
            continue;
        }
        inventory.insert(format!("host.network.{id}.name"), id.clone());
        if let Ok(driver) = fs::read_link(entry.path().join("device/driver"))
            && let Some(driver) = driver.file_name()
        {
            inventory.insert(
                format!("host.network.{id}.driver"),
                driver.to_string_lossy().into_owned(),
            );
        }
    }
}

fn gauge(name: &str, value: f64, unit: &str, quality: Quality) -> MetricPoint {
    MetricPoint::gauge(
        name,
        value,
        unit,
        quality,
        if name.starts_with("system.temperature")
            || name.starts_with("system.disk")
            || name.starts_with("system.network.link")
        {
            "sysfs"
        } else {
            "procfs"
        },
    )
}

fn unavailable(name: &str, unit: &str, quality: Quality, code: &str) -> MetricPoint {
    MetricPoint::unavailable(name, unit, quality, "procfs", code)
}

fn read_first_f64(path: &str) -> Option<f64> {
    fs::read_to_string(path)
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn read_trimmed(path: PathBuf) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn page_size() -> u64 {
    sysconf(SysconfVar::PAGE_SIZE)
        .ok()
        .flatten()
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(4096)
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
    let field = |name: &str| {
        raw.lines()
            .find_map(|row| row.strip_prefix(name)?.trim().parse().ok())
            .unwrap_or(0)
    };
    Ok(CpuTimes {
        total: values.iter().sum(),
        idle: values[3].saturating_add(values.get(4).copied().unwrap_or(0)),
        context_switches: field("ctxt "),
        runnable: field("procs_running "),
        blocked: field("procs_blocked "),
    })
}

fn parse_meminfo(raw: &str) -> BTreeMap<&str, u64> {
    raw.lines()
        .filter_map(|line| {
            let (key, rest) = line.split_once(':')?;
            let mut fields = rest.split_whitespace();
            let value = fields.next()?.parse::<u64>().ok()?;
            let multiplier = if fields.next() == Some("kB") { 1024 } else { 1 };
            Some((key, value.saturating_mul(multiplier)))
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

fn parse_key_values(raw: &str) -> BTreeMap<&str, u64> {
    raw.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((fields.next()?, fields.next()?.parse().ok()?))
        })
        .collect()
}

fn parse_network(raw: &str) -> BTreeMap<String, NetworkCounters> {
    raw.lines()
        .skip(2)
        .filter_map(|line| {
            let (name, values) = line.split_once(':')?;
            let values = values
                .split_whitespace()
                .map(str::parse::<u64>)
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            (values.len() >= 16).then(|| {
                (
                    name.trim().to_owned(),
                    NetworkCounters {
                        rx_bytes: values[0],
                        rx_packets: values[1],
                        rx_errors: values[2],
                        rx_dropped: values[3],
                        tx_bytes: values[8],
                        tx_packets: values[9],
                        tx_errors: values[10],
                        tx_dropped: values[11],
                        carrier_changes: 0,
                    },
                )
            })
        })
        .collect()
}

fn parse_block(raw: &str) -> Option<BlockCounters> {
    let values = raw
        .split_whitespace()
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    (values.len() >= 11).then(|| BlockCounters {
        reads: values[0],
        sectors_read: values[2],
        read_ms: values[3],
        writes: values[4],
        sectors_written: values[6],
        write_ms: values[7],
        busy_ms: values[9],
        queue_ms: values[10],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_counters() {
        let times =
            parse_cpu_times("cpu  10 2 3 80 5 0 0 0\nctxt 42\nprocs_running 3\nprocs_blocked 1\n")
                .unwrap();
        assert_eq!(times.total, 100);
        assert_eq!(times.idle, 85);
        assert_eq!(times.context_switches, 42);
    }

    #[test]
    fn parses_meminfo_units_and_counts() {
        let values = parse_meminfo("MemTotal: 100 kB\nHugePages_Total: 4\nHugepagesize: 2048 kB\n");
        assert_eq!(values["MemTotal"], 102_400);
        assert_eq!(values["HugePages_Total"], 4);
        assert_eq!(values["Hugepagesize"], 2_097_152);
    }

    #[test]
    fn parses_psi_average() {
        let values = parse_pressure(
            "some avg10=0.25 avg60=0.10 avg300=0.01 total=12\nfull avg10=0.02 avg60=0.01 avg300=0.00 total=2\n",
        );
        assert!((values["some"] - 0.25).abs() < f64::EPSILON);
        assert!((values["full"] - 0.02).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_network_counters() {
        let values = parse_network(
            "Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n eth0: 10 2 1 3 0 0 0 0 20 4 2 5 0 0 0 0\n",
        );
        assert_eq!(values["eth0"].rx_bytes, 10);
        assert_eq!(values["eth0"].tx_dropped, 5);
    }

    #[test]
    fn explicit_hugepages_replace_swap_in_allocatable_capacity() {
        let values = parse_meminfo(
            "MemAvailable: 100 kB\nSwapFree: 1000 kB\nHugePages_Total: 4\nHugePages_Free: 2\nHugepagesize: 2048 kB\n",
        );
        assert_eq!(
            allocatable_with_swap(&values),
            Some(100 * 1024 + 2 * 2048 * 1024)
        );
    }

    #[test]
    fn parses_physical_mounts_and_decodes_spaces() {
        let mounts = parse_mountinfo(
            "36 25 259:2 / / rw - ext4 /dev/nvme0n1p2 rw\n37 25 0:4 / /proc rw - proc proc rw\n38 25 8:1 / /media/My\\040Disk rw - xfs /dev/sda1 rw\n39 25 7:1 / /snap/tool/1 ro - squashfs /dev/loop1 ro\n",
        );
        assert_eq!(mounts.len(), 2);
        assert!(mounts.iter().any(|mount| mount.0 == "/media/My Disk"));
    }
}
