#![allow(clippy::cast_precision_loss)]

use std::{
    collections::{BTreeMap, BTreeSet},
    process::Command,
    sync::{Mutex, mpsc},
};

use nvml_wrapper::{
    Nvml, cuda_driver_version_major, cuda_driver_version_minor,
    enum_wrappers::device::{Clock, PerformanceState, TemperatureSensor},
    enums::device::UsedGpuMemory,
    error::NvmlError,
};
use spark_schema::{MetricPoint, Quality};

pub struct NvidiaCollector {
    nvml: Option<Nvml>,
    include_process_allocations: bool,
    xid: XidMonitor,
}

#[allow(dead_code)]
enum XidMessage {
    Ready { uuid: String },
    Event { uuid: String, code: String },
    Unsupported { uuid: String },
}

struct XidMonitor {
    receiver: Mutex<mpsc::Receiver<XidMessage>>,
    ready: Mutex<BTreeSet<String>>,
    unsupported: Mutex<BTreeSet<String>>,
}

impl NvidiaCollector {
    #[must_use]
    pub fn new(include_process_allocations: bool) -> Self {
        Self {
            nvml: Nvml::init().ok(),
            include_process_allocations,
            xid: XidMonitor::start(),
        }
    }

    #[must_use]
    pub fn inventory(&self) -> BTreeMap<String, String> {
        let mut inventory = BTreeMap::new();
        if let Some(nvml) = &self.nvml {
            if let Ok(version) = nvml.sys_driver_version() {
                inventory.insert("nvidia.driver.version".to_owned(), version);
            }
            if let Ok(version) = nvml.sys_cuda_driver_version() {
                inventory.insert(
                    "nvidia.cuda.driver_compatibility".to_owned(),
                    format!(
                        "{}.{}",
                        cuda_driver_version_major(version),
                        cuda_driver_version_minor(version)
                    ),
                );
            }
            if let Ok(count) = nvml.device_count() {
                inventory.insert("nvidia.gpu.count".to_owned(), count.to_string());
                for index in 0..count {
                    if let Ok(device) = nvml.device_by_index(index) {
                        if let Ok(name) = device.name() {
                            if is_gb10(&name) {
                                inventory.insert(
                                    "spark.memory.architecture".to_owned(),
                                    "unified".to_owned(),
                                );
                                inventory.insert(
                                    "spark.memory.bandwidth.capability_gbps".to_owned(),
                                    "273".to_owned(),
                                );
                            }
                            inventory.insert(format!("nvidia.gpu.{index}.name"), name);
                        }
                        if let Ok(uuid) = device.uuid() {
                            inventory.insert(format!("nvidia.gpu.{index}.uuid"), uuid);
                        }
                    }
                }
            }
        }
        inventory
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn collect(&self) -> Vec<MetricPoint> {
        let Some(nvml) = &self.nvml else {
            return collect_smi_fallback();
        };
        let Ok(count) = nvml.device_count() else {
            return vec![unavailable(
                "nvidia.gpu.utilization",
                "%",
                "NVML_DEVICE_COUNT_FAILED",
            )];
        };
        let mut points = Vec::new();
        for index in 0..count {
            let Ok(device) = nvml.device_by_index(index) else {
                continue;
            };
            let uuid = device.uuid().unwrap_or_else(|_| format!("index-{index}"));
            let decorate = |point: MetricPoint| {
                point
                    .with_attribute("gpu.id", &uuid)
                    .with_attribute("gpu.index", index.to_string())
            };

            match device.utilization_rates() {
                Ok(value) => {
                    points.push(decorate(gauge(
                        "nvidia.gpu.utilization",
                        f64::from(value.gpu),
                        "%",
                    )));
                    points.push(decorate(gauge(
                        "nvidia.gpu.memory_controller.utilization",
                        f64::from(value.memory),
                        "%",
                    )));
                }
                Err(error) => {
                    points.push(decorate(nvml_unavailable(
                        "nvidia.gpu.utilization",
                        "%",
                        &error,
                    )));
                    points.push(decorate(nvml_unavailable(
                        "nvidia.gpu.memory_controller.utilization",
                        "%",
                        &error,
                    )));
                }
            }
            points.push(decorate(
                device.temperature(TemperatureSensor::Gpu).map_or_else(
                    |error| nvml_unavailable("nvidia.gpu.temperature", "Cel", &error),
                    |value| gauge("nvidia.gpu.temperature", f64::from(value), "Cel"),
                ),
            ));
            points.push(decorate(device.power_usage().map_or_else(
                |error| nvml_unavailable("nvidia.gpu.power.draw", "W", &error),
                |value| gauge("nvidia.gpu.power.draw", f64::from(value) / 1000.0, "W"),
            )));
            for (clock, label) in [(Clock::Graphics, "graphics"), (Clock::Memory, "memory")] {
                points.push(decorate(
                    device
                        .clock_info(clock)
                        .map_or_else(
                            |error| nvml_unavailable("nvidia.gpu.clock.frequency", "MHz", &error),
                            |value| gauge("nvidia.gpu.clock.frequency", f64::from(value), "MHz"),
                        )
                        .with_attribute("clock.domain", label),
                ));
            }
            points.push(decorate(device.current_throttle_reasons().map_or_else(
                |error| nvml_unavailable("nvidia.gpu.throttle", "1", &error),
                |reasons| gauge("nvidia.gpu.throttle", reasons.bits() as f64, "1"),
            )));
            points.push(decorate(device.performance_state().map_or_else(
                |error| nvml_unavailable("nvidia.gpu.performance_state", "1", &error),
                |state| {
                    gauge(
                        "nvidia.gpu.performance_state",
                        performance_state_value(state),
                        "1",
                    )
                    .with_attribute("nvidia.performance_state", format!("{state:?}"))
                },
            )));
            for (name, utilization) in [
                (
                    "nvidia.gpu.encoder.utilization",
                    device.encoder_utilization(),
                ),
                (
                    "nvidia.gpu.decoder.utilization",
                    device.decoder_utilization(),
                ),
            ] {
                points.push(decorate(utilization.map_or_else(
                    |error| nvml_unavailable(name, "%", &error),
                    |value| gauge(name, f64::from(value.utilization), "%"),
                )));
            }
            if self.include_process_allocations {
                match device.running_compute_processes() {
                    Ok(processes) => {
                        for process in processes {
                            let point = match process.used_gpu_memory {
                                UsedGpuMemory::Used(bytes) => gauge(
                                    "nvidia.gpu.process.memory.allocation",
                                    bytes as f64,
                                    "By",
                                ),
                                UsedGpuMemory::Unavailable => unavailable(
                                    "nvidia.gpu.process.memory.allocation",
                                    "By",
                                    "NVML_MEMORY_UNAVAILABLE",
                                ),
                            }
                            .with_attribute("process.pid", process.pid.to_string());
                            points.push(decorate(point));
                        }
                    }
                    Err(error) => points.push(decorate(nvml_unavailable(
                        "nvidia.gpu.process.memory.allocation",
                        "By",
                        &error,
                    ))),
                }
            }
        }
        points.extend(self.xid.collect());
        points
    }
}

impl XidMonitor {
    fn start() -> Self {
        let (sender, receiver) = mpsc::channel();
        start_xid_workers(sender);
        Self {
            receiver: Mutex::new(receiver),
            ready: Mutex::new(BTreeSet::new()),
            unsupported: Mutex::new(BTreeSet::new()),
        }
    }

    fn collect(&self) -> Vec<MetricPoint> {
        let mut events = Vec::new();
        let mut event_gpus = BTreeSet::new();
        if let Ok(receiver) = self.receiver.lock() {
            for message in receiver.try_iter() {
                match message {
                    XidMessage::Ready { uuid } => {
                        if let Ok(mut ready) = self.ready.lock() {
                            ready.insert(uuid);
                        }
                    }
                    XidMessage::Unsupported { uuid } => {
                        if let Ok(mut unsupported) = self.unsupported.lock() {
                            unsupported.insert(uuid);
                        }
                    }
                    XidMessage::Event { uuid, code } => {
                        event_gpus.insert(uuid.clone());
                        events.push(
                            MetricPoint::counter_delta(
                                "nvidia.gpu.xid.count",
                                1.0,
                                "{event}",
                                "nvml",
                            )
                            .with_attribute("gpu.id", uuid)
                            .with_attribute("nvidia.xid.code", code),
                        );
                    }
                }
            }
        }
        if let Ok(ready) = self.ready.lock() {
            events.extend(
                ready
                    .iter()
                    .filter(|uuid| !event_gpus.contains(*uuid))
                    .map(|uuid| {
                        MetricPoint::counter_delta("nvidia.gpu.xid.count", 0.0, "{event}", "nvml")
                            .with_attribute("gpu.id", uuid)
                    }),
            );
        }
        if let Ok(unsupported) = self.unsupported.lock() {
            events.extend(unsupported.iter().map(|uuid| {
                MetricPoint::unavailable(
                    "nvidia.gpu.xid.count",
                    "{event}",
                    Quality::Unsupported,
                    "nvml",
                    "NVML_XID_NOT_SUPPORTED",
                )
                .with_attribute("gpu.id", uuid)
            }));
        }
        events
    }
}

#[cfg(target_os = "linux")]
fn start_xid_workers(sender: mpsc::Sender<XidMessage>) {
    use nvml_wrapper::bitmasks::event::EventTypes;

    std::thread::spawn(move || {
        let Ok(nvml) = Nvml::init() else { return };
        let Ok(count) = nvml.device_count() else {
            return;
        };
        for index in 0..count {
            let sender = sender.clone();
            std::thread::spawn(move || {
                let Ok(nvml) = Nvml::init() else { return };
                let Ok(device) = nvml.device_by_index(index) else {
                    return;
                };
                let uuid = device.uuid().unwrap_or_else(|_| format!("index-{index}"));
                let Ok(supported) = device.supported_event_types() else {
                    let _ = sender.send(XidMessage::Unsupported { uuid });
                    return;
                };
                if !supported.contains(EventTypes::CRITICAL_XID_ERROR) {
                    let _ = sender.send(XidMessage::Unsupported { uuid });
                    return;
                }
                let Ok(set) = nvml.create_event_set() else {
                    let _ = sender.send(XidMessage::Unsupported { uuid });
                    return;
                };
                let Ok(set) = device.register_events(EventTypes::CRITICAL_XID_ERROR, set) else {
                    let _ = sender.send(XidMessage::Unsupported { uuid });
                    return;
                };
                let _ = sender.send(XidMessage::Ready { uuid: uuid.clone() });
                loop {
                    match set.wait(1000) {
                        Ok(event) if event.event_type.contains(EventTypes::CRITICAL_XID_ERROR) => {
                            let code = event
                                .event_data
                                .map_or_else(|| "unknown".to_owned(), |value| format!("{value:?}"));
                            if sender
                                .send(XidMessage::Event {
                                    uuid: uuid.clone(),
                                    code,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(NvmlError::Timeout) | Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
        }
    });
}

#[cfg(not(target_os = "linux"))]
fn start_xid_workers(_sender: mpsc::Sender<XidMessage>) {}

fn collect_smi_fallback() -> Vec<MetricPoint> {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=uuid,utilization.gpu,utilization.memory,temperature.gpu,power.draw,clocks.current.graphics,clocks.current.memory",
            "--format=csv,noheader,nounits",
        ])
        .output();
    let Ok(output) = output else {
        return vec![unavailable(
            "nvidia.gpu.utilization",
            "%",
            "NVML_AND_NVSMI_UNAVAILABLE",
        )];
    };
    if !output.status.success() {
        return vec![unavailable(
            "nvidia.gpu.utilization",
            "%",
            "NVSMI_QUERY_FAILED",
        )];
    }
    parse_smi_csv(&String::from_utf8_lossy(&output.stdout))
}

fn parse_smi_csv(raw: &str) -> Vec<MetricPoint> {
    let mut points = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        let fields: Vec<_> = line.split(',').map(str::trim).collect();
        if fields.len() != 7 {
            continue;
        }
        let decorate = |point: MetricPoint| {
            point
                .with_attribute("gpu.id", fields[0])
                .with_attribute("gpu.index", index.to_string())
        };
        for (name, unit, field, attribute) in [
            ("nvidia.gpu.utilization", "%", fields[1], None),
            (
                "nvidia.gpu.memory_controller.utilization",
                "%",
                fields[2],
                None,
            ),
            ("nvidia.gpu.temperature", "Cel", fields[3], None),
            ("nvidia.gpu.power.draw", "W", fields[4], None),
            (
                "nvidia.gpu.clock.frequency",
                "MHz",
                fields[5],
                Some("graphics"),
            ),
            (
                "nvidia.gpu.clock.frequency",
                "MHz",
                fields[6],
                Some("memory"),
            ),
        ] {
            let mut point = parse_smi_value(name, unit, field);
            if let Some(clock) = attribute {
                point = point.with_attribute("clock.domain", clock);
            }
            points.push(decorate(point));
        }
    }
    points
}

fn is_gb10(name: &str) -> bool {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .any(|part| part.eq_ignore_ascii_case("gb10"))
}

const fn performance_state_value(state: PerformanceState) -> f64 {
    match state {
        PerformanceState::Zero => 0.0,
        PerformanceState::One => 1.0,
        PerformanceState::Two => 2.0,
        PerformanceState::Three => 3.0,
        PerformanceState::Four => 4.0,
        PerformanceState::Five => 5.0,
        PerformanceState::Six => 6.0,
        PerformanceState::Seven => 7.0,
        PerformanceState::Eight => 8.0,
        PerformanceState::Nine => 9.0,
        PerformanceState::Ten => 10.0,
        PerformanceState::Eleven => 11.0,
        PerformanceState::Twelve => 12.0,
        PerformanceState::Thirteen => 13.0,
        PerformanceState::Fourteen => 14.0,
        PerformanceState::Fifteen => 15.0,
        PerformanceState::Unknown => -1.0,
    }
}

fn parse_smi_value(name: &str, unit: &str, field: &str) -> MetricPoint {
    field.parse::<f64>().map_or_else(
        |_| {
            MetricPoint::unavailable(
                name,
                unit,
                Quality::Unsupported,
                "nvidia-smi",
                "NVSMI_NOT_SUPPORTED",
            )
        },
        |value| MetricPoint::gauge(name, value, unit, Quality::Measured, "nvidia-smi"),
    )
}

fn gauge(name: &str, value: f64, unit: &str) -> MetricPoint {
    MetricPoint::gauge(name, value, unit, Quality::Measured, "nvml")
}

fn unavailable(name: &str, unit: &str, code: &str) -> MetricPoint {
    MetricPoint::unavailable(name, unit, Quality::Unsupported, "nvml", code)
}

fn nvml_unavailable(name: &str, unit: &str, error: &NvmlError) -> MetricPoint {
    let quality = if matches!(error, NvmlError::NotSupported) {
        Quality::Unsupported
    } else {
        Quality::Error
    };
    MetricPoint::unavailable(
        name,
        unit,
        quality,
        "nvml",
        &format!("NVML_{error:?}").to_ascii_uppercase(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smi_na_is_unsupported_not_zero() {
        let point = parse_smi_value("nvidia.gpu.clock.frequency", "MHz", "[N/A]");
        assert_eq!(point.value, None);
        assert_eq!(point.quality, Quality::Unsupported);
    }

    #[test]
    fn parses_gb10_named_query_fixture_without_inventing_memory_clock() {
        let points = parse_smi_csv(include_str!("../tests/fixtures/gb10-nvidia-smi.csv"));
        assert_eq!(points.len(), 6);
        let memory_clock = points
            .iter()
            .find(|point| {
                point.name == "nvidia.gpu.clock.frequency"
                    && point
                        .attributes
                        .get("clock.domain")
                        .is_some_and(|v| v == "memory")
            })
            .unwrap();
        assert!(memory_clock.value.is_none());
        assert_eq!(memory_clock.quality, Quality::Unsupported);
        assert!(
            points
                .iter()
                .all(|point| point.attributes["gpu.id"] == "GPU-GB10-FIXTURE")
        );
    }

    #[test]
    fn detects_only_gb10_inventory_identity() {
        assert!(is_gb10("NVIDIA GB10"));
        assert!(!is_gb10("NVIDIA H100 NVL"));
    }
}
