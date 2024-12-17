use {
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    },
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigTokio {
    /// Number of worker threads in Tokio runtime
    pub worker_threads: Option<usize>,
    /// Threads affinity
    #[serde(deserialize_with = "ConfigTokio::deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

impl ConfigTokio {
    fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Option<Vec<usize>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<&str>::deserialize(deserializer)? {
            Some(taskset) => parse_taskset(taskset).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

pub fn parse_taskset(taskset: &str) -> Result<Vec<usize>, String> {
    let mut set = HashSet::new();
    for taskset2 in taskset.split(',') {
        match taskset2.split_once('-') {
            Some((start, end)) => {
                let start: usize = start
                    .parse()
                    .map_err(|_error| format!("failed to parse {start:?} from {taskset:?}"))?;
                let end: usize = end
                    .parse()
                    .map_err(|_error| format!("failed to parse {end:?} from {taskset:?}"))?;
                if start > end {
                    return Err(format!("invalid interval {taskset2:?} in {taskset:?}"));
                }
                for idx in start..=end {
                    set.insert(idx);
                }
            }
            None => {
                set.insert(
                    taskset2.parse().map_err(|_error| {
                        format!("failed to parse {taskset2:?} from {taskset:?}")
                    })?,
                );
            }
        }
    }

    let mut vec = set.into_iter().collect::<Vec<usize>>();
    vec.sort();

    if let Some(set_max_index) = vec.last().copied() {
        let max_index = affinity::get_thread_affinity()
            .map_err(|_err| "failed to get affinity".to_owned())?
            .into_iter()
            .max()
            .unwrap_or(0);

        if set_max_index > max_index {
            return Err(format!("core index must be in the range [0, {max_index}]"));
        }
    }

    Ok(vec)
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigPrometheus {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10123),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueIntStr<'a> {
    Int(usize),
    Str(&'a str),
}

pub fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    match ValueIntStr::deserialize(deserializer)? {
        ValueIntStr::Int(value) => Ok(value),
        ValueIntStr::Str(value) => value
            .replace('_', "")
            .parse::<usize>()
            .map_err(de::Error::custom),
    }
}
