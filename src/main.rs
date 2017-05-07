extern crate clap;
extern crate futures;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json as json;
extern crate tokio_core;
extern crate tokio_signal;
extern crate twitter_stream;

use clap::{App, Arg};
use futures::{Future, Stream};
use json::Value;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::ops::Not;
use tokio_core::reactor::Core;
use twitter_stream::TwitterStream;

#[derive(Default, Deserialize, Serialize)]
struct TypeSet {
    #[serde(default)]
    #[serde(skip_serializing_if = "Not::not")]
    absent: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Not::not")]
    null: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Not::not")]
    bool: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Not::not")]
    number: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Not::not")]
    string: bool,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    array: Option<Box<TypeSet>>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    object: Option<Map>,
}

type Map = BTreeMap<String, TypeSet>;

impl TypeSet {
    fn new() -> Self {
        TypeSet::default()
    }

    fn add_type_of(&mut self, v: Value) {
        match v {
            Value::Null => self.null = true,
            Value::Bool(_) => self.bool = true,
            Value::Number(_) => self.number = true,
            Value::String(_) => self.string = true,
            Value::Array(a) => {
                let mut types = self.array.take().unwrap_or_else(Default::default);

                if a.is_empty() {
                    types.absent = true;
                } else {
                    for v in a {
                        types.add_type_of(v);
                    }
                }

                self.array = Some(types);
            },
            Value::Object(m) => {
                let mut map = self.object.take().unwrap_or_else(Map::new);

                for (k, types) in &mut map {
                    if ! m.contains_key(k) {
                        types.absent = true;
                    }
                }

                for (k, v) in m {
                    map.entry(k).or_insert_with(TypeSet::new).add_type_of(v);
                }

                self.object = Some(map);
            },
        }
    }
}

fn main() {
    let matches = App::new("types")
        .arg(Arg::with_name("credential")
             .short("c")
             .long("credential")
             .default_value("credential.json"))
        .arg(Arg::with_name("resume")
             .short("r")
             .long("resume")
             .takes_value(true))
        .arg(Arg::with_name("output")
             .short("o")
             .long("output")
             .takes_value(true))
        .get_matches();

    let token = File::open(matches.value_of("credential").unwrap())
        .map(BufReader::new).unwrap();
    let token = json::from_reader(token).unwrap();

    let mut root = matches.value_of("resume")
        .and_then(|f| File::open(f).ok())
        .and_then(|f| json::from_reader(f).ok())
        .unwrap_or_else(TypeSet::new);

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    {
        let fut = TwitterStream::sample(&token, &handle).flatten_stream().for_each(|json| {
            let json = json::from_str(&json).map_err(twitter_stream::Error::custom)?;
            root.add_type_of(json);
            Ok(())
        });

        let ctrl_c = tokio_signal::ctrl_c(&handle).flatten_stream().into_future();

        assert!(core.run(fut.select2(ctrl_c)).is_ok());
    }

    let f = matches.value_of("output")
        .or_else(|| matches.value_of("resume"))
        .unwrap_or("types.json");
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(f)
        .map(BufWriter::new).unwrap();

    json::to_writer_pretty(f, &root).unwrap();
}
