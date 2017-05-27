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
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
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

    fn add_type_of(&mut self, v: Value) -> bool {
        match v {
            Value::Null => {
                let changes = ! self.null;
                self.null = true;
                changes
            },
            Value::Bool(_) => {
                let changes = ! self.bool;
                self.bool = true;
                changes
            },
            Value::Number(_) => {
                let changes = ! self.number;
                self.number = true;
                changes
            },
            Value::String(_) => {
                let changes = ! self.string;
                self.string = true;
                changes
            },
            Value::Array(a) => {
                let mut types = self.array.take().unwrap_or_else(Default::default);

                let changed = if a.is_empty() {
                    let changes = ! types.absent;
                    types.absent = true;
                    changes
                } else {
                    a.into_iter().map(|v| {
                        types.add_type_of(v)
                    }).any(id)
                };

                self.array = Some(types);

                changed
            },
            Value::Object(m) => {
                let mut map = self.object.take().unwrap_or_else(Map::new);

                let changed = map.iter_mut().map(|(k, types)| {
                    if ! m.contains_key(k) {
                        let changes = ! types.absent;
                        types.absent = true;
                        changes
                    } else {
                        false
                    }
                }).any(id);

                let changed = changed || m.into_iter().map(|(k, v)| {
                    map.entry(k).or_insert_with(TypeSet::new).add_type_of(v)
                }).any(id);

                self.object = Some(map);

                changed
            },
        }
    }

    fn write_to_file(&self, mut f: &mut BufWriter<File>) -> io::Result<()> {
        f.seek(SeekFrom::Start(0))?;
        f.get_ref().set_len(0)?;
        json::to_writer_pretty(&mut f, self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        f.flush()
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

    let output = matches.value_of("output")
        .or_else(|| matches.value_of("resume"))
        .unwrap_or("types.json");
    let mut output = OpenOptions::new()
            .write(true)
            .create(true)
            .open(output)
            .map(BufWriter::new).unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    {
        let fut = TwitterStream::sample(&token, &handle).flatten_stream().for_each(|json| {
            let json = json::from_str(&json).map_err(twitter_stream::Error::custom)?;
            let changed = root.add_type_of(json);

            if changed {
                print!("writing to the output file... ");;
                root.write_to_file(&mut output).unwrap();
                println!("ok");
            }

            Ok(())
        });

        // Catch Ctrl-C to ensure graceful shutdown.
        let ctrl_c = tokio_signal::ctrl_c(&handle).flatten_stream().into_future()
            .map_err(|(e, _)| twitter_stream::Error::custom(e));

        core.run(fut.select2(ctrl_c).map_err(|e| e.split().0)).unwrap();
    }

    root.write_to_file(&mut output).unwrap();
}

fn id<T>(t: T) -> T {
    t
}
