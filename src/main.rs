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
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use tokio_core::reactor::Core;
use twitter_stream::TwitterStream;

#[derive(Deserialize, Serialize)]
struct Node {
    kind: HashSet<Type>,
    children: BTreeMap<String, Node>,
}

#[derive(Eq, PartialEq, Hash, Deserialize, Serialize)]
enum Type {
    Absent,
    Null,
    Bool,
    Number,
    String,
    Array(Box<Type>),
    Object,
}

impl Node {
    fn new() -> Self {
        Node {
            kind: HashSet::new(),
            children: BTreeMap::new(),
        }
    }

    fn merge(&mut self, v: Value) {
        self.kind.insert(Type::of(&v));

        if let Value::Object(m) = v {
            for (k, n) in &mut self.children {
                if ! m.contains_key(k) {
                    n.kind.insert(Type::Absent);
                }
            }

            for (k, v) in m {
                self.children.entry(k).or_insert_with(Node::new).merge(v);
            }
        }
    }
}

impl Type {
    fn of(v: &Value) -> Self {
        match *v {
            Value::Null => Type::Null,
            Value::Bool(_) => Type::Bool,
            Value::Number(_) => Type::Number,
            Value::String(_) => Type::String,
            Value::Array(ref a) => a.first().map_or_else(
                || Type::Array(Box::new(Type::Null)),
                |v| Type::Array(Box::new(Type::of(v)))
            ),
            Value::Object(_) => Type::Object,
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
        .unwrap_or_else(Node::new);

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    {
        let fut = TwitterStream::sample(&token, &handle).flatten_stream().for_each(|json| {
            let json = json::from_str(&json).map_err(twitter_stream::Error::custom)?;
            root.merge(json);
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
