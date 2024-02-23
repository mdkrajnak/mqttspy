//
//
// It has slowly been evolved to have a more formal CLI.
//! This started with the following Paho MQTT Rust client: paho-mqtt/examples/sync_consume.rs
//! It has slowly been evolved to have a more formal CLI.
//!
//! This application is an MQTT consumer/subscriber using the Rust
//! synchronous client interface, which uses the queuing API to
//! receive messages.
//!
//! The program:
//!   - Connects to an MQTT server/broker
//!   - Checks server responses
//!   - Subscribes to zero or more topics
//!   - Receiving messages through the queueing consumer API
//!   - Checking payloads for JSON format and pretty printing them.
//!   - Manual reconnects
//!   - Using a persistent (non-clean) session
//!   - Last will and testament
//!   - Using ^C handler for a clean exit
//!

use paho_mqtt as mqtt;
use std::{thread, time::Duration};
use std::error::Error;
use clap::{Arg, ArgMatches, Command};
use serde_json;
use ctrlc;
use paho_mqtt::{ConnectResponse, ServerResponse};
use serde_json::Value;

// Options
#[derive(Debug)]
struct Options {
    host: String,
    port: u32,
    //certificate: Option<String>,
    client_id: String,
    topics: Vec<String>,
}

impl Options {
    fn default() -> Self {
        Options {
            host: String::from("127.0.0.1"),
            port: 1883,
            //certificate: None,
            client_id: String::from("mqttspy"),
            topics: vec!(String::from("test")),
        }
    }

    fn from_args(args: &ArgMatches) -> Self {
        let defaults = Options::default();

        let host : &String = args.get_one("host").unwrap_or(&defaults.host);
        let port : &u32 = args.get_one("port").unwrap_or(&defaults.port);
        let client_id : &String = args.get_one("clientid").unwrap_or(&defaults.client_id);
        let topics : Vec<String> = match args.get_many("topics") {
            Some(values) => values.map(|x : &String| x.clone()).collect(),
            _ => defaults.topics,
        };

        Options {
            host: host.clone(),
            port: *port,
            //certificate: None,
            client_id: client_id.clone(),
            topics: topics,
        }
    }
}

// This will attempt to reconnect to the broker. It can be called after
// connection is lost. In this example, we try to reconnect several times,
// with a few second pause between each attempt. A real system might keep
// trying indefinitely, with a backoff,
// or something like that.

fn try_reconnect(cli: &mqtt::Client) -> bool {
    println!("Connection lost. Reconnecting...");
    for _ in 0..60 {
        thread::sleep(Duration::from_secs(1));
        if cli.reconnect().is_ok() {
            println!("  Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}

// Try to determine if the payload is JSON.
fn is_json(payload: &[u8]) -> bool {
    // Check for leading '[' or '{' for simple JSON detection
    match payload.get(0) {
        Some(b'[') | Some(b'{') => true,
        _ => false,
    }
}

// Convert a JSON payload to a pretty-printed string.
fn format_json(payload: &[u8]) -> Result<String, Box<dyn Error>> {
    let text = std::str::from_utf8(payload)?;
    let json: Value = serde_json::from_str(text)?;
    Ok(serde_json::to_string_pretty(&json)?)
}

// Handle an inbound message.
fn on_message(message: &mqtt::Message) {
    println!("Message received on topic: {}", message.topic());

    let payload = message.payload();
    if is_json(payload) {
        match format_json(payload) {
            Ok(formatted_json) => println!("{}", formatted_json),
            Err(e) => println!("Error parsing JSON: {}", e),
        }
    }
}

// Create client with the specified host and ID.
fn mk_client(host: &str, client_id: &str) -> Result<mqtt::Client, Box<dyn Error>> {
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id(client_id)
        .finalize();

    let client = mqtt::Client::new(create_opts)?;
    Ok(client)
}

// Handle the connect response.
fn handle_connect_response(client : &mqtt::Client, conn_rsp: &ConnectResponse, subscriptions: &Vec<String>) -> Result<(), Box<dyn Error>> {
    println!(
        "Connected to: '{}' with MQTT version {}",
        conn_rsp.server_uri, conn_rsp.mqtt_version
    );
    // The server doesn't have a persistent session already
    // stored for us (1st connection?), so we need to subscribe
    // to the topics we want to receive.
    let qos = vec![0; subscriptions.len()];
    println!("Subscribing to topics {:?}...", subscriptions);

    client.subscribe_many(&subscriptions, &qos)
        .and_then(|rsp| {
            rsp.subscribe_many_response()
                .ok_or(mqtt::Error::General("Bad response"))
        })
        .map(|vqos| {
            println!("QoS granted: {:?}", vqos);
        })?;

    Ok(())
}

fn connect(client: &mqtt::Client) -> Result<ServerResponse, Box<dyn Error>> {
    // Define the set of options for the connection
    let lwt = mqtt::MessageBuilder::new()
        .topic("status")
        .payload("")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .will_message(lwt)
        .finalize();

    Ok(client.connect(conn_opts)?)
}

fn mqttspy(opt: &Options) -> Result<(), Box<dyn Error>> {
    let host = format!("mqtt://{}:{}", opt.host, opt.port);
    println!("Connecting to the MQTT broker at '{}'...", host);

    // Initialize the client and the consumer, then connect.
    let client = mk_client(&host, &opt.client_id)?;
    let rx = client.start_consuming();
    let server_rsp = connect(&client)?;

    // Complete the connection.
    if let Some(connect_rsp) = server_rsp.connect_response() {
        handle_connect_response(&client, &connect_rsp, &opt.topics)?;
    }
    else {
        return Err(Box::try_from(mqtt::Error::General("No connect response"))?);
    }

    // ^C handler will stop the consumer, breaking us out of the loop, below
    let control_c_cli = client.clone();
    ctrlc::set_handler(move || {
        control_c_cli.stop_consuming();
    })?;

    // Just loop on incoming messages.
    // If we get a None message, check if we got disconnected,
    // and then try a reconnect.
    println!("\nWaiting for messages on topics {:?}...", opt.topics);
    for msg in rx.iter() {
        if let Some(msg) = msg {
            on_message(&msg);
        }
        else if client.is_connected() || !try_reconnect(&client) {
            break;
        }
    }

    // If we're still connected, then disconnect now,
    // otherwise we're already disconnected.
    if client.is_connected() {
        client.disconnect(None)?;
    }

    Ok(())
}

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let args = Command::new("mqttspy")
        .version("0.0.1")
        .about("MQTT Spy")
        .arg(Arg::new("host")
            .long("host")
            .short('H')
            .help("The broker host name or IP address.")
            .default_value("localhost"))
        .arg(Arg::new("port")
            .long("port")
            .short('p')
            .value_parser(clap::value_parser!(u32))
            .help("The broker port.")
            .default_value("1883"))
        .arg(Arg::new("clientid")
            .long("clientid")
            .short('i')
            .help("The client id.")
            .default_value("mqttspy"))
        .arg(Arg::new("topics")
            .required(true)
            .help("The list of topics")
            .num_args(1..))
        .get_matches();

    let opt = Options::from_args(&args);
    mqttspy(&opt).unwrap();
}