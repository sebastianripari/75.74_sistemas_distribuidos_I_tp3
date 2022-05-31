use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish, QueueDeclareOptions,
};
use serde::Deserialize;
use serde_json::json;
use std::{env, thread, time::Duration};

#[derive(Deserialize, Debug)]
struct Msg {
    permalink: String,
}

// queue input
const QUEUE_COMMENTS_TO_FILTER_STUDENTS: &str = "QUEUE_COMMENTS_TO_FILTER_STUDENTS";
// queue output
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

const STUDENTS_WORDS: [&'static str; 5] =
    ["university", "college", "student", "teacher", "professor"];

fn main() {
    println!("start");

    // wait rabbit
    thread::sleep(Duration::from_secs(30));

    let rabbitmq_user;
    match env::var("RABBITMQ_USER") {
        Ok(value) => rabbitmq_user = value,
        Err(_) => {
            panic!("could not get rabbitmq user from env")
        }
    }

    let rabbitmq_password;
    match env::var("RABBITMQ_PASSWORD") {
        Ok(value) => rabbitmq_password = value,
        Err(_) => {
            panic!("could not get rabbitmq password user from env")
        }
    }

    let mut rabbitmq_connection;
    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            println!("connected with rabbitmq");
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel
        .queue_declare(
            QUEUE_COMMENTS_TO_FILTER_STUDENTS,
            QueueDeclareOptions::default(),
        )
        .unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    break;
                }

                let value: Msg = serde_json::from_str(&body).unwrap();
                println!("processing: {:?}", value);
                let permalink = value.permalink;

                for word in STUDENTS_WORDS {
                    if body.contains(word) {
                        exchange
                            .publish(Publish::new(
                                json!({ "permalink": permalink }).to_string().as_bytes(),
                                QUEUE_COMMENTS_TO_MAP,
                            ))
                            .unwrap();
                        break;
                    }
                }

                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker filter shutdown");
}
