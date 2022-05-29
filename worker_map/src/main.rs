use std::{thread, time::Duration};
use serde_json::{Value, json};
use regex::Regex;
use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, Publish, Exchange};

// queue input
const QUEUE_COMMENTS_TO_MAP: &str = "QUEUE_COMMENTS_TO_MAP";

// queue output
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

const COMMENT_PERMALINK_REGEX: &str = r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*";

fn main() {
    println!("start");

    let mut stop = false;

    thread::sleep(Duration::from_secs(30));

    let mut rabbitmq_connection;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
        Ok(connection) => {
            println!("connected with rabbitmq");
            rabbitmq_connection = connection;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_connection.open_channel(None).unwrap();
    let queue = channel.queue_declare(QUEUE_COMMENTS_TO_MAP, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                if body == "stop" {
                    break;
                }

                let value: Value = serde_json::from_str(&body).unwrap();
                println!("processing: {}", value);
                let permalink = value["permalink"].to_string();

                let regex = Regex::new(COMMENT_PERMALINK_REGEX).unwrap();
                let post_id = regex.captures(&permalink).unwrap().get(1).unwrap().as_str();
                
                println!("post id found {}", post_id);

                exchange.publish(Publish::new(
                    json!({
                        "post_id": post_id
                    }).to_string().as_bytes(),
                    QUEUE_COMMENTS_TO_JOIN
                )).unwrap();

                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker map shutdown");
}
