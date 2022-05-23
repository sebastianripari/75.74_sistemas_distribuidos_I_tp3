use std::{thread, time::Duration};
use serde_json::{json, Value};
use regex::Regex;
use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};

const QUEUE_COMMENTS: &str = "QUEUE_COMMENTS";

fn main() {
    println!("worker map start");

    thread::sleep(Duration::from_secs(20));

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
    let queue_comments = channel.queue_declare(QUEUE_COMMENTS, QueueDeclareOptions::default()).unwrap();
    let consumer = queue_comments.consume(ConsumerOptions::default()).unwrap();

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);

                let comment: Value = serde_json::from_str(&body).unwrap();
                
                let comment_id = comment["id"].to_string();
                let permalink = comment["permalink"].to_string();
                let regex = Regex::new(r"https://old.reddit.com/r/meirl/comments/([^/]+)/meirl/.*").unwrap();
                let post_id = regex.captures(&permalink).unwrap().get(1).unwrap().as_str();
                
                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }


}
