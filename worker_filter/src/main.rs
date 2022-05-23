use std::{thread, time::Duration};
use serde_json::{Value};
use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};

const QUEUE_COMMENTS_BODY: &str = "QUEUE_COMMENTS_BODY";

const STUDENTS_WORDS: [&'static str; 5] = [
    "university",
    "college",
    "student",
    "teacher",
    "professor"
];

fn main() {
    println!("worker filter start");

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
    let queue_comments_body = channel.queue_declare(QUEUE_COMMENTS_BODY, QueueDeclareOptions::default()).unwrap();
    let consumer = queue_comments_body.consume(ConsumerOptions::default()).unwrap();

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                let comment: Value = serde_json::from_str(&body).unwrap();

                let comment_id = comment["id"].to_string();
                let body = comment["body"].to_string();

                let mut student_body = false;
                for word in STUDENTS_WORDS {
                    if body.contains(word) {
                        student_body = true;
                        break;
                    }
                }
                
                println!("is a student body?: {}", student_body);

                consumer.ack(delivery).unwrap();
            }
            _ => {}
        }
    }

    println!("worker filter shutdown");
}
