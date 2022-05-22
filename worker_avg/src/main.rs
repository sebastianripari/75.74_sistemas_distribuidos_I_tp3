use std::{time::Duration, thread};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};

const ROUTING_KEY: &str = "hello";

fn main() {
    println!("worker avg start");

    thread::sleep(Duration::from_secs(20));

    let mut rabbitmq_conn;
    match Connection::insecure_open("amqp://root:seba1234@rabbitmq:5672") {
        Ok(c) => {
            println!("connected with rabbitmq");
            rabbitmq_conn = c;
        }
        Err(_) => {
            panic!("could not connect with rabbitmq")
        }
    }

    let channel = rabbitmq_conn.open_channel(None).unwrap();
    let queue = channel.queue_declare(ROUTING_KEY, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    
    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                println!("({:>3}) Received [{}]", i, body);
                consumer.ack(delivery).unwrap();
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }
}
