use std::{time::Duration, thread};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};

const QUEUE_SCORE: &str = "score";

fn main() {
    println!("worker avg start");

    let mut count = 0;
    let mut sum = 0;

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
    let queue = channel.queue_declare(QUEUE_SCORE, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                println!("{} received", body);
                if body == "end_of_posts" {
                    // tengo que pushear un elemento en otra cola para que le llegue
                    // al proceso server
                    continue;
                } else{
                    count = count + 1;
                    sum = sum + body.parse::<i32>().unwrap();
                }
                consumer.ack(delivery).unwrap();
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }
}
