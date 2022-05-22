use std::{time::Duration, thread};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Exchange, Publish};

const QUEUE_SCORE: &str = "QUEUE_SCORE";
const QUEUE_SCORE_AVG: &str = "QUEUE_SCORE_AVG";

fn main() {
    println!("worker avg start");

    let mut score_count = 0;
    let mut score_sum = 0;

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
    let exchange = Exchange::direct(&channel);
    let queue_score = channel.queue_declare(QUEUE_SCORE, QueueDeclareOptions::default()).unwrap();
    let consumer = queue_score.consume(ConsumerOptions::default()).unwrap();
    
    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                println!("{} received", body);
                if body == "end_of_posts" {
                    exchange.publish(Publish::new(
                        (score_sum / score_count).to_string().as_bytes(),
                        QUEUE_SCORE_AVG
                    )).unwrap();
                    continue;
                } else {
                    score_count = score_count + 1;
                    score_sum = score_sum + body.parse::<i32>().unwrap();
                }
                consumer.ack(delivery).unwrap();
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker avg shutdown");
}
