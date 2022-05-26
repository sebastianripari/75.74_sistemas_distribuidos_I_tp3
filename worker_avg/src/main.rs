use std::{time::Duration, thread};
use serde_json::{Value, json};
use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Publish, Exchange};

// input
const QUEUE_POSTS_TO_AVG: &str = "QUEUE_POSTS_TO_AVG";

// output
const AVG_TO_FILTER_SCORE: &str = "AVG_TO_FILTER_SCORE";
const QUEUE_TO_CLIENT: &str = "QUEUE_TO_CLIENT";

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
    let queue = channel.queue_declare(QUEUE_POSTS_TO_AVG, QueueDeclareOptions::default()).unwrap();
    let consumer = queue.consume(ConsumerOptions::default()).unwrap();
    let exchange = Exchange::direct(&channel);

    loop {
        if stop {
            break;
        }

        let mut score_count: u64 = 0;
        let mut score_sum: u64 = 0;

        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    if body == "stop" {
                        println!("doing stop");
                        stop = true;
                        consumer.ack(delivery).unwrap();
                        break;
                    }

                    if body == "end" {
                        println!("doing end");
                        exchange.publish(Publish::new(
                            json!({
                                "score_avg": score_sum / score_count
                            }).to_string().as_bytes(),
                            AVG_TO_FILTER_SCORE
                        )).unwrap();

                        exchange.publish(Publish::new(
                            json!({
                                "score_avg": score_sum / score_count
                            }).to_string().as_bytes(),
                            QUEUE_TO_CLIENT
                        )).unwrap();

                        consumer.ack(delivery).unwrap();
                        break;
                    }

                    let value: Value = serde_json::from_str(&body).unwrap();
                    println!("processing: {}", value);
                    let score = value["score"].to_string();
                    match score.parse::<i32>() {
                        Ok(score) => {
                            score_count = score_count + 1;
                            score_sum = score_sum + score as u64;
                        }
                        Err(err) => {
                            println!("error: {}", err)
                        }
                    } 
                    
                    consumer.ack(delivery).unwrap();
                }
                _ => {
                    println!("stop consuming");
                }
            }
        }
        println!("stop consuming");
    }

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("shutdown");
}
