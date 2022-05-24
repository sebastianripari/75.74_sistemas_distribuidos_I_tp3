use std::{thread, time::Duration};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage};
use serde_json::Value;

use crate::entities::post::Post;

mod entities;

const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

fn main() {
    println!("worker join start");

    let mut stop = false;

    let mut posts = vec![];

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

    let queue_posts_to_join = channel.queue_declare(QUEUE_POSTS_TO_JOIN, QueueDeclareOptions::default()).unwrap();
    let queue_comments_to_join = channel.queue_declare(QUEUE_COMMENTS_TO_JOIN, QueueDeclareOptions::default()).unwrap();

    let consumer_posts = queue_posts_to_join.consume(ConsumerOptions::default()).unwrap();
    let consumer_comments = queue_comments_to_join.consume(ConsumerOptions::default()).unwrap();

    loop {

        if stop {
            break;
        }

        for message in consumer_posts.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    if body == "stop" {
                        stop = true;
                        break;
                    }

                    if body == "end" {
                        consumer_posts.ack(delivery).unwrap();
                        break;
                    }

                    let value: Value = serde_json::from_str(&body).unwrap();
    
                    let post_id = value["post_id"].to_string();
                    let score = value["score"].to_string().parse::<i32>().unwrap();
                    let url = value["url"].to_string();
    
                    let post = Post::new(post_id, score, url);
    
                    posts.push(post);
    
                    consumer_posts.ack(delivery).unwrap();
                }
                _ => {}
            }

            println!("===== Result: 2 =====");
            for message in consumer_comments.receiver().iter() {
                match message {
                    ConsumerMessage::Delivery(delivery) => {
                        let body = String::from_utf8_lossy(&delivery.body);
                        let value: Value = serde_json::from_str(&body).unwrap();
        
                        let post_id = value["post_id"].to_string();
        
                        for post in posts.iter_mut() {
                            if post.id == post_id {
                                println!("{:?}", post);
                                break;
                            }
                        }
                        
                        consumer_comments.ack(delivery).unwrap();
                    }
                    _ => {}
                }
            }
            println!("===== =====")
        }

        posts.clear()
    }
    

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
