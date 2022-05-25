use std::{thread, time::Duration};

use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, ConsumerMessage, Publish, Exchange};
use serde_json::{Value, json};

use crate::entities::post::Post;

mod entities;

// queue input
const QUEUE_POSTS_TO_JOIN: &str = "QUEUE_POSTS_TO_JOIN";
const QUEUE_COMMENTS_TO_JOIN: &str = "QUEUE_COMMENTS_TO_JOIN";

// queue output
const QUEUE_TO_CLIENT: &str = "QUEUE_TO_CLIENT";


fn main() {
    println!("worker join start");

    let mut stop = false;

    let mut posts = vec![];

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
    let exchange = Exchange::direct(&channel);

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
                    let url = value["url"].to_string();
    
                    let post = Post::new(post_id, url);
    
                    posts.push(post);
    
                    consumer_posts.ack(delivery).unwrap();
                }
                _ => {}
            }

            println!("n posts (score > score_avg) to join: {}", posts.len());

            for message in consumer_comments.receiver().iter() {
                match message {
                    ConsumerMessage::Delivery(delivery) => {
                        let body = String::from_utf8_lossy(&delivery.body);
                        let value: Value = serde_json::from_str(&body).unwrap();
                        println!("processing: {}", value);
                        let post_id = value["post_id"].to_string();
        
                        for post in posts.iter_mut() {
                            if post.id == post_id {
                                println!("match: {}, url: {}", post.id, post.url);
                                exchange.publish(Publish::new(
                                    json!({
                                        "url": post.url
                                    }).to_string().as_bytes(),
                                    QUEUE_TO_CLIENT
                                )).unwrap();
                                break;
                            }
                        }
                        
                        consumer_comments.ack(delivery).unwrap();
                    }
                    _ => {}
                }
            }
        }

        posts.clear()
    }
    

    if let Ok(_) = rabbitmq_connection.close() {
        println!("rabbitmq connection closed")
    }

    println!("worker join shutdown");
}
