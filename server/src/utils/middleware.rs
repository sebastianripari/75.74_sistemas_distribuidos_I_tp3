use super::logger::Logger;
use amiquip::{
    Channel, Connection, Consumer, ConsumerOptions, Exchange, Publish, Queue, QueueDeclareOptions,
};
use serde::{Deserialize, Serialize};
use std::{env, thread, time::Duration};

/*
    ** Middleware **

    Encapsule the communication with other process,

    Makes use of RabbitMQ.
*/

// get RabbitMQ user from ENV
fn get_rabbitmq_user() -> String {
    match env::var("RABBITMQ_USER") {
        Ok(value) => value,
        Err(_) => {
            panic!("could not get rabbitmq user from env")
        }
    }
}

// get RabbitMQ password from ENV
fn get_rabbitmq_password() -> String {
    match env::var("RABBITMQ_PASSWORD") {
        Ok(value) => value,
        Err(_) => {
            panic!("could not get rabbitmq password from env")
        }
    }
}

// get the numbers of producers from ENV
fn get_n_producers() -> usize {
    let mut n_producers = 1;
    if let Ok(value) = env::var("N_PRODUCERS") {
        n_producers = value.parse::<usize>().unwrap();
    }
    n_producers
}

// get the numbers of consumers from ENV
fn get_n_consumers() -> Vec<usize> {
    let mut value = "1".to_string();
    if let Ok(v) = env::var("N_CONSUMERS") {
        value = v;
    }
    let n_consumers: Vec<&str>;
    n_consumers = value.split(',').collect();
    n_consumers
        .iter()
        .flat_map(|x| x.parse::<usize>())
        .collect()
}

// makes the connection with RabbitMQ
// iterations: try and wait each one seconds, because RabbitMQ takes a bit to wake up
pub fn middleware_connect(logger: &Logger) -> Connection {
    loop {
        match middleware_connect_(&logger) {
            Ok(value) => {
                return value;
            }
            Err(_) => {
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn middleware_connect_(logger: &Logger) -> Result<Connection, ()> {
    let rabbitmq_user = get_rabbitmq_user();
    let rabbitmq_password = get_rabbitmq_password();

    match Connection::insecure_open(
        &format!(
            "amqp://{}:{}@rabbitmq:5672",
            rabbitmq_user, rabbitmq_password
        )
        .to_owned(),
    ) {
        Ok(connection) => {
            logger.info("connected with rabbitmq".to_string());
            return Ok(connection);
        }
        Err(_) => {
            logger.debug("could not connect with rabbitmq".to_string());
            return Err(());
        }
    }
}

// makes a channel to send and receive messages
pub fn middleware_create_channel(connection: &mut Connection) -> Channel {
    connection.open_channel(None).unwrap()
}

// makes a queue declaration
pub fn middleware_declare_queue<'a>(channel: &'a Channel, queue_name: &'a str) -> Queue<'a> {
    channel
        .queue_declare(queue_name, QueueDeclareOptions::default())
        .unwrap()
}

// makes queue consumer
pub fn middleware_create_consumer<'a>(queue: &'a Queue) -> Consumer<'a> {
    queue.consume(ConsumerOptions::default()).unwrap()
}

// makes exchange
pub fn middleware_create_exchange(channel: &Channel) -> Exchange {
    Exchange::direct(&channel)
}

pub const MESSAGE_OPCODE_END: u8 = 0;
pub const MESSAGE_OPCODE_NORMAL: u8 = 1;

#[derive(Serialize, Deserialize)]
pub struct Message<T: Serialize> {
    pub opcode: u8,
    pub payload: Option<T>,
}

// send message end to other process, pushing to a queue
pub fn middleware_send_msg_end(exchange: &Exchange, queue_name: &str) {
    let message = Message::<()> {
        opcode: MESSAGE_OPCODE_END,
        payload: None,
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&message).unwrap().as_bytes(),
            queue_name,
        ))
        .unwrap();
}

// send message to other process, pushing to a queue
pub fn middleware_send_msg<T: Serialize>(exchange: &Exchange, payload: &T, queue_name: &str) {
    let message = Message {
        opcode: MESSAGE_OPCODE_NORMAL,
        payload: Some(payload),
    };

    exchange
        .publish(Publish::new(
            serde_json::to_string(&message).unwrap().as_bytes(),
            queue_name,
        ))
        .unwrap();
}

pub fn middleware_send_msg_all_consumers<T: Serialize>(
    exchange: &Exchange,
    payload: &T,
    queues: Vec<&str>,
) {
    let consumers = get_n_consumers();

    for n in consumers {
        for queue in queues.iter() {
            for _ in 0..n {
                middleware_send_msg(exchange, payload, queue);
            }
        }
    }
}

// detect if the producer finished
fn middleware_end_reached(n_end: &mut usize) -> bool {
    *n_end += 1;

    let n_producers = get_n_producers();

    *n_end == n_producers
}

// send end to the consumer
pub fn middleware_consumer_end(n_end: &mut usize, exchange: &Exchange, queues: Vec<&str>) -> bool {
    if middleware_end_reached(n_end) {
        let consumers = get_n_consumers();

        for n in consumers {
            for queue in queues.iter() {
                for _ in 0..n {
                    middleware_send_msg_end(exchange, queue);
                }
            }
        }

        return true;
    }

    return false;
}
