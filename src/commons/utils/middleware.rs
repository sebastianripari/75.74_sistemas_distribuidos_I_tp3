use crate::commons::constants::queues::{QUEUE_TO_CLIENT, QUEUE_POSTS_TO_JOIN, QUEUE_POSTS_TO_GROUP_BY, QUEUE_POSTS_TO_AVG, QUEUE_COMMENTS_TO_JOIN, QUEUE_COMMENTS_TO_GROUP_BY, AVG_TO_FILTER_SCORE, QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_INITIAL_STATE, QUEUE_COMMENTS_TO_MAP};

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
fn get_n_producers() -> Vec<usize> {
    let mut value = "1".to_string();
    if let Ok(v) = env::var("N_PRODUCERS") {
        value = v;
    }
    let n_producers: Vec<&str>;
    n_producers = value.split(',').collect();
    n_producers.iter().flat_map(|x| x.parse::<usize>()).collect()
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
pub fn middleware_end_reached(n_end: &mut usize, producer_index: usize) -> bool {
    *n_end += 1;

    let n_producers = get_n_producers();

    n_producers[producer_index] == *n_end
}

// send end to the consumer
pub fn middleware_consumer_end(n_end: &mut usize, exchange: &Exchange, queues: Vec<&str>, producer_index: usize) -> bool {
    
    if middleware_end_reached(n_end, producer_index) {
        let consumers = get_n_consumers();

        for (i, n) in consumers.iter().enumerate() {
            for _ in 0..*n {
                middleware_send_msg_end(exchange, queues[i]);
            }
        }

        return true;
    }

    return false;
}

fn get_env_var(var: &str) -> usize {
    match env::var(var) {
        Ok(value) => value.parse::<usize>().unwrap(),
        Err(_) => 1
    }
}

pub fn middleware_stop_all_consumers(exchange: &Exchange) {
    let n_worker_initial_state = get_env_var("N_WORKER_INITIAL_STATE");
    let n_worker_filter_students = get_env_var("N_WORKER_FILTER_STUDENTS");
    let n_worker_map = get_env_var("N_WORKER_MAP");
    let n_server = 1;
    let n_worker_filter_score = 1;
    let n_worker_average = 1;
    let n_worker_group_by = 1;
    let n_worker_join = 1;

    // producer * consumer = numbers of ends needed

    for _ in 0..(n_worker_initial_state * n_worker_map)  {
        middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_MAP);
    }

    for _ in 0..(n_server * n_worker_initial_state) {
        middleware_send_msg_end(&exchange, QUEUE_INITIAL_STATE);
    }

    for _ in 0..(n_worker_initial_state * n_worker_filter_students) {
        middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_FILTER_STUDENTS);
    }

    for _ in 0..(n_worker_initial_state * n_worker_filter_score) {
        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_FILTER_SCORE);
    }

    for _ in 0..(n_worker_average * n_worker_filter_score){
        middleware_send_msg_end(&exchange, AVG_TO_FILTER_SCORE);
    }

    for _ in 0..(n_worker_map * n_worker_group_by) {
        middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_GROUP_BY);
    }

    for _ in 0..(n_worker_filter_students * n_worker_join) {
        middleware_send_msg_end(&exchange, QUEUE_COMMENTS_TO_JOIN);
    }

    for _ in 0..(n_worker_initial_state * n_worker_average) {
        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_AVG);
    }

    for _ in 0..(n_worker_initial_state * n_worker_group_by) {
        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_GROUP_BY);
    }

    for _ in 0..(n_worker_filter_score * n_worker_join) {
        middleware_send_msg_end(&exchange, QUEUE_POSTS_TO_JOIN);
    }

    middleware_send_msg_end(&exchange, QUEUE_TO_CLIENT);
}
