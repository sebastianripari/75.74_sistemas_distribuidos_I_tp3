use crate::commons::constants::queues::{
    AVG_TO_FILTER_SCORE, QUEUE_COMMENTS_TO_FILTER_STUDENTS, QUEUE_COMMENTS_TO_GROUP_BY,
    QUEUE_COMMENTS_TO_JOIN, QUEUE_COMMENTS_TO_MAP, QUEUE_INITIAL_STATE, QUEUE_POSTS_TO_AVG,
    QUEUE_POSTS_TO_FILTER_SCORE, QUEUE_POSTS_TO_GROUP_BY, QUEUE_POSTS_TO_JOIN, QUEUE_TO_CLIENT,
};

use super::logger::Logger;
use amiquip::{
    Channel, Connection, Consumer, ConsumerMessage, ConsumerOptions, Exchange, Publish,
    QueueDeclareOptions,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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
    n_producers
        .iter()
        .flat_map(|x| x.parse::<usize>())
        .collect()
}

// get the numbers of consumers from ENV
pub fn get_n_consumers() -> Vec<usize> {
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

pub struct MiddlewareConnection {
    connection: Connection,
    channel: Channel,
    logger: Logger,
}

impl MiddlewareConnection {
    pub fn new(logger: &Logger) -> MiddlewareConnection {
        let mut connection = middleware_connect(logger);
        let channel = middleware_create_channel(&mut connection);

        MiddlewareConnection {
            connection,
            channel,
            logger: logger.clone(),
        }
    }

    pub fn get_consumer(&self, queue: &str) -> Consumer {
        let queue = self
            .channel
            .queue_declare(queue, QueueDeclareOptions::default())
            .unwrap();

        queue.consume(ConsumerOptions::default()).unwrap()
    }

    pub fn get_direct_exchange(&self) -> Exchange {
        Exchange::direct(&self.channel)
    }

    // makes a queues declaration
    pub fn declare_queues(&self, queues: Vec<&str>) {
        for queue in queues {
            self.channel
                .queue_declare(queue, QueueDeclareOptions::default())
                .unwrap();
        }
    }

    pub fn close(self) {
        if let Ok(_) = self.connection.close() {
            self.logger.info("connection closed".to_string())
        }
    }
}

pub trait MiddlewareService<T> {
    fn process_message(
        &self,
        payload: &mut T,
        n_processed: &mut usize,
        exchange: &Exchange,
        logger: &Logger,
    );

    fn process_end(&self, exchange: &Exchange);

    fn consume(&self, consumer: &Consumer, exchange: &Exchange, logger: &Logger)
    where
        T: Serialize,
        T: DeserializeOwned,
    {
        let mut end = false;
        let mut n_end = 0;
        let mut n_processed = 0;

        let n = get_n_producers()[0];

        for message in consumer.receiver().iter() {
            if let ConsumerMessage::Delivery(delivery) = message {
                let body = String::from_utf8_lossy(&delivery.body);
                let msg: Message<T> = serde_json::from_str(&body).unwrap();
                let opcode = msg.opcode;
                let payload = msg.payload;

                match opcode {
                    MESSAGE_OPCODE_END => {
                        n_end += 1;

                        if n_end == n {
                            end = true;
                            self.process_end(exchange);
                        }
                    }
                    MESSAGE_OPCODE_NORMAL => {
                        self.process_message(
                            &mut payload.unwrap(),
                            &mut n_processed,
                            exchange,
                            logger,
                        );
                    }
                    _ => {}
                }

                consumer.ack(delivery).unwrap();

                if end {
                    break;
                }
            }
        }
    }
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
pub fn middleware_consumer_end(
    n_end: &mut usize,
    exchange: &Exchange,
    queues: Vec<&str>,
    producer_index: usize,
) -> bool {
    if middleware_end_reached(n_end, producer_index) {
        let consumers = get_n_consumers();

        for (i, queue) in queues.iter().enumerate() {
            for _ in 0..consumers[i] {
                middleware_send_msg_end(exchange, queue);
            }
        }

        return true;
    }

    return false;
}

fn get_env_var(var: &str) -> usize {
    match env::var(var) {
        Ok(value) => value.parse::<usize>().unwrap(),
        Err(_) => 1,
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

    for _ in 0..(n_worker_initial_state * n_worker_map) {
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

    for _ in 0..(n_worker_average * n_worker_filter_score) {
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
