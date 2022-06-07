use std::{net::{IpAddr, Ipv4Addr, TcpStream, SocketAddr}, time::Duration, thread, env};
use utils::logger::logger_create;

use crate::utils::{socket::{SocketWriter}, file::{send_posts_from_file, send_comments_from_file}, logger::Logger};

const PORT_DEFAULT: u16 = 12345;
const FILENAME_POSTS_DEFAULT: &str = "posts.csv";
const FILENAME_COMMENTS_DEFAULT: &str = "comments.csv";

const OPCODE_POST: u8 = 0;
const OPCODE_POST_END: u8 = 1;
const OPCODE_COMMENT: u8 = 2;
const OPCODE_COMMENT_END: u8 = 3;

mod utils;
mod entities;

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut port = PORT_DEFAULT;
    let mut filename_posts = FILENAME_POSTS_DEFAULT.to_string();
    let mut filename_comments = FILENAME_COMMENTS_DEFAULT.to_string();

    if let Ok(p) = env::var("SERVER_PORT") {
        port = p.parse::<u16>().unwrap();
    }

    if let Ok(filename) = env::var("FILENAME_POSTS") {
        filename_posts = filename;
    }

    if let Ok(filename) = env::var("FILENAME_COMMENTS") {
        filename_comments = filename;
    }

    // wait server up
    thread::sleep(Duration::from_secs(32));

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 25, 125, 2)), port);

    let mut writer;

    match TcpStream::connect(&address) {
        Ok(stream) => {
            println!("connected with the server");
            let stream_clone = stream.try_clone().unwrap();
            writer = SocketWriter::new(stream_clone);
        }
        Err(err) => {
            panic!("could not connect {}", err);
        }
    }

    logger.info(format!("filename posts: {}", filename_posts));
    logger.info(format!("filename comments: {}", filename_comments));

    thread::sleep(Duration::from_secs(20));
    

    send_posts_from_file(filename_posts, &mut writer, &logger);
    send_comments_from_file(filename_comments, &mut writer, &logger);

    logger.info("shutdown".to_string());
}
