use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::{entities::{comment::Comment, post::Post}, OPCODE_POST, OPCODE_POST_END, OPCODE_COMMENT, OPCODE_COMMENT_END};

use super::{socket::SocketWriter, logger::Logger};

const BATCH_SIZE: usize = 100;

pub fn send_posts_from_file(path: String, writter: &mut SocketWriter, logger: &Logger) {
    let mut posts = Vec::new();
    let mut n_post_sent = 0;
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let mut reader = csv::Reader::from_reader(file);
            for line_result in reader.records() {
                if let Ok(line) = line_result {
                    if let Ok(post) = Post::from_file(line) {
                        logger.debug(format!("about to send post: {}", post.id));
                        posts.push(post.serialize());
                        if posts.len() == BATCH_SIZE {
                            writter.send(format!("{}|{}", OPCODE_POST, posts.join("")));
                            n_post_sent = n_post_sent + BATCH_SIZE;
                            logger.info(format!("n post sent: {}", n_post_sent));
                            posts.clear();
                        }
                    } else {
                        logger.debug("bad post".to_string())
                    }
                }
            }
        }
        Err(_) => {
            logger.debug("could not open file".to_string());
        }
    }
    writter.send(format!("{}|{}", OPCODE_POST, posts.join("")));
    logger.info("all sent".to_string());
    writter.send(format!("{}|", OPCODE_POST_END));
}

pub fn send_comments_from_file(path: String, writter: &mut SocketWriter, logger: &Logger) {
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line_result in reader.lines().skip(1) {
                if let Ok(line) = line_result {
                    if let Ok(comment) = Comment::from_file(line) {
                        logger.debug(format!("about to send comment: {}", comment.id));
                        writter.send(format!("{}|{}", OPCODE_COMMENT, comment.serialize()))
                    } else {
                        logger.debug("bad comment".to_string());
                    }
                }
            }
        }
        Err(_) => {
            logger.info("could not open file".to_string());
        }
    }
    writter.send(format!("{}|", OPCODE_COMMENT_END));
}