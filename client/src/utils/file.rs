use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::{entities::{comment::Comment, post::Post}, OPCODE_POST, OPCODE_POST_END, OPCODE_COMMENT, OPCODE_COMMENT_END};

use super::{socket::SocketWriter, logger::Logger};

const BATCH_POSTS_SIZE: usize = 100;
const BATCH_COMMENTS_SIZE: usize = 100;

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
                        if posts.len() == BATCH_POSTS_SIZE {
                            if let Err(_) = writter.send(format!("{}|{}", OPCODE_POST, posts.join(""))) {
                                logger.debug("send error".to_string());
                                return
                            }
                            n_post_sent = n_post_sent + BATCH_POSTS_SIZE;
                            posts.clear();
                            if n_post_sent % 100000 == 0 {
                                logger.info(format!("n post sent: {}", n_post_sent));
                            }
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
    if let Err(_) = writter.send(format!("{}|{}", OPCODE_POST, posts.join(""))) {
        logger.debug("send error".to_string());
        return
    }
    if let Err(_) = writter.send(format!("{}|", OPCODE_POST_END)) {
        logger.debug("send error".to_string());
    }
    logger.info("all sent".to_string());
}

pub fn send_comments_from_file(path: String, writter: &mut SocketWriter, logger: &Logger) {
    let mut comments = Vec::new();
    let mut n_comment_sent = 0;
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let mut reader = csv::Reader::from_reader(file);
            for line_result in reader.records() {
                if let Ok(line) = line_result {
                    if let Ok(comment) = Comment::from_file(line) {
                        logger.debug(format!("about to send comment: {}", comment.id));
                        comments.push(comment.serialize());
                        if comments.len() == BATCH_COMMENTS_SIZE {
                            if let Err(_) = writter.send(format!("{}|{}", OPCODE_COMMENT, comments.join(""))) {
                                logger.debug("send error".to_string());
                                break;
                            }
                            n_comment_sent = n_comment_sent + BATCH_COMMENTS_SIZE;
                            comments.clear();
                            if n_comment_sent % 100000 == 0 {
                                logger.info(format!("n comment sent: {}", n_comment_sent));
                            }
                        }
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
    if let Err(_) = writter.send(format!("{}|{}", OPCODE_COMMENT, comments.join(""))) {
        logger.debug("send error".to_string());
        return
    }
    if let Err(_) = writter.send(format!("{}|", OPCODE_COMMENT_END)) {
        logger.debug("send error".to_string());
    }
    logger.info("all sent".to_string());
}