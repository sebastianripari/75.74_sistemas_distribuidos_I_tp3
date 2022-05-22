use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::{entities::{comment::Comment, post::Post}, OPCODE_POST, OPCODE_POST_END, OPCODE_COMMENT, OPCODE_COMMENT_END};

use super::socket::SocketWriter;

pub fn send_posts_from_file(path: String, writter: &mut SocketWriter) {
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line_result in reader.lines().skip(1) {
                if let Ok(line) = line_result {
                    if let Ok(post) = Post::from_file(line) {
                        println!("{} sent", post.id);
                        writter.send(format!("{}|{}", OPCODE_POST, post.serialize()))
                    } else {
                        println!("bad post")
                    }
                }
            }
        }
        Err(_) => {
            println!("could not open file");
        }
    }
    writter.send(format!("{}|\n", OPCODE_POST_END));
}

pub fn send_comments_from_file(path: String, writter: &mut SocketWriter) {
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line_result in reader.lines().skip(1) {
                if let Ok(line) = line_result {
                    if let Ok(comment) = Comment::from_file(line) {
                        println!("{} sent", comment.id);
                        writter.send(format!("{}|{}", OPCODE_COMMENT, comment.serialize()))
                    } else {
                        println!("bad comment")
                    }
                }
            }
        }
        Err(_) => {
            println!("could not open file");
        }
    }
    writter.send(format!("{}|\n", OPCODE_COMMENT_END));
}