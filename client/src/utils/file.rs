use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::{entities::{comment::Comment, post::Post}, OPCODE_POST, OPCODE_POST_END, OPCODE_COMMENT, OPCODE_COMMENT_END};

use super::socket::SocketWriter;

pub fn send_posts_from_file(path: String, writter: &mut SocketWriter) {
    let mut posts = Vec::new();
    match OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let mut reader = csv::Reader::from_reader(file);
            for line_result in reader.records() {
                if let Ok(line) = line_result {
                    if let Ok(post) = Post::from_file(line) {
                        posts.push(post.serialize());
                        if posts.len() == 10 {
                            println!("sent posts");
                            writter.send(format!("{}|{}\n", OPCODE_POST, posts.join("")));
                            posts.clear();
                        }
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
    writter.send(format!("{}|{}\n", OPCODE_POST, posts.join("")));
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