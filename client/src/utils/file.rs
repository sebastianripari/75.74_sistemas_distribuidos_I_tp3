use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::entities::post::Post;

use super::socket::SocketWriter;

pub fn send_posts_from_file(path: String, writter: &mut SocketWriter) {
    match  OpenOptions::new().read(true).open(path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line_result in reader.lines().skip(1) {
                if let Ok(line) = line_result {
                    if let Ok(post) = Post::from_file(line) {
                        println!("{} sent", post.id);
                        writter.send(post.serialize())
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
    writter.send("end_of_posts\n".to_string());
}