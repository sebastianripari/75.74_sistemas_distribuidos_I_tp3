use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::entities::post::Post;

use super::socket::SocketWriter;

pub fn send_posts_from_file(path: String, writter: &mut SocketWriter) {
    if let Ok(file) = OpenOptions::new().read(true).open(path) {
        let reader = BufReader::new(file);
        for (i, line) in reader.lines().skip(1).enumerate() {
            if let Ok(l) = line {
                if let Ok(post) = Post::from_file(l) {
                    println!("post readed {}", i);
                    writter.send(post.serialize())
                }
            }
        }
    }
    writter.send("end_of_posts\n".to_string());
}