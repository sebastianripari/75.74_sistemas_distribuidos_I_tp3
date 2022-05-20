use std::{io::{BufReader, BufRead}, fs::OpenOptions};

use crate::entities::post::Post;

pub fn read_file_posts(path: String) -> Vec<Post> {
    let mut lines = vec![];
    if let Ok(file) = OpenOptions::new().read(true).open(path) {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(l) = line {
                if let Ok(post) = Post::from_file(l) {
                    lines.push(post);
                }
            }
        }
    }
    lines
}