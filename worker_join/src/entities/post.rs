#[derive(Debug)]
pub struct Post {
    pub id: String,
    pub url: String,
    pub score: i32
}

impl Post {
    pub fn new(id: String, score: i32, url: String) -> Post {
        Post {
            id: id,
            score: score,
            url: url,
        }
    }
}

