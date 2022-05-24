#[derive(Debug)]
pub struct Post {
    pub id: String,
    pub url: String
}

impl Post {
    pub fn new(id: String, url: String) -> Post {
        Post {
            id: id,
            url: url,
        }
    }
}

