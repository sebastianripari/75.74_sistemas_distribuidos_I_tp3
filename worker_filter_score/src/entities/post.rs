pub struct Post {
    pub id: String,
    pub score: i32,
    pub url: String
}

impl Post {
    pub fn new(id: String, score: i32, url: String) -> Post {
        Post {
            id: id,
            score: score,
            url: url
        }
    }
}