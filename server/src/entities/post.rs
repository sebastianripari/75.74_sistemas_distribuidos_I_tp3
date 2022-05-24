#[derive(Debug)]
pub struct Post {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    permalink: String,
    domain: String,
    pub url: String,
    selftext: String,
    title: String,
    pub score: i32
}

impl Post {
    pub fn deserialize(s: String) -> Post {
        let parts: Vec<&str> = s.split(',').collect();

        Post {
            id: parts[0].to_string(),
            subreddit_id: parts[1].to_string(),
            subreddit_name: parts[2].to_string(),
            subreddit_nsfw: parts[3].to_string(),
            created_utc: parts[4].to_string(),
            permalink: parts[5].to_string(),
            domain: parts[6].to_string(),
            url: parts[7].to_string(),
            selftext: parts[8].to_string(),
            title: parts[9].to_string(),
            score: parts[10].to_string().parse::<i32>().unwrap()
        }
    }
}

