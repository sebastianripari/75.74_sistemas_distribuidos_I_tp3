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
        let split: Vec<&str> = s.split(',').collect();

        Post {
            id: split[0].to_string(),
            subreddit_id: split[1].to_string(),
            subreddit_name: split[2].to_string(),
            subreddit_nsfw: split[3].to_string(),
            created_utc: split[4].to_string(),
            permalink: split[5].to_string(),
            domain: split[6].to_string(),
            url: split[7].to_string(),
            selftext: split[8].to_string(),
            title: split[8].to_string(),
            score: split[10].to_string().parse::<i32>().unwrap()
        }
    }
}

