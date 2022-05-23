pub struct Comment {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    pub permalink: String,
    pub body: String,
    pub sentiment: String,
    pub score: String
}

impl Comment {
    pub fn from_file(s: String) -> Result<Comment, String> {
        let parts: Vec<&str> = s.split(',').collect();

        Ok(Comment{
            id: parts[1].to_string(),
            subreddit_id: parts[2].to_string(),
            subreddit_name: parts[3].to_string(),
            subreddit_nsfw: parts[4].to_string(),
            created_utc: parts[5].to_string(),
            permalink: parts[6].to_string(),
            body: parts[7].to_string(),
            sentiment: parts[8].to_string(),
            score: parts[9].to_string()
        })
    }

    pub fn deserialize(s: String) -> Comment {
        let splited: Vec<&str> = s.split(',').collect();

        Comment {
            id: splited[0].to_string(),
            subreddit_id: splited[1].to_string(),
            subreddit_name: splited[2].to_string(),
            subreddit_nsfw: splited[3].to_string(),
            created_utc: splited[4].to_string(),
            permalink: splited[5].to_string(),
            body: splited[6].to_string(),
            sentiment: splited[7].to_string(),
            score: splited[8].to_string()
        }
    }
}