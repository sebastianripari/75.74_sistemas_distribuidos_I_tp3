pub struct Comment {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    permalink: String,
    body: String,
    sentiment: String,
    score: String
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

    pub fn serialize(&self) -> String {
        format!("{},{},{},{},{},{},{},{},{},\n",
            self.id,
            self.subreddit_id,
            self.subreddit_name,
            self.subreddit_nsfw,
            self.created_utc,
            self.permalink,
            self.body,
            self.sentiment,
            self.score
        )
    }
}