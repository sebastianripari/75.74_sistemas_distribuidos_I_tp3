#[derive(Debug)]
pub struct Post {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    permalink: String,
    domain: String,
    url: String,
    selftext: String,
    title: String,
    score: i32
}

impl Post {
    pub fn from_file(s: String) -> Result<Post, String> {
        let parts: Vec<&str> = s.split(',').collect();

        if parts.len() != 12 {
            return Err("bad row".to_string())
        }

        Ok(Post{
            id: parts[1].to_string(),
            subreddit_id: parts[2].to_string(),
            subreddit_name: parts[3].to_string(),
            subreddit_nsfw: parts[4].to_string(),
            created_utc: parts[5].to_string(),
            permalink: parts[6].to_string(),
            domain: parts[7].to_string(),
            url: parts[8].to_string(),
            selftext: parts[9].to_string(),
            title: parts[10].to_string(),
            score: parts[11].to_string().parse::<i32>().unwrap()
        })
    }

    pub fn serialize(&self) -> String {
        format!("{},{},{},{},{},{},{},{},{},{},{},\n",
            self.id,
            self.subreddit_id,
            self.subreddit_name,
            self.subreddit_nsfw,
            self.created_utc,
            self.permalink,
            self.domain,
            self.url,
            self.selftext,
            self.title,
            self.score
        )
    }
}