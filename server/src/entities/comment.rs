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
    pub fn deserialize(s: String) -> Comment {
        let splited: Vec<&str> = s.split("_c_f_d_").collect();

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