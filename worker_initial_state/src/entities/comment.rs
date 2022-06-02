pub struct Comment {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    pub permalink: String,
    pub body: String,
    pub sentiment: f32,
    pub score: String
}

impl Comment {
    pub fn deserialize_multiple(s: String) -> Vec<Comment> {
        let mut v = Vec::new();
        let mut comments: Vec<&str> = s.split("_c_e_d_").collect();
        comments.pop();
        for comment_str in comments {
            v.push(Comment::deserialize(comment_str.to_string()))
        }
        v
    }

    pub fn deserialize(s: String) -> Comment {
        let splited: Vec<&str> = s.split("_c_f_d_").collect();

        let mut sentiment = 0.0;
        if let Ok(value) = splited[7].to_string().parse::<f32>() {
            sentiment = value;
        }

        Comment {
            id: splited[0].to_string(),
            subreddit_id: splited[1].to_string(),
            subreddit_name: splited[2].to_string(),
            subreddit_nsfw: splited[3].to_string(),
            created_utc: splited[4].to_string(),
            permalink: splited[5].to_string(),
            body: splited[6].to_string(),
            sentiment: sentiment,
            score: splited[8].to_string()
        }
    }
}