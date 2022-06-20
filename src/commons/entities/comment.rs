use csv::StringRecord;

pub struct Comment {
    pub id: String,
    subreddit_id: String,
    subreddit_name: String,
    subreddit_nsfw: String,
    created_utc: String,
    pub permalink: String,
    pub body: String,
    pub sentiment: f32,
    score: String
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

        Comment {
            id: splited[0].to_string(),
            subreddit_id: splited[1].to_string(),
            subreddit_name: splited[2].to_string(),
            subreddit_nsfw: splited[3].to_string(),
            created_utc: splited[4].to_string(),
            permalink: splited[5].to_string(),
            body: splited[6].to_string(),
            sentiment: splited[7].to_string().parse::<f32>().unwrap(),
            score: splited[8].to_string()
        }
    }

    pub fn from_file(s: StringRecord) -> Result<Comment, String> {

        let sentiment_str = s[8].to_string();
        let sentiment;
        match sentiment_str.parse::<f32>() {
            Ok(value) => {
                sentiment = value;
            }
            Err(_) => {
                return Err("sentiment invalid".to_string());
            }
        }

        Ok(Comment{
            id: s[1].to_string(),
            subreddit_id: s[2].to_string(),
            subreddit_name: s[3].to_string(),
            subreddit_nsfw: s[4].to_string(),
            created_utc: s[5].to_string(),
            permalink: s[6].to_string(),
            body: s[7].to_string(),
            sentiment,
            score: s[9].to_string()
        })
    }

    pub fn serialize(&self) -> String {
        // _c_f_d_: comment field delimiter
        // _c_e_d_: comment end delimiter
        format!("{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_e_d_",
            self.id.replace('\n', " "),
            self.subreddit_id.replace('\n', " "),
            self.subreddit_name.replace('\n', " "),
            self.subreddit_nsfw.replace('\n', " "),
            self.created_utc.replace('\n', " "),
            self.permalink.replace('\n', " "),
            self.body.replace('\n', " "),
            self.sentiment,
            self.score.replace('\n', " ")
        )
    }
}