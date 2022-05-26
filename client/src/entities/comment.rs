use csv::StringRecord;

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
    pub fn from_file(s: StringRecord) -> Result<Comment, String> {
        Ok(Comment{
            id: s[1].to_string(),
            subreddit_id: s[2].to_string(),
            subreddit_name: s[3].to_string(),
            subreddit_nsfw: s[4].to_string(),
            created_utc: s[5].to_string(),
            permalink: s[6].to_string(),
            body: s[7].to_string(),
            sentiment: s[8].to_string(),
            score: s[9].to_string()
        })
    }

    pub fn serialize(&self) -> String {
        // _c_f_d_: comment field delimiter
        // _c_e_d_: comment field delimiter
        format!("{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_f_d_{}_c_e_d_",
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