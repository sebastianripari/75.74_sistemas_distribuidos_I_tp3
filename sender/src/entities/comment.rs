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
            id: s[1].to_string().replace('\n', " "),
            subreddit_id: s[2].to_string().replace('\n', " "),
            subreddit_name: s[3].to_string().replace('\n', " "),
            subreddit_nsfw: s[4].to_string().replace('\n', " "),
            created_utc: s[5].to_string().replace('\n', " "),
            permalink: s[6].to_string().replace('\n', " "),
            body: s[7].to_string().replace('\n', " "),
            sentiment: s[8].to_string().replace('\n', " "),
            score: s[9].to_string().replace('\n', " ")
        })
    }

    pub fn serialize(&self) -> String {
        // _c_f_d_: comment field delimiter
        // _c_e_d_: comment end delimiter
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