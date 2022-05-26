use csv::StringRecord;

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
    pub fn from_file(s: StringRecord) -> Result<Post, String> {
        Ok(Post{
            id: s[1].to_string(),
            subreddit_id: s[2].to_string(),
            subreddit_name: s[3].to_string(),
            subreddit_nsfw: s[4].to_string(),
            created_utc: s[5].to_string(),
            permalink: s[6].to_string(),
            domain: s[7].to_string(),
            url: s[8].to_string().replace('\n', " "),
            selftext: s[9].to_string().replace('\n', " "),
            title: s[10].to_string().replace('\n', " "),
            score: s[11].to_string().parse::<i32>().unwrap(),
        })
    }

    pub fn serialize(&self) -> String {
        // _p_f_d_: post field delimiter
        format!("{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_\n",
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