use csv::StringRecord;

#[derive(Debug, Clone)]
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

    pub fn serialize(&self) -> String {
        // _p_f_d_: post field delimiter
        // _p_e_d_: post end
        format!("{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_f_d_{}_p_e_d_",
            self.id,
            self.subreddit_id,
            self.subreddit_name,
            self.subreddit_nsfw,
            self.created_utc,
            self.permalink.replace('\n', " "),
            self.domain.replace('\n', " "),
            self.url.replace('\n', " "),
            self.selftext.replace('\n', " "),
            self.title.replace('\n', " "),
            self.score
        )
    }

    pub fn from_file(s: StringRecord) -> Result<Post, String> {
        Ok(Post{
            id: s[1].to_string(),
            subreddit_id: s[2].to_string(),
            subreddit_name: s[3].to_string(),
            subreddit_nsfw: s[4].to_string(),
            created_utc: s[5].to_string(),
            permalink: s[6].to_string(),
            domain: s[7].to_string(),
            url: s[8].to_string(),
            selftext: s[9].to_string(),
            title: s[10].to_string(),
            score: s[11].to_string().parse::<i32>().unwrap(),
        })
    }

    pub fn deserialize_multiple(s: String) -> Vec<Post> {
        let mut v = Vec::new();
        let mut posts: Vec<&str> = s.split("_p_e_d_").collect();
        posts.pop();
        for post_str in posts {
            v.push(Post::deserialize(post_str.to_string()))
        }
        v
    }

    pub fn deserialize(s: String) -> Post {
        let split: Vec<&str> = s.split("_p_f_d_").collect();

        let mut score = 0;
        match split[10].to_string().parse::<i32>() {
            Ok(value) => {
                score = value;
            }
            Err(err) => {
                panic!("s: {}, err: {}, score: {}", s, err, split[10].to_string());
            }
        }

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
            title: split[9].to_string(),
            score: score
        }
    }
}

