#[derive(Debug)]
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
        println!("split: {:?}", split);

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

