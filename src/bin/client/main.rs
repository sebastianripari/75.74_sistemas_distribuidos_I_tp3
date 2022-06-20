use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    thread,
    time::Duration, fs::File, io::Write,
};

use reddit_meme_analyzer::commons::{
    constants::{
        filename::{FILENAME_COMMENTS_DEFAULT, FILENAME_POSTS_DEFAULT},
        host::PORT_DEFAULT,
    },
    utils::{
        logger::{logger_create, Logger},
        socket::{SocketReader, SocketWriter}, file::{send_posts_from_file, send_comments_from_file},
    },
};

fn handle_receive(socket_reader: &mut SocketReader, logger: &Logger) {
    let mut best_students_memes_url_received = false;
    let mut posts_score_avg_received = false;
    let mut meme_with_best_sentiment_received = false;

    loop {
        if let Some(key) = socket_reader.receive() {
            if key == "best_students_memes_url" {
                if let Some(value) = socket_reader.receive() {
                    logger.info(format!("response: {}: {}", key, value));
                }
                best_students_memes_url_received = true
            }
            if key == "posts_score_avg" {
                if let Some(value) = socket_reader.receive() {
                    logger.info(format!("response: {}: {}", key, value));
                }
                posts_score_avg_received = true;
            }
            if key == "meme_with_best_sentiment" {
                logger.info("meme_with_best_sentiment".to_string());
                if let Some(filename) = socket_reader.receive() {
                    if let Some(n_str) = socket_reader.receive() {
                        let n = n_str.parse::<usize>().unwrap();
                        if let Some(value) = socket_reader.receive_bytes(n) {
                            let mut file =
                                File::create(format!("./downloads/{}", filename)).unwrap();
                            file.write_all(&value).unwrap();
                        }
                    }
                }
                meme_with_best_sentiment_received = true;
            }

            if best_students_memes_url_received
                && posts_score_avg_received
                && meme_with_best_sentiment_received
            {
                break;
            }
        }
    }
}

fn main() {
    let logger = logger_create();
    logger.info("start".to_string());

    let mut port = PORT_DEFAULT;
    let mut filename_posts = FILENAME_POSTS_DEFAULT.to_string();
    let mut filename_comments = FILENAME_COMMENTS_DEFAULT.to_string();

    if let Ok(p) = env::var("SERVER_PORT") {
        port = p.parse::<u16>().unwrap();
    }

    if let Ok(filename) = env::var("FILENAME_POSTS") {
        filename_posts = filename;
    }

    if let Ok(filename) = env::var("FILENAME_COMMENTS") {
        filename_comments = filename;
    }

    // wait server up
    thread::sleep(Duration::from_secs(32));

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 25, 125, 2)), port);

    let mut socket_reader;
    let mut socket_writer;

    match TcpStream::connect(&address) {
        Ok(stream) => {
            println!("connected with the server");
            let stream_clone = stream.try_clone().unwrap();
            socket_reader = SocketReader::new(stream);
            socket_writer = SocketWriter::new(stream_clone);
        }
        Err(err) => {
            panic!("could not connect {}", err);
        }
    }

    logger.info(format!("filename posts: {}", filename_posts));
    logger.info(format!("filename comments: {}", filename_comments));

    let logger_clone = logger.clone();
    let receiver = thread::spawn(move || handle_receive(&mut socket_reader, &logger_clone));

    send_posts_from_file(filename_posts, &mut socket_writer, &logger);
    send_comments_from_file(filename_comments, &mut socket_writer, &logger);

    receiver.join().unwrap();

    logger.info("shutdown".to_string());
}
