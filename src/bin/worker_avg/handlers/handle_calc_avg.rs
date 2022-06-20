use reddit_meme_analyzer::commons::utils::logger::{Logger, LOG_RATE};

pub fn handle_calc_avg(
    scores: Vec<i32>,
    n: &mut usize,
    logger: &Logger,
    count: &mut usize,
    sum: &mut u64,
) {
    *n += scores.len();

    for score in scores {
        logger.debug(format!("processing: {}", score));
        *count += 1;
        *sum += score as u64;
    }

    if *n % LOG_RATE == 0 {
        logger.info(format!("n processed: {}", n));
    }
}