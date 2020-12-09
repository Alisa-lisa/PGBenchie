use anyhow::Result;
use sqlx::{Pool, Postgres};
use rand::seq::IteratorRandom;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use futures::StreamExt;
use itertools::Itertools;
use std::iter::repeat;


pub async fn setup_database(pool: &Pool<Postgres>) -> Result<()> {
    // execute "setup sql statement" in order; non-parametric
    sqlx::query_file!("queries/1_parent_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/2_parent_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/3_second_parent_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/4_second_parent_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/5_child_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/6_child_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/7_child_index.sql").execute(pool).await?;
    Ok(())  // return positive result -> result is an enum or Ok and Err state. It will throw an Err if smth is wrong, but default return is Ok
}

pub async fn populate_parents(total_users: i32, chunk: usize, attribute_names: &Vec<String>, pool: &Pool<Postgres>) -> Result<()> {
    // populate "parent" tables. We could use threads here to speed up population, but it is not
    // really needed for production and test is doing fine with simple optimizations
    let mut execution_vec = vec![];  // mut = mutability -> allowed changes, & -> refernce since just calling an object will "move it out" and make unreachable in previous place for other uses
    for _ in 0..total_users {
        execution_vec.push(sqlx::query_file!("queries/8_populate_first_parent.sql").execute(pool));
    };
    for name in attribute_names {
        execution_vec.push(sqlx::query_file!("queries/9_populate_second_parent.sql", &name).execute(pool));
    }
    let stream_of_futures = futures::stream::iter(execution_vec);  // to execute futures concurrently streams are used. It is similar to task queue in async python
    let buffered = stream_of_futures.buffer_unordered(chunk);  // chunk is the breaking point of execution -> futures are attempted to be started in batches 
    // start each chunk of futures
    buffered.for_each(|b| async {
        match b {  // future can return Ok or Err of certain type, we need to actually check it explicitly 
            Ok(_b) => (),
            Err(e) => eprintln!("nope to users: {}", e),
        }
    }).await;

    Ok(())  // rerurn "Positive result"
}

pub fn create_combinations(total: i32, first_range_start: i32, first_range_limit: i32, 
                           second_range_start: i32, second_range_limit: i32) -> Vec<(i32, i32)> {
    // create all possible pairs of user-attribute ids and sample randomly
    let mut rng = thread_rng();  
    let full_vec = (first_range_start..first_range_limit).cartesian_product(second_range_start..second_range_limit);
    full_vec.choose_multiple(&mut rng, total as usize)  // return a Vec of tuples of i32;i32
}

pub async fn populate_child(first_range_start: i32, first_range_limit: i32,
                            second_range_start: i32, second_range_limit: i32,
                            total_rows: i32, chunk: usize, pool: &Pool<Postgres>) -> Result<()> {
    // child in our case has 2 parameters: usrid, attribute id. Everything else will eb generated
    let combinations = create_combinations(total_rows, first_range_start, first_range_limit, second_range_start, second_range_limit);
    let mut execution_vec = vec![];
    for fk in combinations {
        execution_vec.push(sqlx::query_file!("queries/10_populate_child.sql", &fk.0, &fk.1).execute(pool));  // we need to refernce here sinec tuple belongs to combinations and it is not possible dynamically to get "partial" ownership
    }
    let buffered = futures::stream::iter(execution_vec).buffer_unordered(chunk); 
    buffered.for_each(|b| async {
        match b {
            Ok(_b) => (),
            Err(e) => eprintln!("nope to attributes: {}", e),
        }
    }).await;

    Ok(())
}

pub fn create_attributes(number: i32) -> Vec<String> {
    // create random strings from Alphabet [Aa-Zz] of size 10 chars
    let mut attributes: Vec<String> = Vec::new();
    for _ in 0..number {
        let mut rng = thread_rng();
        attributes.push(repeat(()).map(|()| rng.sample(Alphanumeric)).take(10).collect());
    };
    attributes
}

