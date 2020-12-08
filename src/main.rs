use anyhow::Result;
use sqlx::{Pool, postgres::PgPoolOptions, Postgres};
use rand::seq::{IteratorRandom};
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use futures::{stream::FuturesUnordered, future::{join_all, try_join_all}, StreamExt, TryStreamExt, TryFutureExt};
use itertools::Itertools;
use std::env;
use std::iter::repeat;

// fn get_queries(directory: String) -> Vec<String> {
//     // return sorted query filed from a directory
//     let path  = Path::new(&directory);
//     let mut queries = vec![];
//     for entry in path.read_dir().expect("can't read dirrectory") {
//         if let Ok(entry) = entry {
//             queries.push(entry.path().to_str().unwrap().to_owned());
//         }
//     };
//     queries.sort();
//     queries
// }
//

async fn setup_database(pool: &Pool<Postgres>) -> Result<()> {
    // TODO: how can i loop over all sql statements and execute them by providig a dirtectory?

    sqlx::query_file!("queries/1_parent_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/2_parent_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/3_second_parent_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/4_second_parent_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/5_child_clean.sql").execute(pool).await?;
    sqlx::query_file!("queries/6_child_creation.sql").execute(pool).await?;
    sqlx::query_file!("queries/7_child_index.sql").execute(pool).await?;
    Ok(())
}


async fn populate_parents(total_users: i32, chunk: usize, attribute_names: &Vec<String>, pool: &Pool<Postgres>) -> Result<()> {
    // populate parents can be done simulataneoiusly
    let mut execution_vec = vec![];
    for _ in 0..total_users {
        execution_vec.push(sqlx::query_file!("queries/8_populate_first_parent.sql").execute(pool));
    };
    for name in attribute_names {
        execution_vec.push(sqlx::query_file!("queries/9_populate_second_parent.sql", &name).execute(pool));
    }
    let stream_of_futures = futures::stream::iter(execution_vec);
    let buffered = stream_of_futures.buffer_unordered(chunk);

    buffered.for_each(|b| async {
        match b {
            Ok(_b) => (),
            Err(e) => eprintln!("nope to users: {}", e),
        }
    }).await;

    Ok(())
}

async fn populate_child(total_rows: i32, chunk: usize, attributes: &Vec<String>, pool: &Pool<Postgres>) -> Result<()> {
    // TODO: get ids -> 
    let full_vec = (1..TOTAL_USERS).cartesian_product(1..TOTAL_ATTRIBUTES);
    let mut rng = thread_rng();
    let combinations: Vec<(i32, i32)> = full_vec.choose_multiple(&mut rng, total_rows as usize);

    let mut execution_vec = vec![];
    for fk in combinations {
        execution_vec.push(sqlx::query_file!("queries/10_populate_child.sql", &fk.0, &fk.1).execute(pool));
    }
    let future_attribute_stream = futures::stream::iter(execution_vec);
    let buffered = future_attribute_stream.buffer_unordered(chunk);
    buffered.for_each(|b| async {
        match b {
            Ok(_b) => (),
            Err(e) => eprintln!("nope to attributes: {}", e),
        }
    }).await;

    Ok(())
}


static CHUNK_SIZE: usize = 1000;  // 1000;
static TOTAL_USERS: i32 = 25000000;  // 2500000;
static TOTAL_USER_ATTRIBUTES: i32 = 5000000;  // 200000000;
static TOTAL_ATTRIBUTES: i32 = 5000;  // 5000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();


    // Create a connection pool
    let pool = PgPoolOptions::new()
        .max_connections(10000)
        .connect(&env::var("DATABASE_URL")?).await?;
    // dbg!(&pool);

    setup_database(&pool).await?;
    println!("Created schema!");

    let mut attributes = Vec::new();
    for _ in 0..TOTAL_ATTRIBUTES {
        let mut rng = thread_rng();
        attributes.push(repeat(()).map(|()| rng.sample(Alphanumeric)).take(10).collect());
    }
    populate_parents(TOTAL_USERS, CHUNK_SIZE, &attributes, &pool).await?;
    pool.close().await;
    println!("Populated parents!");
    let pool2 = PgPoolOptions::new()
        .max_connections(1000)
        .connect(&env::var("DATABASE_URL")?).await?;

    populate_child(TOTAL_USER_ATTRIBUTES, CHUNK_SIZE, &attributes, &pool2).await?;
    pool2.close().await;
    println!("Populated child");
    Ok(())
}
