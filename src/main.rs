use anyhow::Result;
use std::env;
use sqlx::{Pool, postgres::PgPoolOptions, Postgres};
use std::fs::read_to_string;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use futures::{stream::FuturesUnordered, future::{join_all, try_join_all}, StreamExt, TryStreamExt, TryFutureExt};

async fn setup_database(pool: &Pool<Postgres>) -> Result<()> {
    // According to postgres Docs: ROP DATABASE cannot be executed inside a transaction block.
    // This command cannot be executed while connected to the target database. 
    // Thus, it might be more convenient to use the program dropdb instead, which is a wrapper around this command.
    // existing db should be dropped and recreated manually as well as set in .env
    // 1. create tables (not optimized yet)
    sqlx::query!("DROP TABLE IF EXISTS users CASCADE ").execute(pool).await?;
    // let parent_table = read_to_string("queries/parent_table.sql");  // TODO: how to put this query into string literal????
    sqlx::query!("CREATE TABLE IF NOT EXISTS users (
            id integer PRIMARY KEY,
            name varchar(120) NOT NULL,
            age integer NOT NULL,
            gender varchar(10),
            city varchar(120),
            primary_activity varchar(120),
            status varchar(20),
            registration date,
            last_active date,
            tmp1 integer,
            tmp2 bool
            )")
        .execute(pool)
        .await?;    
    sqlx::query!("CREATE INDEX IF NOT EXISTS user_activity on users (registration, last_active) INCLUDE (tmp1)")
        .execute(pool)
        .await?;
    sqlx::query!("DROP TABLE IF EXISTS attributes").execute(pool).await?;
    sqlx::query!("CREATE TABLE IF NOT EXISTS attributes (
            usrid int,
            FOREIGN KEY(usrid) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE,
            last_update date,
            attribute_category varchar(10),
            attribute_key varchar(60),
            attribute_value varchar(60)
            )")
        .execute(pool)
        .await?;
    sqlx::query!("CREATE INDEX IF NOT EXISTS usr on attributes (usrid)")
        .execute(pool)
        .await?;
    // pool.close().await;  TODO: should i close connection here? i.e. restart?
    Ok(())
}

static TOTAL_USERS: i32 = 2000000;
static CHUNK_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // Create a connection pool
    let pool = PgPoolOptions::new()
        .max_connections(1000)
        .connect(&env::var("DATABASE_URL")?).await?;
    // dbg!(&pool);

    setup_database(&pool).await?;
    
    let mut user_insertions = vec![];
    for id in 0..TOTAL_USERS {
        user_insertions.push(sqlx::query!("INSERT INTO users VALUES ($1, 's', 26, 'o', 'NC', 'S', 'S', '2020-12-04', '2020-12-04', 230, true)", id).execute(&pool));
    };
    
    let stream_of_futures = futures::stream::iter(user_insertions);
    let buffered = stream_of_futures.buffer_unordered(CHUNK_SIZE as usize);

    buffered.for_each(|b| async {
        match b {
            Ok(_b) => (),
            Err(e) => eprintln!("nope: {}", e),
        }
    }).await;

    Ok(())
}
