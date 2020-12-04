use anyhow::Result;
use std::env;
use sqlx::postgres::PgPoolOptions;
use std::fs::read_to_string;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;


#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // Create a connection pool
    let pool = PgPoolOptions::new()
        .max_connections(100)
        .connect(&env::var("DATABASE_URL")?).await?;
    // dbg!(&pool);

    // According to postgres Docs: ROP DATABASE cannot be executed inside a transaction block.
    // This command cannot be executed while connected to the target database. 
    // Thus, it might be more convenient to use the program dropdb instead, which is a wrapper around this command.
    // existing db should be dropped and recreated manually as well as set in .env
    // 1. create tables (not optimized yet)
    sqlx::query!("DROP TABLE IF EXISTS users CASCADE ").execute(&pool).await?;
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
        .execute(&pool)
        .await?;    
    sqlx::query!("CREATE INDEX IF NOT EXISTS user_activity on users (registration, last_active) INCLUDE (tmp1)")
        .execute(&pool)
        .await?;
    sqlx::query!("DROP TABLE IF EXISTS attributes").execute(&pool).await?;
    sqlx::query!("CREATE TABLE IF NOT EXISTS attributes (
            usrid int,
            FOREIGN KEY(usrid) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE,
            last_update date,
            attribute_category varchar(10),
            attribute_key varchar(60),
            attribute_value varchar(60)
            )")
        .execute(&pool)
        .await?;
    // pool.close().await;  TODO: should i close connection here? i.e. restart?

    
    // TODO: how to start jobs in parallel?
    let mut populate_users = Vec::new();
    let mut userids = Vec::new();
    // make initial population as a sql batch opertion
    for id in 1..101 {
        userids.push(id);
        populate_users
            .push(sqlx::query!("INSERT INTO users VALUES ($1, 'some_name', 26, 'Other', 'NewCity', 'Some', 'Single', '2020-12-04', '2020-12-04', 230, true)", id));
    }
    for task in populate_users {
        task.execute(&pool).await?;
    }
    let mut populate_attributes = Vec::new();
    
    for _ in 1..1001 {
        let mut rngesus = thread_rng();
        let cat: String = std::iter::repeat(()).map(|()| rngesus.sample(Alphanumeric)).take(10).collect();
        populate_attributes.push(sqlx::query!("INSERT INTO attributes VALUES ($1, '2020-12-04', $2, $3, $4)", 
                                              userids.choose(&mut rngesus), 
                                              &cat, 
                                              &cat, 
                                              &cat));
    }
    for task in populate_attributes {
        task.execute(&pool).await?;
    }
   // on top fo populated  

    let record = sqlx::query!(r#"select 1 as "id""#)
        .fetch_one(&pool).await?;

    dbg!(record);

    Ok(())
}
