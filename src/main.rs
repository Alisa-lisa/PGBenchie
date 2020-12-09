use std::env;
mod utils;  // make other module available in this space
use utils::*;  // "globe" operator = import *
use sqlx::postgres::PgPoolOptions;
use anyhow::Result;  // simplified Error handling, so that we do not have to specify EXACT error type for each result returning function
use rand::thread_rng;
use rand::seq::SliceRandom;
use futures::StreamExt;
use structopt::StructOpt;


static CHUNK_SIZE: usize = 1000;  // buffered stream chunking
static TOTAL_USERS: i32 = 25000;  // how many unique users to create in users table and later draw from 
static TOTAL_USER_ATTRIBUTES_ROWS: i32 = 500000;  
static TOTAL_ATTRIBUTES: i32 = 500;  // 5000;
static TARGET_GROUP_SIZE: i64 = 200;
static NEW_ATTRIBUTES: i32 = 50;
static NEW_ATTRIBUTE_ANSWERS_ROWS: i32 = 200;

// CLI parser
#[derive(Debug, StructOpt)]
enum Cli {
    /// Create Schema
    Create,
    /// Populate tables in simple configuration
    Parents,
    /// Populate Child
    Child,
    /// Execute target group selection
    Target,
    /// Scan query
    Scan,
    /// Upsert query
    Upsert,
    /// Create, Insert new attributes
    Createandupdate

}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let cli = Cli::from_args();
    // reusable things params
    let initial_attributes = utils::create_attributes(TOTAL_ATTRIBUTES);
    let new_attributes= utils::create_attributes(NEW_ATTRIBUTES);
    let mut rng = thread_rng();
    let upsert_combinations = utils::create_combinations(TOTAL_USER_ATTRIBUTES_ROWS, 1, TOTAL_USERS, 1, TOTAL_ATTRIBUTES);

    // connection 
    let pool = PgPoolOptions::new()
        .max_connections(10000)
        .connect(&env::var("DATABASE_URL")?).await?;
    match cli {
        Cli::Create => {
            // setup
            utils::setup_database(&pool).await?;
            // println!("Setup feddig");
        },
        Cli::Parents => {
            // initial populate parents
            utils::populate_parents(TOTAL_USERS, CHUNK_SIZE, &initial_attributes, &pool).await?;
            // println!("Populate parents feddig");
        },
        Cli::Child => {
            // initial populate child
            utils::populate_child(1, TOTAL_USERS, 1, TOTAL_ATTRIBUTES, TOTAL_USER_ATTRIBUTES_ROWS, CHUNK_SIZE, &pool).await?;
            // println!("Populate child feddig");
        },
        Cli::Target => {
            // collect targets single query; choose some attributes for target
            let target_attribute: &String = &initial_attributes.choose(&mut rng).unwrap();
            sqlx::query_file!("queries/get_target_group.sql", target_attribute, TARGET_GROUP_SIZE).fetch_all(&pool).await?;  // we do not return, but could
            // println!("Group collection feddig");
        },
        Cli::Scan => {
            // get all expired attributes and ppl with these expired attributes -> db scan
            sqlx::query_file!("queries/get_expired_attributes.sql").fetch_all(&pool).await?;  // we do not return but could
            // println!("DB scan scenario feddig");
        },
        Cli::Upsert => {
             // inert/update (upsert) user attributes
            let mut execution_vec = vec![];
            for fk in upsert_combinations {
                execution_vec.push(sqlx::query_file!("queries/upsert_attribute.sql", &fk.0, &fk.1).execute(&pool));
            };
            // this part pf code is coming quite often, but kinda hard to get specific ErrType of
            // FutureUnordered struct -> i don't bother in a prototype
            let buffered = futures::stream::iter(execution_vec).buffer_unordered(CHUNK_SIZE);
            buffered.for_each(|b| async {
                match b {
                    Ok(_b) => (),
                    Err(e) => eprintln!("nope to upserts: {}", e),
                }
            }).await; 
            // println!("Upsert scenario feddig");
        },
        Cli::Createandupdate => {
            // add new attributes and connected userattributes information -> 2 consequent queries
            let mut execution_vec = vec![];
            for name in new_attributes {
                execution_vec.push(sqlx::query_file!("queries/9_populate_second_parent.sql", &name).execute(&pool));
            };
            let buffered = futures::stream::iter(execution_vec).buffer_unordered(CHUNK_SIZE);
            buffered.for_each(|b| async {
                match b {
                    Ok(_b) => (),
                    Err(e) => eprintln!("nope to upserts: {}", e),
                }
            }).await; 
            populate_child(1, TOTAL_USERS, 
                           TOTAL_ATTRIBUTES, TOTAL_ATTRIBUTES+NEW_ATTRIBUTES,
                           NEW_ATTRIBUTE_ANSWERS_ROWS, CHUNK_SIZE, &pool).await?;
            // println!("Adding new attributes and answers feddig");
        }
    };

    pool.close().await;
    Ok(())
}
