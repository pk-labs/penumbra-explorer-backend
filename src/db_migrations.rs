use anyhow::{Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::{info, warn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Define queryable struct for EXISTS query
#[derive(QueryableByName)]
struct Exists {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}

/// Run database migrations to set up the schema
///
/// # Errors
/// Returns an error if database connection fails, if tables can't be checked,
/// or if migrations can't be executed successfully
pub fn run_migrations(database_url: &str) -> Result<()> {
    info!("Connecting to database");
    let mut conn =
        PgConnection::establish(database_url).context("Failed to connect to database")?;

    info!("Running database migrations");

    let table_exists = diesel::sql_query(
        "
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '__diesel_schema_migrations'
        ) as exists
    ",
    )
        .get_result::<Exists>(&mut conn)?
        .exists;

    if table_exists {
        info!("Migration tracking table exists, using existing migration state");
    } else {
        info!("Migration tracking table doesn't exist, using Diesel's migration system");
    }

    match conn.run_pending_migrations(MIGRATIONS) {
        Ok(applied) => {
            info!(
                "Successfully applied {} migrations: {:?}",
                applied.len(),
                applied
            );
        }
        Err(e) => {
            warn!("Failed to run migrations: {:?}", e);
            return Err(anyhow::anyhow!("Migration error: {:?}", e));
        }
    }

    Ok(())
}
