use sqlx::PgPool;

#[allow(clippy::module_name_repetitions)]
pub struct ApiContext {
    pub db: PgPool,
}

impl ApiContext {
    #[must_use]
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}
