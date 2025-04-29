//! Query module for building flexible tag-based search queries.

pub trait Dialect {
    fn placeholder(idx: usize) -> String;
    fn exists_tag_query(idx: usize) -> String;
}

#[cfg(feature = "sqlite")]
pub struct SqliteDialect;

#[cfg(feature = "sqlite")]
impl Dialect for SqliteDialect {
    fn placeholder(_idx: usize) -> String {
        "?".to_string()
    }

    fn exists_tag_query(_idx: usize) -> String {
        "EXISTS (SELECT 1 FROM image_tags WHERE image_tags.image_hash = images.hash AND image_tags.tag_name = ?)".to_string()
    }
}

#[cfg(feature = "sqlite")]
pub type CurrentDialect = SqliteDialect;

#[derive(Debug, Clone)]
pub enum QueryExpr {
    /// Represents a single tag condition.
    Tag(String),

    /// Logical AND of two expressions.
    And(Box<QueryExpr>, Box<QueryExpr>),

    /// Logical OR of two expressions.
    Or(Box<QueryExpr>, Box<QueryExpr>),

    // Logical NOT of expressions.
    Not(Box<QueryExpr>),
}

impl QueryExpr {
    /// Creates a single tag query.
    pub fn tag<T: Into<String>>(tag: T) -> Self {
        QueryExpr::Tag(tag.into())
    }

    /// Combines two queries with AND.
    pub fn and(self, other: QueryExpr) -> Self {
        QueryExpr::And(Box::new(self), Box::new(other))
    }

    /// Combines two queries with OR.
    pub fn or(self, other: QueryExpr) -> Self {
        QueryExpr::Or(Box::new(self), Box::new(other))
    }

    pub fn not(expr: QueryExpr) -> Self {
        QueryExpr::Not(Box::new(expr))
    }

    /// Converts the QueryExpr into an SQL WHERE clause and parameters.
    pub fn to_sql(&self) -> (String, Vec<String>) {
        let mut params = Vec::new();
        let sql = self.build_sql(&mut params);
        (sql, params)
    }

    fn build_sql(&self, params: &mut Vec<String>) -> String {
        match self {
            QueryExpr::Tag(tag) => {
                params.push(tag.clone());
                CurrentDialect::exists_tag_query(params.len())
            }
            QueryExpr::And(lhs, rhs) => {
                format!("({} AND {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            QueryExpr::Or(lhs, rhs) => {
                format!("({} OR {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            QueryExpr::Not(query_expr) => {
                format!("NOT {}", query_expr.build_sql(params))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub expr: QueryExpr,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Query {
    pub fn new(expr: QueryExpr) -> Self {
        Self {
            expr,
            limit: None,
            offset: None,
        }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Builds SQL WHERE clause + LIMIT/OFFSET
    pub fn to_sql(&self) -> (String, Vec<String>) {
        let (mut where_sql, mut params) = self.expr.to_sql();

        if let Some(limit) = self.limit {
            params.push(limit.to_string());
            where_sql
                .push_str(format!(" LIMIT {}", CurrentDialect::placeholder(params.len())).as_str());
        }

        if let Some(offset) = self.offset {
            params.push(offset.to_string());
            where_sql.push_str(
                format!(" OFFSET {}", CurrentDialect::placeholder(params.len())).as_str(),
            );
        }

        (where_sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::{CurrentDialect, Dialect, Query, QueryExpr};

    #[test]
    fn test_build_query() {
        let query = Query::new(
            QueryExpr::tag("cat")
                .and(QueryExpr::tag("cute"))
                .or(QueryExpr::not(QueryExpr::tag("dog"))),
        )
        .with_limit(10)
        .with_offset(20);

        let (sql, params) = query.to_sql();

        assert_eq!(
            format!(
                "(({} AND {}) OR NOT {}) LIMIT {} OFFSET {}",
                CurrentDialect::exists_tag_query(1),
                CurrentDialect::exists_tag_query(2),
                CurrentDialect::exists_tag_query(3),
                CurrentDialect::placeholder(4),
                CurrentDialect::placeholder(5)
            ),
            sql
        );
        assert_eq!(vec!["cat", "cute", "dog", "10", "20"], params);
    }
}
