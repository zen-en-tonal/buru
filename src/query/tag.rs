use crate::dialect::{CurrentDialect, Dialect};

/// Represents a logical expression for querying tags.
#[derive(Debug, Clone, PartialEq)]
pub enum TagQueryExpr {
    /// Matches tags that are exactly equal to the given string.
    Exact(String),

    /// Matches tags that start with the given prefix.
    Prefix(String),

    /// Matches tags that contain the given substring.
    Contains(String),

    /// Logical AND of two expressions.
    And(Box<TagQueryExpr>, Box<TagQueryExpr>),

    /// Logical OR of two expressions.
    Or(Box<TagQueryExpr>, Box<TagQueryExpr>),

    /// Logical NOT of an expression.
    Not(Box<TagQueryExpr>),
}

impl TagQueryExpr {
    /// Combines the current expression with another using a logical AND.
    pub fn and(self, other: TagQueryExpr) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    /// Combines the current expression with another using a logical OR.
    pub fn or(self, other: TagQueryExpr) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    /// Converts the logical expression to an SQL clause and parameters.
    ///
    /// # Returns
    /// - `(String, Vec<String>)`: SQL clause and ordered parameters
    pub fn to_sql(&self) -> (String, Vec<String>) {
        let mut params = Vec::new();
        let sql = self.build_sql(&mut params);
        (sql, params)
    }

    /// Recursively builds the SQL clause for the expression and collects parameters.
    fn build_sql(&self, params: &mut Vec<String>) -> String {
        match self {
            TagQueryExpr::Exact(name) => {
                params.push(name.clone());
                format!("name = {}", CurrentDialect::placeholder(params.len()))
            }
            TagQueryExpr::Prefix(prefix) => {
                params.push(format!("{}%", prefix));
                format!("name LIKE {}", CurrentDialect::placeholder(params.len()))
            }
            TagQueryExpr::Contains(substr) => {
                params.push(format!("%{}%", substr));
                format!("name LIKE {}", CurrentDialect::placeholder(params.len()))
            }
            TagQueryExpr::And(lhs, rhs) => {
                format!("({} AND {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            TagQueryExpr::Or(lhs, rhs) => {
                format!("({} OR {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            TagQueryExpr::Not(expr) => {
                format!("NOT ({})", expr.build_sql(params))
            }
        }
    }
}

/// Represents the kind of query being performed on tags.
#[derive(Debug, Clone)]
pub enum TagQueryKind {
    /// Query all tags without any conditions.
    All,

    /// Query tags that satisfy a certain logical expression.
    Where(TagQueryExpr),
}

impl TagQueryKind {
    /// Converts the query kind into an SQL clause and parameters.
    ///
    /// # Returns
    /// - `(String, Vec<String>)`: SQL clause and ordered parameters
    pub fn to_sql(&self) -> (String, Vec<String>) {
        match self {
            TagQueryKind::All => ("".to_string(), vec![]),
            TagQueryKind::Where(expr) => {
                let (sql, params) = expr.to_sql();
                (format!("WHERE {}", sql), params)
            }
        }
    }
}

/// Represents a complete query, including logical expression and pagination.
#[derive(Debug, Clone)]
pub struct TagQuery {
    /// The logical expression used for filtering.
    pub expr: TagQueryKind,

    /// The maximum number of results to return.
    pub limit: Option<u32>,

    /// The offset into the result set.
    pub offset: Option<u32>,
}

impl TagQuery {
    /// Creates a new query from a query expression.
    pub fn new(expr: TagQueryKind) -> Self {
        Self {
            expr,
            limit: None,
            offset: None,
        }
    }

    /// Sets the `LIMIT` for this query.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the `OFFSET` for this query.
    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Converts the full query into an SQL string and bound parameters.
    ///
    /// # Returns
    /// - `(String, Vec<String>)`: SQL clause and ordered parameters
    ///
    /// The generated SQL includes any specified LIMIT or OFFSET.
    pub fn to_sql(&self) -> (String, Vec<String>) {
        let (mut where_sql, mut params) = self.expr.to_sql();

        if let Some(limit) = self.limit {
            params.push(limit.to_string());
            where_sql.push_str(
                format!(
                    " LIMIT CAST({} AS INTEGER)",
                    CurrentDialect::placeholder(params.len())
                )
                .as_str(),
            );
        }

        if let Some(offset) = self.offset {
            params.push(offset.to_string());
            where_sql.push_str(
                format!(
                    " OFFSET CAST({} AS INTEGER)",
                    CurrentDialect::placeholder(params.len())
                )
                .as_str(),
            );
        }

        (where_sql, params)
    }
}
