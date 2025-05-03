//! Query module for building flexible tag-based search queries.
//!
//! This module allows you to construct logical tag expressions (`AND`, `OR`, `NOT`)
//! and convert them into database-agnostic SQL queries. The SQL dialect can be swapped
//! at compile time using Cargo feature flags (e.g., `sqlite`, `postgres`).

use crate::dialect::{CurrentDialect, Dialect};
use chrono::{DateTime, Utc};

/// Represents a logical tag-based query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    /// A single tag condition.
    Tag(String),

    /// Logical AND of two subexpressions.
    And(Box<QueryExpr>, Box<QueryExpr>),

    /// Logical OR of two subexpressions.
    Or(Box<QueryExpr>, Box<QueryExpr>),

    /// Logical NOT of a subexpression.
    Not(Box<QueryExpr>),

    DateUntil(DateTime<Utc>),

    DateSince(DateTime<Utc>),
}

impl QueryExpr {
    /// Creates a query expression from a single tag.
    pub fn tag<T: Into<String>>(tag: T) -> Self {
        QueryExpr::Tag(tag.into())
    }

    /// Combines two expressions with a logical AND.
    pub fn and(self, other: QueryExpr) -> Self {
        QueryExpr::And(Box::new(self), Box::new(other))
    }

    /// Combines two expressions with a logical OR.
    pub fn or(self, other: QueryExpr) -> Self {
        QueryExpr::Or(Box::new(self), Box::new(other))
    }

    /// Negates a query expression.
    pub fn not(expr: QueryExpr) -> Self {
        QueryExpr::Not(Box::new(expr))
    }

    pub fn date_until(date: DateTime<Utc>) -> Self {
        QueryExpr::DateUntil(date)
    }

    pub fn date_since(date: DateTime<Utc>) -> Self {
        QueryExpr::DateSince(date)
    }

    /// Converts the query expression into an SQL WHERE clause and its bound parameters.
    ///
    /// # Returns
    /// - `(String, Vec<String>)`: A tuple containing the SQL fragment and the corresponding parameter values.
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
            QueryExpr::Not(expr) => {
                format!("NOT {}", expr.build_sql(params))
            }
            QueryExpr::DateUntil(date_time) => {
                params.push(date_time.to_rfc3339());
                CurrentDialect::exists_date_until_query(params.len())
            }
            QueryExpr::DateSince(date_time) => {
                params.push(date_time.to_rfc3339());
                CurrentDialect::exists_date_since_query(params.len())
            }
        }
    }
}

/// Represents a full query including logical expression and pagination.
#[derive(Debug, Clone)]
pub struct Query {
    /// The logical expression used for filtering.
    pub expr: QueryExpr,

    /// The maximum number of results to return.
    pub limit: Option<u32>,

    /// The offset into the result set.
    pub offset: Option<u32>,
}

impl Query {
    /// Creates a new query from a query expression.
    pub fn new(expr: QueryExpr) -> Self {
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
    use chrono::DateTime;
    use std::str::FromStr;

    #[test]
    fn test_build_query() {
        let query = Query::new(
            QueryExpr::tag("cat")
                .and(QueryExpr::tag("cute"))
                .or(QueryExpr::not(QueryExpr::tag("dog")))
                .and(QueryExpr::date_until(
                    DateTime::from_str("2025-05-02T01:18:49.678809123+00:00").unwrap(),
                )),
        )
        .with_limit(10)
        .with_offset(20);

        let (sql, params) = query.to_sql();

        assert_eq!(
            format!(
                "((({} AND {}) OR NOT {}) AND {}) LIMIT {} OFFSET {}",
                CurrentDialect::exists_tag_query(1),
                CurrentDialect::exists_tag_query(2),
                CurrentDialect::exists_tag_query(3),
                CurrentDialect::exists_date_until_query(4),
                CurrentDialect::placeholder(5),
                CurrentDialect::placeholder(6),
            ),
            sql
        );
        assert_eq!(
            vec![
                "cat",
                "cute",
                "dog",
                "2025-05-02T01:18:49.678809123+00:00",
                "10",
                "20",
            ],
            params
        );
    }
}
