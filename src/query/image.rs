use crate::dialect::{CurrentDialect, Dialect};
use chrono::{DateTime, Utc};

/// Represents a logical tag-based query expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ImageQueryExpr {
    /// A single tag condition.
    Tag(String),

    /// Logical AND of two subexpressions.
    And(Box<ImageQueryExpr>, Box<ImageQueryExpr>),

    /// Logical OR of two subexpressions.
    Or(Box<ImageQueryExpr>, Box<ImageQueryExpr>),

    /// Logical NOT of a subexpression.
    Not(Box<ImageQueryExpr>),

    DateUntil(DateTime<Utc>),

    DateSince(DateTime<Utc>),
}

impl ImageQueryExpr {
    /// Creates a query expression from a single tag.
    pub fn tag<T: Into<String>>(tag: T) -> Self {
        ImageQueryExpr::Tag(tag.into())
    }

    /// Combines two expressions with a logical AND.
    pub fn and(self, other: ImageQueryExpr) -> Self {
        ImageQueryExpr::And(Box::new(self), Box::new(other))
    }

    /// Combines two expressions with a logical OR.
    pub fn or(self, other: ImageQueryExpr) -> Self {
        ImageQueryExpr::Or(Box::new(self), Box::new(other))
    }

    /// Negates a query expression.
    pub fn not(expr: ImageQueryExpr) -> Self {
        ImageQueryExpr::Not(Box::new(expr))
    }

    pub fn date_until(date: DateTime<Utc>) -> Self {
        ImageQueryExpr::DateUntil(date)
    }

    pub fn date_since(date: DateTime<Utc>) -> Self {
        ImageQueryExpr::DateSince(date)
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
            ImageQueryExpr::Tag(tag) => {
                params.push(tag.clone());
                CurrentDialect::exists_tag_query(params.len())
            }
            ImageQueryExpr::And(lhs, rhs) => {
                format!("({} AND {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            ImageQueryExpr::Or(lhs, rhs) => {
                format!("({} OR {})", lhs.build_sql(params), rhs.build_sql(params))
            }
            ImageQueryExpr::Not(expr) => {
                format!("NOT {}", expr.build_sql(params))
            }
            ImageQueryExpr::DateUntil(date_time) => {
                params.push(date_time.to_rfc3339());
                CurrentDialect::exists_date_until_query(params.len())
            }
            ImageQueryExpr::DateSince(date_time) => {
                params.push(date_time.to_rfc3339());
                CurrentDialect::exists_date_since_query(params.len())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ImageQueryKind {
    All,
    Where(ImageQueryExpr),
}

impl ImageQueryKind {
    pub fn to_sql(&self) -> (String, Vec<String>) {
        match self {
            ImageQueryKind::All => ("".to_string(), vec![]),
            ImageQueryKind::Where(query_expr) => {
                let (sql, params) = query_expr.to_sql();

                (format!("WHERE {}", sql), params)
            }
        }
    }
}

/// Represents a full query including logical expression and pagination.
#[derive(Debug, Clone)]
pub struct ImageQuery {
    /// The logical expression used for filtering.
    pub expr: ImageQueryKind,

    /// The maximum number of results to return.
    pub limit: Option<u32>,

    /// The offset into the result set.
    pub offset: Option<u32>,
}

impl ImageQuery {
    /// Creates a new query from a query expression.
    pub fn new(expr: ImageQueryKind) -> Self {
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
    use super::{CurrentDialect, Dialect, ImageQuery, ImageQueryExpr, ImageQueryKind};
    use chrono::DateTime;
    use std::str::FromStr;

    #[test]
    fn test_build_query() {
        let query = ImageQuery::new(ImageQueryKind::Where(
            ImageQueryExpr::tag("cat")
                .and(ImageQueryExpr::tag("cute"))
                .or(ImageQueryExpr::not(ImageQueryExpr::tag("dog")))
                .and(ImageQueryExpr::date_until(
                    DateTime::from_str("2025-05-02T01:18:49.678809123+00:00").unwrap(),
                )),
        ))
        .with_limit(10)
        .with_offset(20);

        let (sql, params) = query.to_sql();

        assert_eq!(
            format!(
                "WHERE ((({} AND {}) OR NOT {}) AND {}) LIMIT {} OFFSET {}",
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
