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

    /// A condition to filter results until a specific date.
    DateUntil(DateTime<Utc>),

    /// A condition to filter results since a specific date.
    DateSince(DateTime<Utc>),
}

impl ImageQueryExpr {
    /// Creates a query expression from a single tag.
    ///
    /// # Arguments
    /// - `tag` - A tag to be used in the query as a condition.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A query expression representing the tag condition.
    pub fn tag<T: Into<String>>(tag: T) -> Self {
        ImageQueryExpr::Tag(tag.into())
    }

    /// Combines two expressions with a logical AND.
    ///
    /// # Arguments
    /// - `other` - The another expression to be combined with.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A new expression representing the logical AND.
    pub fn and(self, other: impl Into<ImageQueryExpr>) -> Self {
        ImageQueryExpr::And(Box::new(self), Box::new(other.into()))
    }

    /// Combines two expressions with a logical OR.
    ///
    /// # Arguments
    /// - `other` - The another expression to be combined with.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A new expression representing the logical OR.
    pub fn or(self, other: impl Into<ImageQueryExpr>) -> Self {
        ImageQueryExpr::Or(Box::new(self), Box::new(other.into()))
    }

    /// Negates a query expression.
    ///
    /// # Arguments
    /// - `expr` - The expression to be negated.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A new expression representing the negation.
    pub fn not(expr: impl Into<ImageQueryExpr>) -> Self {
        ImageQueryExpr::Not(Box::new(expr.into()))
    }

    /// Creates an expression to filter results until a specific date.
    ///
    /// # Arguments
    /// - `date` - The date until which results should be filtered.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A new expression with the date condition.
    pub fn date_until(date: impl AsRef<str>) -> Self {
        ImageQueryExpr::DateUntil(
            DateTime::parse_from_rfc3339(date.as_ref())
                .unwrap()
                .with_timezone(&Utc),
        )
    }

    /// Creates an expression to filter results since a specific date.
    ///
    /// # Arguments
    /// - `date` - The date since which results should be filtered.
    ///
    /// # Returns
    /// - `ImageQueryExpr` - A new expression with the date condition.
    pub fn date_since(date: impl AsRef<str>) -> Self {
        ImageQueryExpr::DateSince(
            DateTime::parse_from_rfc3339(date.as_ref())
                .unwrap()
                .with_timezone(&Utc),
        )
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

/// Creates a query expression from a single tag.
///
/// # Arguments
/// - `tag` - A tag to be used in the query as a condition.
///
/// # Returns
/// - `ImageQueryExpr` - A query expression representing the tag condition.
pub fn tag(tag: impl Into<String>) -> ImageQueryExpr {
    ImageQueryExpr::tag(tag)
}

/// Creates an expression to filter results until a specific date.
///
/// # Arguments
/// - `date` - A reference to a string that represents the date until which results should be filtered.
///
/// # Returns
/// - `ImageQueryExpr` - A new expression representing the condition to filter results until the specified date.
pub fn date_until(date: impl AsRef<str>) -> ImageQueryExpr {
    ImageQueryExpr::date_until(date)
}

/// Creates an expression to filter results since a specific date.
///
/// # Arguments
/// - `date` - A reference to a string that represents the date since which results should be filtered.
///
/// # Returns
/// - `ImageQueryExpr` - A new expression representing the condition to filter results since the specified date.
pub fn date_since(date: impl AsRef<str>) -> ImageQueryExpr {
    ImageQueryExpr::date_since(date)
}

/// Negates a given query expression.
///
/// This function takes a query expression, negates it, and returns a new
/// `ImageQueryExpr` representing the logical NOT of the original expression.
/// It is useful for excluding certain conditions in queries.
///
/// # Arguments
/// - `expr` - The expression to be negated. It can be any expression that
///   implements `Into<ImageQueryExpr>`, allowing for flexible input types.
///
/// # Returns
/// - `ImageQueryExpr` - A new expression representing the negation of the
///   original input expression.
pub fn not(expr: impl Into<ImageQueryExpr>) -> ImageQueryExpr {
    ImageQueryExpr::not(expr.into())
}

/// Represents the kind of the image query, which can either be a query for all images or a filtered query.
#[derive(Debug, Clone, PartialEq)]
pub enum ImageQueryKind {
    /// Represents a query that retrieves all images.
    All,

    /// Represents a filtered query, containing a logical expression.
    Where(ImageQueryExpr),
}

impl ImageQueryKind {
    /// Converts the image query kind into an SQL string and bound parameters.
    ///
    /// # Returns
    /// - `(String, Vec<String>)`: SQL clause and ordered parameters
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

/// Represents the ordering options available for the query results.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderBy {
    /// Orders the results by creation date in ascending order.
    CreatedAtAsc,

    /// Orders the results by creation date in descending order.
    CreatedAtDesc,

    /// Orders the results by file size in ascending order.
    FileSizeAsc,

    /// Orders the results by file size in descending order.
    FileSizeDesc,

    /// Orders the results randomly.
    Random,
}

impl OrderBy {
    /// Converts the ordering option into its corresponding SQL string.
    ///
    /// # Returns
    /// - `&'static str`: The SQL segment for the ORDER BY clause.
    fn to_sql(&self) -> &'static str {
        match self {
            OrderBy::CreatedAtAsc => " ORDER BY created_at ASC",
            OrderBy::CreatedAtDesc => " ORDER BY created_at DESC",
            OrderBy::FileSizeAsc => " ORDER BY file_size ASC",
            OrderBy::FileSizeDesc => " ORDER BY file_size DESC",
            OrderBy::Random => " ORDER BY RANDOM()",
        }
    }
}

/// Represents a full query including logical expression and pagination.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageQuery {
    /// The logical expression used for filtering.
    pub expr: ImageQueryKind,

    /// The maximum number of results to return.
    pub limit: Option<u32>,

    /// The offset into the result set.
    pub offset: Option<u32>,

    /// The ordering of the results.
    pub order: Option<OrderBy>,
}

impl ImageQuery {
    /// Creates a new query from a query expression.
    ///
    /// # Arguments
    /// - `expr` - The logical expression to use for filtering results.
    ///
    /// # Returns
    /// - `Self`: A new `ImageQuery` instance.
    pub fn new(expr: ImageQueryKind) -> Self {
        Self {
            expr,
            limit: None,
            offset: None,
            order: None,
        }
    }

    /// Creates a new filtered query based on the given expression.
    ///
    /// # Arguments
    /// - `expr` - A logical expression to filter the query results.
    ///
    /// # Returns
    /// - `Self`: A new `ImageQuery` instance initialized with the provided filter expression.
    pub fn filter(expr: impl Into<ImageQueryExpr>) -> Self {
        Self::new(ImageQueryKind::Where(expr.into()))
    }

    /// Creates a new query instance that retrieves all images without any filters.
    ///
    /// # Returns
    /// - `Self`: A new `ImageQuery` instance configured to fetch all images.
    pub fn all() -> Self {
        Self::new(ImageQueryKind::All)
    }

    /// Sets the `LIMIT` for this query.
    ///
    /// # Arguments
    /// - `limit` - The maximum number of results to return.
    ///
    /// # Returns
    /// - `Self`: The updated `ImageQuery` instance.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the `OFFSET` for this query.
    ///
    /// # Arguments
    /// - `offset` - The offset into the result set.
    ///
    /// # Returns
    /// - `Self`: The updated `ImageQuery` instance.
    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the `ORDER BY` clause for this query.
    ///
    /// # Arguments
    /// - `order` - The ordering criterion for the results.
    ///
    /// # Returns
    /// - `Self`: The updated `ImageQuery` instance.
    pub fn with_order(mut self, order: OrderBy) -> Self {
        self.order = Some(order);
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

        if let Some(order) = &self.order {
            where_sql.push_str(order.to_sql());
        }

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

#[cfg(test)]
mod tests {
    use super::{CurrentDialect, Dialect, ImageQuery, date_until, not, tag};
    use crate::query::OrderBy;

    #[test]
    fn test_build_query() {
        let query = ImageQuery::filter(
            tag("cat")
                .and(tag("cute"))
                .or(not(tag("dog")))
                .and(date_until("2024-12-01T00:00:00Z")),
        )
        .with_limit(10)
        .with_offset(20)
        .with_order(OrderBy::CreatedAtDesc);

        let (sql, params) = query.to_sql();

        assert_eq!(
            format!(
                "WHERE ((({} AND {}) OR NOT {}) AND {}) ORDER BY created_at DESC LIMIT CAST({} AS INTEGER) OFFSET CAST({} AS INTEGER)",
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
                "2024-12-01T00:00:00+00:00",
                "10",
                "20",
            ],
            params
        );
    }
}
