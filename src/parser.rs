use crate::query::QueryExpr;
use chrono::DateTime;
use nom::{
    AsChar, IResult, Parser,
    branch::alt,
    bytes::complete::{tag as t, take_while1},
    character::complete::{char, multispace0},
    combinator::opt,
    multi::many0,
    sequence::{delimited, preceded},
};
use std::str::FromStr;

// <query>    ::= <or_expr>
// <or_expr>  ::= <and_expr> { "OR" <and_expr> }
// <and_expr> ::= <not_expr> { "AND" <not_expr> }
// <not_expr> ::= [ "NOT" ] <primary>
// <primary>  ::= <date_expr>
//              | "(" <query> ")"
//              | <tag>
pub fn parse_query(input: &str) -> Result<QueryExpr, ParseErrorDetail> {
    let (rest, query) = query_expr(input).map_err(|e| match e {
        nom::Err::Error(e) | nom::Err::Failure(e) => e,
        nom::Err::Incomplete(_) => ParseErrorDetail {
            kind: ParseErrorKind::UnexpectedToken,
            location: "<incomplete>".to_string(),
        },
    })?;

    if !rest.trim().is_empty() {
        return Err(ParseErrorDetail {
            kind: ParseErrorKind::UnexpectedToken,
            location: rest.to_string(),
        });
    }

    Ok(query)
}

fn query_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
    fn or_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        let (input, init) = and_expr(input)?;
        many0(preceded(ws(t("OR")), and_expr))
            .parse(input)
            .map(|(input, rest)| {
                let expr = rest.into_iter().fold(init, |acc, e| acc.or(e));
                (input, expr)
            })
    }

    fn and_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        let (input, init) = not_expr(input)?;
        many0(preceded(ws(t("AND")), not_expr))
            .parse(input)
            .map(|(input, rest)| {
                let expr = rest.into_iter().fold(init, |acc, e| acc.and(e));
                (input, expr)
            })
    }

    fn not_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        let (input, not_opt) = opt(preceded(ws(t("NOT")), primary)).parse(input)?;
        match not_opt {
            Some(expr) => Ok((input, QueryExpr::not(expr))),
            None => primary(input),
        }
    }

    fn primary(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        alt((date_expr, paren_expr, tag)).parse(input)
    }

    fn tag(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        ws(take_while1(|c: char| c.is_alphanumeric() || c == '_'))
            .parse(input)
            .map(|(i, tag_str)| (i, QueryExpr::Tag(tag_str.to_string())))
    }

    fn date_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        let is_datetime_char = |c: char| {
            AsChar::is_dec_digit(c) || c == '-' || c == ':' || c == '.' || c == 'T' || c == 'Z'
        };

        let (input, (_field, op, date_str)) = (
            ws(t("date")),
            ws(alt((t(">="), t("<=")))),
            ws(take_while1(is_datetime_char)),
        )
            .parse(input)?;

        let dt = DateTime::from_str(date_str).expect("Invalid date format");

        match op {
            ">=" => Ok((input, QueryExpr::DateSince(dt))),
            "<=" => Ok((input, QueryExpr::DateUntil(dt))),
            _ => unreachable!(),
        }
    }

    fn paren_expr(input: &str) -> IResult<&str, QueryExpr, ParseErrorDetail> {
        delimited(ws(char('(')), query_expr, ws(char(')'))).parse(input)
    }

    or_expr(input)
}

fn ws<'a, F: 'a>(inner: F) -> impl Parser<&'a str, Output = F::Output, Error = F::Error>
where
    F: Parser<&'a str>,
{
    delimited(multispace0, inner, multispace0)
}

#[derive(Debug, PartialEq)]
pub enum ParseErrorKind {
    UnexpectedToken,
    ExpectedTag,
    ExpectedDate,
    ExpectedExpr,
    InvalidDateFormat,
}

#[derive(Debug, PartialEq)]
pub struct ParseErrorDetail {
    pub kind: ParseErrorKind,
    pub location: String,
}

impl nom::error::ParseError<&str> for ParseErrorDetail {
    fn from_error_kind(input: &str, _kind: nom::error::ErrorKind) -> Self {
        ParseErrorDetail {
            kind: ParseErrorKind::UnexpectedToken,
            location: input.to_string(),
        }
    }

    fn append(_input: &str, _kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::parse_query, query::QueryExpr};
    use chrono::DateTime;
    use std::str::FromStr;

    #[test]
    fn test_parse_query_expr() {
        let input = "cat AND (cute OR NOT dog) AND date >= 2025-05-02T01:18:49.678809123Z";

        assert_eq!(
            QueryExpr::tag("cat")
                .and(QueryExpr::tag("cute").or(QueryExpr::not(QueryExpr::tag("dog"))))
                .and(QueryExpr::date_since(
                    DateTime::from_str("2025-05-02T01:18:49.678809123Z").unwrap()
                )),
            parse_query(input).unwrap()
        );
    }

    #[test]
    fn test_parse_error() {
        let input = "cat AND OR dog";
        let res = parse_query(input);

        dbg!(res);
    }
}
