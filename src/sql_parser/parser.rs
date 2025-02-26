use super::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Column {
    All,
    Regular(String),
    Aggregation(AggregateFn),
}

#[derive(Debug, PartialEq)]
pub enum AggregateFn {
    CountAll,
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Equals,
}

#[derive(Debug, PartialEq)]
pub struct Comparison {
    pub operator: Operator,
    pub column: String,
    pub value: String,
}

#[derive(Debug)]
pub struct SelectQuery {
    pub columns: Vec<Column>,
    pub table: String,
    pub where_clause: Option<Comparison>,
}

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    pub fn parse(&mut self) -> SelectQuery {
        self.consume(Token::Select);
        let columns = self.parse_columns();
        self.consume(Token::From);
        let table = self.parse_identifier();
        let where_clause = if self.matches(Token::Where) {
            self.consume(Token::Where);
            Some(self.parse_where_clause())
        } else {
            None
        };
        SelectQuery {
            columns,
            table,
            where_clause,
        }
    }

    fn parse_columns(&mut self) -> Vec<Column> {
        let mut columns = Vec::new();
        loop {
            if self.matches(Token::Count) {
                self.consume(Token::Count);
                columns.push(Column::Aggregation(AggregateFn::CountAll));
            } else if self.matches(Token::Asterisk) {
                self.consume(Token::Asterisk);
                columns.push(Column::All);
            } else {
                let column = self.parse_identifier();
                columns.push(Column::Regular(column));
            }
            if !self.matches(Token::Comma) {
                break;
            }
            self.consume(Token::Comma);
        }
        columns
    }

    fn parse_identifier(&mut self) -> String {
        if let Token::Identifier(name) = self.advance() {
            name
        } else {
            panic!("Expected identifier")
        }
    }

    fn parse_where_clause(&mut self) -> Comparison {
        let left = self.parse_identifier();
        self.consume(Token::Equals);
        let right = if let Token::StringLiteral(value) = self.advance() {
            value
        } else {
            panic!("Expected string literal")
        };
        Comparison {
            operator: Operator::Equals,
            column: left,
            value: right,
        }
    }

    fn matches(&self, token: Token) -> bool {
        self.tokens.get(self.position) == Some(&token)
    }

    fn consume(&mut self, token: Token) {
        if self.matches(token.clone()) {
            self.position += 1;
        } else {
            panic!(
                "Expected token: {:?} recieved {:?}",
                token,
                self.tokens
                    .get(self.position)
                    .expect("couldn't get current token")
            );
        }
    }

    fn advance(&mut self) -> Token {
        self.position += 1;
        self.tokens.get(self.position - 1).unwrap().clone()
    }
}
#[cfg(test)]
mod parser_tests {
    use crate::sql_parser::{
        lexer::lexer,
        parser::{AggregateFn, Column, Comparison, Operator},
    };

    use super::{Parser, SelectQuery};

    fn parse_sql(query: &str) -> SelectQuery {
        let tokens = lexer(query);
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    #[test]
    fn test_select_count() {
        let query = "SELECT COUNT(*) FROM apples";
        let parsed_query = parse_sql(query);
        assert_eq!(
            parsed_query.columns[0],
            Column::Aggregation(AggregateFn::CountAll)
        );
        assert_eq!(parsed_query.table, "apples")
    }

    #[test]
    fn test_select_column() {
        let query = "SELECT name FROM apples";
        let parsed_query = parse_sql(query);
        assert_eq!(parsed_query.columns[0], Column::Regular("name".to_string()));
        assert_eq!(parsed_query.table, "apples")
    }

    #[test]
    fn test_select_column_with_where() {
        let query = "SELECT name FROM apples WHERE color = 'Yellow'";
        let parsed_query = parse_sql(query);
        assert_eq!(parsed_query.columns[0], Column::Regular("name".to_string()));
        assert_eq!(parsed_query.table, "apples");
        let expected_comparison = Comparison {
            column: "color".to_string(),
            value: "Yellow".to_string(),
            operator: Operator::Equals,
        };
        assert_eq!(parsed_query.where_clause.unwrap(), expected_comparison)
    }

    #[test]
    fn test_select_count_and_column() {
        let query = "SELECT COUNT(*), name FROM apples WHERE color = 'Yellow'";
        let parsed_query = parse_sql(query);
        assert_eq!(
            parsed_query.columns[0],
            Column::Aggregation(AggregateFn::CountAll)
        );
        assert_eq!(parsed_query.columns[1], Column::Regular("name".to_string()));
        assert_eq!(parsed_query.table, "apples")
    }

    #[test]
    fn test_select_all_columns() {
        let query = "SELECT * from oranges";
        let parsed_query = parse_sql(query);
        assert_eq!(parsed_query.columns[0], Column::All);
        assert_eq!(parsed_query.table, "oranges")
    }
}
