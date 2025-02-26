#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Select,
    From,
    Where,
    Identifier(String),
    Count, // TODO: support specifying count
    Equals,
    StringLiteral(String),
    Comma,
    Asterisk,
    EOF,
}

pub fn lexer(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            // Skip whitespace
            c if c.is_ascii_whitespace() => {
                chars.next();
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            '=' => {
                tokens.push(Token::Equals);
                chars.next();
            }
            '\'' => {
                chars.next(); // Skip the opening quote
                let literal: String = chars.by_ref().take_while(|&c| c != '\'').collect();
                tokens.push(Token::StringLiteral(literal));
                chars.next(); // Skip the closing quote
            }
            '*' => {
                tokens.push(Token::Asterisk);
                chars.next();
            }
            _ => {
                if ch.is_alphabetic() {
                    let mut identifier = String::new();
                    // take while consumes non-matching character
                    while let Some(ch) = chars.next_if(|c| !c.is_ascii_whitespace() && *c != ',') {
                        identifier.push(ch)
                    }
                    let token = match identifier.to_lowercase().as_str() {
                        "select" => Token::Select,
                        "from" => Token::From,
                        "where" => Token::Where,
                        "count(*)" => Token::Count,
                        _ => Token::Identifier(identifier),
                    };
                    tokens.push(token);
                } else {
                    panic!("Unexpected character: {}", ch);
                }
            }
        }
    }
    tokens.push(Token::EOF);
    tokens
}

#[cfg(test)]
mod lexer_tests {
    use super::{lexer, Token};

    #[test]
    fn test_tokenizing_multiple_columns() {
        let tokens = lexer("col1, col2, Count(*)");
        assert_eq!(Token::Identifier("col1".to_string()), tokens[0]);
        assert_eq!(Token::Comma, tokens[1]);
        assert_eq!(Token::Identifier("col2".to_string()), tokens[2]);
        assert_eq!(Token::Comma, tokens[3]);
        assert_eq!(Token::Count, tokens[4]);
    }
}
