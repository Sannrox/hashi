use crate::Error;

pub(super) enum Statement {
    Begin,
    Commit,
    Schema { altering: bool },
}

/// Treat unrecognized or potentially destructive statements as a recovery gate.
/// This lexer needs keywords only; quoted names/values cannot grant a safe classification.
pub(super) fn classify(sql: &str) -> Result<Statement, Error> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut tokens = Vec::new();
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                index += 1;
                let mut closed = false;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        return Err(Error::new(
                            "MySQL DDL escape semantics require explicit recovery",
                        ));
                    }
                    if bytes[index] == quote {
                        index += 1;
                        if bytes.get(index) == Some(&quote) {
                            index += 1;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    index += 1;
                }
                if !closed {
                    return Err(Error::new("MySQL statement has an unterminated quote"));
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                if bytes.get(index + 2) == Some(&b'!') {
                    return Err(Error::new(
                        "MySQL executable SQL comments require explicit recovery",
                    ));
                }
                index += 2;
                while index + 1 < bytes.len() && &bytes[index..index + 2] != b"*/" {
                    index += 1;
                }
                if index + 1 >= bytes.len() {
                    return Err(Error::new("MySQL statement has an unterminated comment"));
                }
                index += 2;
            }
            b'#' => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'-' if bytes.get(index + 1) == Some(&b'-')
                && bytes.get(index + 2).is_some_and(u8::is_ascii_whitespace) =>
            {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    index += 1;
                }
                tokens.push(sql[start..index].to_ascii_uppercase());
            }
            _ => index += 1,
        }
    }
    let first = tokens.first().map(String::as_str);
    match first {
        Some("BEGIN" | "SAVEPOINT") => Ok(Statement::Begin),
        Some("RELEASE") if tokens.get(1).is_some_and(|word| word == "SAVEPOINT") => {
            Ok(Statement::Begin)
        }
        Some("COMMIT") => Ok(Statement::Commit),
        Some("CREATE") => Err(Error::new(
            "MySQL CREATE transaction boundary requires explicit recovery; refusing to advance before possible row events",
        )),
        Some("DROP" | "TRUNCATE" | "RENAME") => Err(Error::new(
            "MySQL destructive DDL requires explicit consumer recovery before cursor advancement",
        )),
        Some("ALTER")
            if tokens.iter().any(|word| {
                matches!(
                    word.as_str(),
                    "PARTITION"
                        | "PARTITIONS"
                        | "TABLESPACE"
                        | "TRUNCATE"
                        | "MODIFY"
                        | "CHANGE"
                        | "CONVERT"
                        | "ENGINE"
                        | "FORCE"
                        | "DROP"
                        | "RENAME"
                        | "FOREIGN"
                )
            }) || tokens.iter().skip(1).any(|word| word == "ALTER") =>
        {
            Err(Error::new(
                "MySQL table reset or data-transforming ALTER requires explicit consumer recovery before cursor advancement",
            ))
        }
        Some("ALTER") => Ok(Statement::Schema {
            altering: first == Some("ALTER"),
        }),
        _ => Err(Error::new(
            "MySQL unsupported statement event; refusing to skip source changes",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{Statement, classify};
    #[test]
    fn destructive_ddl_cannot_hide_behind_sql_whitespace_or_comments() {
        for sql in [
            "DROP TABLE t",
            "DROP\nTABLE t",
            "TRUNCATE TABLE t",
            "ALTER TABLE t TRUNCATE\nPARTITION p0",
            "ALTER TABLE t DROP /* comment */ PARTITION p0",
            "ALTER TABLE t RENAME TO other",
            "ALTER TABLE t DROP COLUMN value",
            "ALTER TABLE t RENAME COLUMN old TO new",
            "/*!80000 TRUNCATE TABLE t */",
        ] {
            assert!(classify(sql).is_err(), "{sql}");
        }
        assert!(matches!(
            classify("ALTER TABLE t ADD COLUMN `partition` INT DEFAULT 0"),
            Ok(Statement::Schema { .. })
        ));
    }
}
