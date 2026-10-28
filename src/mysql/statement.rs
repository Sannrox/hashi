use crate::Error;

pub(super) enum Statement {
    Begin,
    Commit,
    Schema {
        altering: bool,
    },
    /// Account management (`ALTER USER`, `CREATE USER`, `DROP USER`,
    /// `GRANT`, `REVOKE`, `SET PASSWORD`). MySQL commits implicitly around
    /// it, so it is a transaction of its own with no row events (#49).
    Account,
}

/// Leading keywords of the account statements capture skips. Exact pairs:
/// `DROP TABLE` or `ALTER TABLE user` never match, and any other statement,
/// `RENAME USER` and role statements included, keeps failing closed.
fn is_account_statement(tokens: &[String]) -> bool {
    let words: Vec<&str> = tokens.iter().take(2).map(String::as_str).collect();
    matches!(
        words.as_slice(),
        ["ALTER" | "CREATE" | "DROP", "USER"] | ["GRANT" | "REVOKE", ..] | ["SET", "PASSWORD"]
    )
}

/// Treat unrecognized or potentially destructive statements as a recovery gate.
/// This lexer needs keywords only; quoted names/values cannot grant a safe classification.
///
/// A backslash inside quotes stops lexing: what follows depends on the
/// session's escape mode. Account statements are named by their leading
/// keywords, before any credential string, so they are still recognized;
/// anything else fails closed as before.
pub(super) fn classify(sql: &str) -> Result<Statement, Error> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut tokens = Vec::new();
    let mut escaped = false;
    'lex: while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' | b'`' => {
                let quote = bytes[index];
                index += 1;
                let mut closed = false;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        escaped = true;
                        break 'lex;
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
    if is_account_statement(&tokens) {
        return Ok(Statement::Account);
    }
    if escaped {
        return Err(Error::new(
            "MySQL DDL escape semantics require explicit recovery",
        ));
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

    /// Account statements, as the binlog logs them with credential strings
    /// that may hold backslashes, are skipped; table DDL and unknown or
    /// escaped statements still fail closed (#49).
    #[test]
    fn account_statements_are_skipped_and_nothing_else_is() {
        for sql in [
            r"ALTER USER 'hashi'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '$A$005$a\b'c'",
            r"CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'p\w'",
            "DROP USER IF EXISTS 'reader'@'%'",
            "GRANT SELECT ON shop.orders TO 'reader'@'%'",
            "REVOKE SELECT ON shop.orders FROM 'reader'@'%'",
            r"SET PASSWORD FOR 'hashi'@'%' = 'n\ew'",
            "drop /* rotate */ user 'old'@'%'",
        ] {
            assert!(matches!(classify(sql), Ok(Statement::Account)), "{sql}");
        }
        for sql in [
            "DROP TABLE user",
            "ALTER TABLE user DROP COLUMN name",
            "CREATE TABLE user (id INT)",
            "RENAME USER 'a'@'%' TO 'b'@'%'",
            "SET GLOBAL read_only = 1",
            r"ALTER TABLE t COMMENT 'a\b'",
            "/*!80000 DROP USER 'x'@'%' */ DROP TABLE t",
        ] {
            assert!(classify(sql).is_err(), "{sql}");
        }
    }
}
