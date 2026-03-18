export interface QuerySqlValidationResult {
  errors: string[];
  isValid: boolean;
}

const FORBIDDEN_QUERY_PATTERN =
  /\b(update|delete|alter|attach|detach|pragma|vacuum|reindex|begin|commit|rollback|drop|create|insert|replace)\b/i;
const ALLOWED_QUERY_PATTERNS = [/^select\s+/i, /^with\s+/i];

export function validateQuerySql(sqlText: string): QuerySqlValidationResult {
  const errors: string[] = [];
  const statements = splitSqlStatements(sqlText.trim());

  if (statements.length === 0) {
    return {
      errors: ["Generated query SQL must not be empty."],
      isValid: false,
    };
  }

  if (statements.length > 1) {
    errors.push("Generated query SQL must contain exactly one statement.");
  }

  const [statement] = statements;

  if (statement && FORBIDDEN_QUERY_PATTERN.test(statement)) {
    errors.push("Generated query SQL contains a forbidden SQL statement.");
  }

  if (
    statement &&
    !ALLOWED_QUERY_PATTERNS.some((pattern) => pattern.test(statement))
  ) {
    errors.push("Generated query SQL must start with SELECT or WITH.");
  }

  return {
    errors,
    isValid: errors.length === 0,
  };
}

function splitSqlStatements(sqlText: string): string[] {
  return sqlText
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}
