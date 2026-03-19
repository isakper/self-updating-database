export interface PipelineSqlValidationResult {
  errors: string[];
  isValid: boolean;
}

const FORBIDDEN_PATTERN =
  /\b(update|delete|alter|attach|detach|pragma|vacuum|reindex|begin|commit|rollback)\b/i;
const FORBIDDEN_SOURCE_WRITE_PATTERN =
  /\b(?:create\s+(?:table|view)|insert\s+into|drop\s+(?:table|view))(?:\s+if\s+exists)?\s+source\./i;
const FORBIDDEN_SOURCE_INDEX_PATTERN =
  /\bcreate\s+(?:unique\s+)?index\b[\s\S]*?\bon\s+source\./i;
const ALLOWED_STATEMENT_PATTERNS = [
  /^create\s+table\s+/i,
  /^create\s+view\s+/i,
  /^create\s+(unique\s+)?index\s+(if\s+not\s+exists\s+)?/i,
  /^insert\s+into\s+/i,
  /^drop\s+table\s+if\s+exists\s+/i,
  /^drop\s+view\s+if\s+exists\s+/i,
  /^with\s+/i,
];

export function validatePipelineSql(
  sqlText: string
): PipelineSqlValidationResult {
  const errors: string[] = [];
  const trimmedSql = sqlText.trim();

  if (trimmedSql.length === 0) {
    return {
      errors: ["Pipeline SQL must not be empty."],
      isValid: false,
    };
  }

  if (FORBIDDEN_PATTERN.test(trimmedSql)) {
    errors.push("Pipeline SQL contains a forbidden SQL statement.");
  }

  if (FORBIDDEN_SOURCE_WRITE_PATTERN.test(trimmedSql)) {
    errors.push("Pipeline SQL must not write to source.* tables or views.");
  }

  if (FORBIDDEN_SOURCE_INDEX_PATTERN.test(trimmedSql)) {
    errors.push("Pipeline SQL must not create indexes on source.* objects.");
  }

  splitSqlStatements(trimmedSql).forEach((statement) => {
    if (
      !ALLOWED_STATEMENT_PATTERNS.some((pattern) => pattern.test(statement))
    ) {
      errors.push(`Statement is not allowed in v1 pipeline SQL: ${statement}`);
    }
  });

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
