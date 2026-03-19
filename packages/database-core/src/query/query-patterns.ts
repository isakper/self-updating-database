import { createHash } from "node:crypto";

import type {
  OptimizationHint,
  QueryExecutionLog,
  SqlQueryKind,
  SqlQueryPatternSummary,
} from "../../../shared/src/index.js";

const PATTERN_VERSION = 1;
const AGGREGATE_FUNCTIONS = ["sum", "count", "avg", "min", "max"];
const TOP_LEVEL_KEYWORDS = [
  " union ",
  " intersect ",
  " except ",
  " from ",
  " where ",
  " group by ",
  " having ",
  " order by ",
  " limit ",
] as const;

export interface ExtractQueryPatternOptions {
  cleanDatabaseId: string;
  sqlText: string;
}

export interface ExtractedQueryPattern {
  matchedClusterId: string;
  patternSummary: SqlQueryPatternSummary;
}

export function extractQueryPattern(
  options: ExtractQueryPatternOptions
): ExtractedQueryPattern {
  const normalizedSql = normalizeSql(options.sqlText);
  const fallbackQueryKind: SqlQueryKind =
    normalizedSql.includes(" group by ") ||
    AGGREGATE_FUNCTIONS.some((name) => normalizedSql.includes(`${name}(`))
      ? "aggregate"
      : "detail";

  const clausePositions = findTopLevelKeywordPositions(normalizedSql);
  const fromPosition = clausePositions[" from "];

  if (fromPosition === undefined) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind: fallbackQueryKind,
    });
  }

  const selectPosition = findOuterSelectPosition(normalizedSql, fromPosition);

  if (selectPosition === undefined) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind: fallbackQueryKind,
    });
  }

  if (
    clausePositions[" union "] !== undefined ||
    clausePositions[" intersect "] !== undefined ||
    clausePositions[" except "] !== undefined ||
    clausePositions[" having "] !== undefined
  ) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind: fallbackQueryKind,
    });
  }

  const selectClause = normalizedSql
    .slice(selectPosition + " select ".length, fromPosition)
    .trim();
  const wherePosition = clausePositions[" where "];
  const groupByPosition = clausePositions[" group by "];
  const orderByPosition = clausePositions[" order by "];
  const limitPosition = clausePositions[" limit "];

  const fromClause = sliceClause(normalizedSql, fromPosition, [
    wherePosition,
    groupByPosition,
    orderByPosition,
    limitPosition,
  ]);
  const whereClause =
    wherePosition === undefined
      ? null
      : sliceClause(normalizedSql, wherePosition, [
          groupByPosition,
          orderByPosition,
          limitPosition,
        ]);
  const groupByClause =
    groupByPosition === undefined
      ? null
      : sliceClause(normalizedSql, groupByPosition, [
          orderByPosition,
          limitPosition,
        ]);
  const orderByClause =
    orderByPosition === undefined
      ? null
      : sliceClause(normalizedSql, orderByPosition, [limitPosition]);

  const relationParse = parseRelations(fromClause);

  if (!relationParse) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind: fallbackQueryKind,
    });
  }

  const queryKind = groupByClause ? "aggregate" : "detail";
  const aggregates = parseAggregates(selectClause, relationParse.aliases);

  if (
    (queryKind === "aggregate" && aggregates.length === 0) ||
    hasWindowFunction(normalizedSql)
  ) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind,
    });
  }

  const groupBy =
    groupByClause === null
      ? []
      : parseReferenceList(groupByClause, relationParse.aliases);
  const orderBy =
    orderByClause === null
      ? []
      : parseOrderBy(orderByClause, relationParse.aliases);
  const filters =
    whereClause === null
      ? []
      : parseFilters(whereClause, relationParse.aliases);

  if (!groupBy || !orderBy || !filters) {
    return createUnsupportedPattern({
      cleanDatabaseId: options.cleanDatabaseId,
      normalizedSql,
      queryKind,
    });
  }

  const optimizationEligible =
    queryKind === "detail" ||
    filters.every((filter) => {
      const leftSide = filter
        .split(/\s+(?:in|between|=|<=|>=|<|>)\s+/i)[0]
        ?.trim();
      return !leftSide || groupBy.includes(leftSide);
    });

  const patternSummary: SqlQueryPatternSummary = {
    aggregates: [...new Set(aggregates)].sort(),
    cleanDatabaseId: options.cleanDatabaseId,
    filters: [...new Set(filters)].sort(),
    groupBy: [...new Set(groupBy)].sort(),
    joins: [...new Set(relationParse.joins)].sort(),
    optimizationEligible,
    orderBy: [...new Set(orderBy)].sort(),
    patternFingerprint: "",
    patternVersion: PATTERN_VERSION,
    queryKind,
    relations: [...new Set(relationParse.relations)].sort(),
  };
  const fingerprintPayload = JSON.stringify(patternSummary);
  const patternFingerprint = hashValue(fingerprintPayload);
  const matchedClusterId = buildQueryClusterId(
    options.cleanDatabaseId,
    patternFingerprint
  );

  return {
    matchedClusterId,
    patternSummary: {
      ...patternSummary,
      patternFingerprint,
    },
  };
}

export function buildQueryClusterId(
  cleanDatabaseId: string,
  patternFingerprint: string
): string {
  return `query_cluster_${cleanDatabaseId}_${patternFingerprint.slice(0, 12)}`;
}

export function detectUsedOptimizationObjects(options: {
  optimizationHints: OptimizationHint[];
  sqlText: string;
}): string[] {
  const normalizedSql = normalizeSql(options.sqlText);

  return [
    ...new Set(
      options.optimizationHints.flatMap((hint) =>
        hint.preferredObjects.filter((objectName) =>
          normalizedSql.match(
            new RegExp(`\\b${escapeRegExp(objectName.toLowerCase())}\\b`)
          )
        )
      )
    ),
  ].sort();
}

function createUnsupportedPattern(options: {
  cleanDatabaseId: string;
  normalizedSql: string;
  queryKind: SqlQueryKind;
}): ExtractedQueryPattern {
  const payload = {
    cleanDatabaseId: options.cleanDatabaseId,
    normalizedSql: redactLiterals(options.normalizedSql),
    patternVersion: PATTERN_VERSION,
    queryKind: options.queryKind,
    unsupported: true,
  };
  const patternFingerprint = hashValue(JSON.stringify(payload));

  return {
    matchedClusterId: buildQueryClusterId(
      options.cleanDatabaseId,
      patternFingerprint
    ),
    patternSummary: {
      aggregates: [],
      cleanDatabaseId: options.cleanDatabaseId,
      filters: [],
      groupBy: [],
      joins: [],
      optimizationEligible: false,
      orderBy: [],
      patternFingerprint,
      patternVersion: PATTERN_VERSION,
      queryKind: options.queryKind,
      relations: [],
    },
  };
}

function sliceClause(
  sqlText: string,
  clausePosition: number,
  nextPositions: Array<number | undefined>
): string {
  const nextPosition = nextPositions
    .filter((value): value is number => value !== undefined)
    .filter((value) => value > clausePosition)
    .sort((left, right) => left - right)[0];

  return sqlText
    .slice(clausePosition, nextPosition)
    .replace(/^( from | where | group by | order by | limit )/i, "")
    .trim();
}

function parseRelations(fromClause: string): {
  aliases: Map<string, string>;
  joins: string[];
  relations: string[];
} | null {
  if (fromClause.includes(",")) {
    return null;
  }

  const aliases = new Map<string, string>();
  const relations: string[] = [];
  const joins: string[] = [];
  const trimmedClause = fromClause.trim();
  const joinRegex =
    /\b(inner join|left join|join)\s+([a-z0-9_."`]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?\s+on\s+(.+?)(?=\binner join\b|\bleft join\b|\bjoin\b|$)/gi;
  const firstJoinMatch = joinRegex.exec(trimmedClause);
  const baseSegment =
    firstJoinMatch === null
      ? trimmedClause
      : trimmedClause.slice(0, firstJoinMatch.index).trim();

  const baseRelation = parseRelationToken(baseSegment);

  if (!baseRelation) {
    return null;
  }

  relations.push(baseRelation.tableName);
  aliases.set(baseRelation.alias, baseRelation.tableName);
  aliases.set(baseRelation.tableName, baseRelation.tableName);

  if (firstJoinMatch !== null) {
    joinRegex.lastIndex = firstJoinMatch.index;
  }

  let joinMatch: RegExpExecArray | null;

  while ((joinMatch = joinRegex.exec(trimmedClause)) !== null) {
    const joinType =
      joinMatch[1]?.toLowerCase() === "join"
        ? "inner"
        : joinMatch[1]?.toLowerCase();
    const relationName = stripIdentifierQuotes(joinMatch[2] ?? "");
    const relationAlias = stripIdentifierQuotes(joinMatch[3] ?? relationName);
    const onExpression = joinMatch[4]?.trim() ?? "";

    if (!relationName || !relationAlias || !onExpression) {
      return null;
    }

    relations.push(relationName);
    aliases.set(relationAlias, relationName);
    aliases.set(relationName, relationName);

    const joinParts = splitTopLevel(onExpression, " and ");

    if (joinParts.length !== 1) {
      return null;
    }

    const joinCondition = joinParts[0]?.match(/^(.+?)\s*=\s*(.+)$/i);

    if (!joinCondition) {
      return null;
    }

    const left = canonicalizeReference(joinCondition[1] ?? "", aliases);
    const right = canonicalizeReference(joinCondition[2] ?? "", aliases);

    if (!left || !right) {
      return null;
    }

    joins.push(`${joinType}(${[left, right].sort().join("=")})`);
  }

  return {
    aliases,
    joins,
    relations,
  };
}

function parseRelationToken(
  value: string
): { alias: string; tableName: string } | null {
  const match = value
    .trim()
    .match(/^([a-z0-9_."`]+)(?:\s+(?:as\s+)?([a-z0-9_]+))?$/i);

  if (!match) {
    return null;
  }

  const tableName = stripIdentifierQuotes(match[1] ?? "");
  const alias = stripIdentifierQuotes(match[2] ?? tableName);

  if (!tableName || !alias) {
    return null;
  }

  return {
    alias,
    tableName,
  };
}

function parseAggregates(
  selectClause: string,
  aliases: Map<string, string>
): string[] {
  return splitTopLevel(selectClause, ",")
    .map((entry) => stripTrailingAlias(entry))
    .flatMap((expression) => {
      const aggregateMatch = expression.match(
        /^(sum|count|avg|min|max)\s*\((.*)\)$/i
      );

      if (!aggregateMatch) {
        return [];
      }

      const functionName = aggregateMatch[1]?.toLowerCase() ?? "";
      const inner = (aggregateMatch[2] ?? "").trim();
      const canonicalInner =
        inner === "*"
          ? "*"
          : (canonicalizeReference(inner, aliases) ?? redactLiterals(inner));

      return [`${functionName}(${canonicalInner})`];
    });
}

function parseReferenceList(
  clause: string,
  aliases: Map<string, string>
): string[] | null {
  const values = splitTopLevel(clause, ",").map((value) =>
    canonicalizeReference(stripTrailingAlias(value), aliases)
  );

  return values.every((value): value is string => value !== null)
    ? values
    : null;
}

function parseOrderBy(
  clause: string,
  aliases: Map<string, string>
): string[] | null {
  const values = splitTopLevel(clause, ",").map((entry) => {
    const match = entry.trim().match(/^(.*?)(?:\s+(asc|desc))?$/i);
    const reference = canonicalizeReference(match?.[1] ?? "", aliases);

    if (!reference) {
      return null;
    }

    return `${reference} ${(match?.[2] ?? "asc").toLowerCase()}`;
  });

  return values.every((value): value is string => value !== null)
    ? values
    : null;
}

function parseFilters(
  clause: string,
  aliases: Map<string, string>
): string[] | null {
  if (containsTopLevelKeyword(clause, " or ")) {
    return null;
  }

  const filters = splitTopLevel(clause, " and ").map((entry) =>
    canonicalizeFilter(entry, aliases)
  );

  return filters.every((value): value is string => value !== null)
    ? filters
    : null;
}

function canonicalizeFilter(
  value: string,
  aliases: Map<string, string>
): string | null {
  const trimmedValue = value.trim();
  const betweenMatch = trimmedValue.match(
    /^(.+?)\s+between\s+(.+?)\s+and\s+(.+)$/i
  );

  if (betweenMatch) {
    const reference = canonicalizeReference(betweenMatch[1] ?? "", aliases);
    return reference ? `${reference} between ? and ?` : null;
  }

  const inMatch = trimmedValue.match(/^(.+?)\s+in\s*\((.+)\)$/i);

  if (inMatch) {
    const reference = canonicalizeReference(inMatch[1] ?? "", aliases);
    return reference ? `${reference} in ?` : null;
  }

  const binaryMatch = trimmedValue.match(/^(.+?)\s*(=|<=|>=|<|>)\s*(.+)$/i);

  if (!binaryMatch) {
    return null;
  }

  const reference = canonicalizeReference(binaryMatch[1] ?? "", aliases);

  if (!reference) {
    return null;
  }

  return `${reference} ${binaryMatch[2]} ?`;
}

function canonicalizeReference(
  value: string,
  aliases: Map<string, string>
): string | null {
  const trimmedValue = stripWrappingParentheses(
    stripTrailingAlias(value.trim())
  );

  if (!trimmedValue) {
    return null;
  }

  const qualifiedMatch = trimmedValue.match(/^([a-z0-9_]+)\.([a-z0-9_]+)$/i);

  if (qualifiedMatch) {
    const alias = stripIdentifierQuotes(qualifiedMatch[1] ?? "");
    const columnName = stripIdentifierQuotes(qualifiedMatch[2] ?? "");
    const relationName = aliases.get(alias);

    return relationName ? `${relationName}.${columnName}` : null;
  }

  const unqualifiedMatch = trimmedValue.match(/^([a-z0-9_]+)$/i);

  if (!unqualifiedMatch) {
    return null;
  }

  const relationNames = [...new Set(aliases.values())];

  return relationNames.length === 1
    ? `${relationNames[0]}.${stripIdentifierQuotes(unqualifiedMatch[1] ?? "")}`
    : null;
}

function stripTrailingAlias(value: string): string {
  return value
    .trim()
    .replace(/\s+as\s+[a-z0-9_]+$/i, "")
    .replace(/\s+[a-z0-9_]+$/i, (match) =>
      match.includes("(") || match.includes(".") ? match : ""
    )
    .trim();
}

function stripWrappingParentheses(value: string): string {
  let nextValue = value.trim();

  while (nextValue.startsWith("(") && nextValue.endsWith(")")) {
    nextValue = nextValue.slice(1, -1).trim();
  }

  return nextValue;
}

function normalizeSql(value: string): string {
  return value
    .replace(/--.*$/gm, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/;$/, "")
    .toLowerCase();
}

function redactLiterals(value: string): string {
  return value.replace(/'[^']*'/g, "?").replace(/\b\d+(?:\.\d+)?\b/g, "?");
}

function splitTopLevel(value: string, delimiter: "," | " and "): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let inSingleQuote = false;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index] ?? "";

    if (character === "'" && value[index - 1] !== "\\") {
      inSingleQuote = !inSingleQuote;
    }

    if (!inSingleQuote) {
      if (character === "(") {
        depth += 1;
      } else if (character === ")") {
        depth = Math.max(depth - 1, 0);
      }
    }

    if (
      depth === 0 &&
      !inSingleQuote &&
      value.slice(index, index + delimiter.length).toLowerCase() === delimiter
    ) {
      parts.push(current.trim());
      current = "";
      index += delimiter.length - 1;
      continue;
    }

    current += character;
  }

  if (current.trim().length > 0) {
    parts.push(current.trim());
  }

  return parts;
}

function findTopLevelKeywordPositions(
  sqlText: string
): Partial<Record<(typeof TOP_LEVEL_KEYWORDS)[number], number>> {
  const positions: Partial<
    Record<(typeof TOP_LEVEL_KEYWORDS)[number], number>
  > = {};
  let depth = 0;
  let inSingleQuote = false;

  for (let index = 0; index < sqlText.length; index += 1) {
    const character = sqlText[index] ?? "";

    if (character === "'" && sqlText[index - 1] !== "\\") {
      inSingleQuote = !inSingleQuote;
    }

    if (!inSingleQuote) {
      if (character === "(") {
        depth += 1;
      } else if (character === ")") {
        depth = Math.max(depth - 1, 0);
      }
    }

    if (depth !== 0 || inSingleQuote) {
      continue;
    }

    for (const keyword of TOP_LEVEL_KEYWORDS) {
      if (positions[keyword] !== undefined) {
        continue;
      }

      if (sqlText.slice(index, index + keyword.length) === keyword) {
        positions[keyword] = index;
      }
    }
  }

  return positions;
}

function findOuterSelectPosition(
  sqlText: string,
  fromPosition: number
): number | undefined {
  let depth = 0;
  let inSingleQuote = false;
  let selectPosition: number | undefined;

  for (let index = 0; index < fromPosition; index += 1) {
    const character = sqlText[index] ?? "";

    if (character === "'" && sqlText[index - 1] !== "\\") {
      inSingleQuote = !inSingleQuote;
    }

    if (!inSingleQuote) {
      if (character === "(") {
        depth += 1;
      } else if (character === ")") {
        depth = Math.max(depth - 1, 0);
      }
    }

    if (depth !== 0 || inSingleQuote) {
      continue;
    }

    if (matchesKeywordAt(sqlText, index, "select")) {
      selectPosition = index;
    }
  }

  return selectPosition;
}

function matchesKeywordAt(
  value: string,
  index: number,
  keyword: string
): boolean {
  if (value.slice(index, index + keyword.length) !== keyword) {
    return false;
  }

  const previousCharacter = value[index - 1] ?? " ";
  const nextCharacter = value[index + keyword.length] ?? " ";

  return isWordBoundary(previousCharacter) && isWordBoundary(nextCharacter);
}

function isWordBoundary(value: string): boolean {
  return !/[a-z0-9_]/i.test(value);
}

function containsTopLevelKeyword(value: string, keyword: string): boolean {
  let depth = 0;
  let inSingleQuote = false;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index] ?? "";

    if (character === "'" && value[index - 1] !== "\\") {
      inSingleQuote = !inSingleQuote;
    }

    if (!inSingleQuote) {
      if (character === "(") {
        depth += 1;
      } else if (character === ")") {
        depth = Math.max(depth - 1, 0);
      }
    }

    if (
      depth === 0 &&
      !inSingleQuote &&
      value.slice(index, index + keyword.length).toLowerCase() === keyword
    ) {
      return true;
    }
  }

  return false;
}

function hasWindowFunction(sqlText: string): boolean {
  return containsTopLevelKeyword(sqlText, " over ");
}

function stripIdentifierQuotes(value: string): string {
  return value.replace(/["`]/g, "").trim();
}

function hashValue(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function buildPatternMetadataUpdate(options: {
  queryLog: QueryExecutionLog;
  usedOptimizationObjects?: string[];
}): {
  matchedClusterId: string | null;
  optimizationEligible: boolean;
  patternFingerprint: string;
  patternSummaryJson: QueryExecutionLog["patternSummaryJson"];
  patternVersion: number;
  queryKind: QueryExecutionLog["queryKind"];
  queryLogId: string;
  usedOptimizationObjects?: string[];
} | null {
  if (
    options.queryLog.status !== "succeeded" ||
    options.queryLog.generatedSql === null
  ) {
    return null;
  }

  const extracted = extractQueryPattern({
    cleanDatabaseId: options.queryLog.cleanDatabaseId,
    sqlText: options.queryLog.generatedSql,
  });

  return {
    matchedClusterId: extracted.matchedClusterId,
    optimizationEligible: extracted.patternSummary.optimizationEligible,
    patternFingerprint: extracted.patternSummary.patternFingerprint,
    patternSummaryJson: extracted.patternSummary,
    patternVersion: extracted.patternSummary.patternVersion,
    queryKind: extracted.patternSummary.queryKind,
    queryLogId: options.queryLog.queryLogId,
    ...(options.usedOptimizationObjects
      ? { usedOptimizationObjects: options.usedOptimizationObjects }
      : {}),
  };
}
