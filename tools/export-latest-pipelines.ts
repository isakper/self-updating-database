import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import initSqlJs from "sql.js";

const datasetId = process.argv[2] ?? "dataset_ftuw7he7";
const outDir = resolve("output/pipelines");
mkdirSync(outDir, { recursive: true });

const SQL = await initSqlJs();
const db = new SQL.Database(
  readFileSync(resolve(".data/source-datasets.sqlite"))
);

const revResult = db.exec(`
  SELECT
    optimization_revision_id,
    base_pipeline_version_id,
    candidate_pipeline_version_id,
    decision,
    status,
    updated_at
  FROM optimization_revisions
  WHERE source_dataset_id = '${datasetId.replaceAll("'", "''")}'
  ORDER BY updated_at DESC
  LIMIT 1;
`);

const revRow = revResult[0]?.values?.[0];
if (!revRow) {
  throw new Error(`No optimization revision found for ${datasetId}`);
}

const optimizationRevisionId = String(revRow[0]);
const basePipelineVersionId = String(revRow[1]);
const candidatePipelineVersionId =
  revRow[2] === null ? null : String(revRow[2]);

const versions = [basePipelineVersionId, candidatePipelineVersionId].filter(
  (value): value is string => value !== null
);

for (const pipelineVersionId of versions) {
  const sqlResult = db.exec(`
    SELECT sql_text
    FROM pipeline_versions
    WHERE pipeline_version_id = '${pipelineVersionId.replaceAll("'", "''")}'
    LIMIT 1;
  `);
  const sqlText = String(sqlResult[0]?.values?.[0]?.[0] ?? "");
  const filePath = resolve(outDir, `${datasetId}-${pipelineVersionId}.sql`);
  writeFileSync(filePath, `${sqlText}\n`, "utf8");

  const hasRound = /\bround\s*\(/i.test(sqlText);
  console.log(`wrote_sql=${filePath}`);
  console.log(`has_round_${pipelineVersionId}=${hasRound}`);
}

console.log(`dataset_id=${datasetId}`);
console.log(`optimization_revision_id=${optimizationRevisionId}`);
console.log(`base_pipeline_version_id=${basePipelineVersionId}`);
console.log(
  `candidate_pipeline_version_id=${candidatePipelineVersionId ?? ""}`
);

db.close();
