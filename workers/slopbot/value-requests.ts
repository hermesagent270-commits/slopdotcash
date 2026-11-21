import { SCORE_WINDOW_DAYS } from "../../src/lib/score-window";
import type { D1Database } from "./persistence";
import { registeredRepository, requireRegisteredRepository } from "./registry";

export type ValueRequest = {
  deliveryId: string;
  installationId: number;
  repositoryId: number;
  owner: string;
  repo: string;
  number: number;
};

/** A durable source-change request, not a score or permission to call a model.
 * Delivery retries must retain exactly the same identifiers.
 */
export async function recordValueRequest(db: D1Database, job: ValueRequest) {
  if (
    !job.deliveryId ||
    ![job.installationId, job.repositoryId, job.number].every(
      (id) => Number.isSafeInteger(id) && id > 0,
    ) ||
    !/^[A-Za-z0-9-]+$/.test(job.owner) ||
    !/^[A-Za-z0-9_.-]+$/.test(job.repo)
  )
    throw new Error("Invalid value request identifiers");
  await requireRegisteredRepository(db, job);
  await db
    .prepare(
      `INSERT INTO slopbot_value_requests
       (delivery_id,installation_id,repository_id,owner,repo,pull_request_number,received_at)
       SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS
       (SELECT 1 FROM slopbot_value_requests WHERE delivery_id=?)`,
    )
    .bind(
      job.deliveryId,
      job.installationId,
      job.repositoryId,
      job.owner,
      job.repo,
      job.number,
      new Date().toISOString(),
      job.deliveryId,
    )
    .run();
  const stored = await db
    .prepare(
      `SELECT 1 AS ok FROM slopbot_value_requests WHERE delivery_id=?
       AND installation_id=? AND repository_id=? AND owner=? AND repo=?
       AND pull_request_number=?`,
    )
    .bind(
      job.deliveryId,
      job.installationId,
      job.repositoryId,
      job.owner,
      job.repo,
      job.number,
    )
    .first<{ ok: number }>();
  if (!stored) throw new Error("Value delivery identity changed");
}

type BranchChangedSource = {
  deliveryId: string;
  installationId: number | null;
  repositoryId: number;
  githubUrl: string | null;
  number: number;
};

const DAY_MS = 24 * 60 * 60 * 1000;
const BRANCH_REQUEUE_LIMIT = 10;

export async function requeueBranchChangedValueSources(
  db: D1Database,
  start: number,
  at = new Date(),
  limit = BRANCH_REQUEUE_LIMIT,
): Promise<number> {
  const windowStart = new Date(
    Math.max(at.getTime() - SCORE_WINDOW_DAYS * DAY_MS, start),
  ).toISOString();
  const row = await db
    .prepare(
      `WITH changed AS (
         SELECT r.project_id AS projectId,r.revision,r.created_at AS changedAt,
           a.key AS position,
           CASE WHEN a.key=0
             THEN json_extract(r.manifest,'$.authority.repositoryNodeId')
             ELSE json_extract(a.value,'$.nodeId') END AS nodeId,
           CAST(CASE WHEN a.key=0
             THEN json_extract(r.manifest,'$.authority.repositoryId')
             ELSE json_extract(a.value,'$.githubId') END AS INTEGER) AS repositoryId
         FROM project_revisions r
         JOIN project_revisions p ON p.project_id=r.project_id
           AND p.revision=r.revision-1
         JOIN json_each(r.manifest,'$.repositories') a
         JOIN json_each(p.manifest,'$.repositories') b ON b.key=a.key
         WHERE julianday(r.created_at)>=julianday(?1)
           AND json_extract(a.value,'$.id')=json_extract(b.value,'$.id')
           AND json_extract(a.value,'$.integrationBranch')
             <>json_extract(b.value,'$.integrationBranch')
       ), sources AS (
         SELECT c.projectId,c.revision,c.position,c.nodeId,c.repositoryId,
           json_extract(j.record_json,'$.pullRequestNumber') AS number
         FROM changed c
         JOIN slopbot_value_journal j ON j.repository_node_id=c.nodeId
         WHERE julianday(j.recorded_at)<julianday(c.changedAt)
           AND julianday(json_extract(j.record_json,'$.occurredAt'))>=julianday(?1)
           AND NOT EXISTS (SELECT 1 FROM slopbot_value_journal n
             WHERE n.supersedes=j.record_digest)
         UNION
         SELECT c.projectId,c.revision,c.position,c.nodeId,c.repositoryId,
           json_extract(source.value,'$.pullRequestNumber')
         FROM changed c
         JOIN slopbot_work_unit_sources s ON s.repository_node_id=c.nodeId
         JOIN slopbot_work_unit_journal w ON w.record_digest=s.record_digest
         JOIN json_each(w.record_json,'$.sources') source
           ON json_extract(source.value,'$.repositoryNodeId')=s.repository_node_id
           AND json_extract(source.value,'$.pullRequestNodeId')=s.pull_request_node_id
         WHERE julianday(w.recorded_at)<julianday(c.changedAt)
           AND julianday(json_extract(source.value,'$.occurredAt'))>=julianday(?1)
           AND NOT EXISTS (SELECT 1 FROM slopbot_work_unit_predecessors x
             WHERE x.predecessor_digest=w.record_digest)
       ), pending AS (
         SELECT 'integration-branch:'||projectId||':'||revision||':'
             ||repositoryId||':'||number AS deliveryId,
           (SELECT q.installation_id FROM slopbot_value_requests q
             WHERE q.repository_id=sources.repositoryId
             ORDER BY q.received_at DESC,q.delivery_id DESC LIMIT 1) AS installationId,
           repositoryId,
           (SELECT json_extract(current.value,'$.githubUrl')
             FROM projects p, json_each(p.manifest,'$.repositories') current
             WHERE p.id=sources.projectId
               AND ((sources.position=0 AND current.key=0)
                 OR (sources.position>0 AND current.key>0
                   AND json_extract(current.value,'$.nodeId')=sources.nodeId))
           ) AS githubUrl,
           number
         FROM sources
         WHERE repositoryId>0 AND number>0
       )
       SELECT json_group_array(json_object('deliveryId',deliveryId,
         'installationId',installationId,'repositoryId',repositoryId,
         'githubUrl',githubUrl,'number',number)) AS rows_json
       FROM (SELECT * FROM pending
         WHERE NOT EXISTS (SELECT 1 FROM slopbot_value_requests v
           WHERE v.delivery_id=pending.deliveryId)
         ORDER BY deliveryId)`,
    )
    .bind(windowStart)
    .first<{ rows_json: string }>();
  if (!row) throw new Error("Branch-changed value sources are unreadable");
  const sources = JSON.parse(row.rows_json) as BranchChangedSource[];
  const active = new Map<string, boolean>();
  let queued = 0;
  for (const source of sources) {
    if (queued >= limit) break;
    if (source.installationId === null || source.githubUrl === null) continue;
    const [owner, repo] = new URL(source.githubUrl).pathname
      .replace(/^\/|\/$/gu, "")
      .split("/");
    const repository = { owner, repo, repositoryId: source.repositoryId };
    const key = `${source.repositoryId}:${owner}/${repo}`;
    if (!active.has(key))
      active.set(key, await registeredRepository(db, repository));
    if (!active.get(key)) continue;
    await db
      .prepare(
        "INSERT OR IGNORE INTO slopbot_deliveries (delivery_id, event, received_at) VALUES (?, 'integration_branch', ?)",
      )
      .bind(source.deliveryId, at.toISOString())
      .run();
    await recordValueRequest(db, {
      deliveryId: source.deliveryId,
      installationId: source.installationId,
      number: source.number,
      ...repository,
    });
    queued++;
  }
  return queued;
}
