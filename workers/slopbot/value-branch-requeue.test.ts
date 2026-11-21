import { afterEach, expect, setSystemTime, test } from "bun:test";
import { createHash, generateKeyPairSync } from "node:crypto";
import { assertProjectDefinition } from "../../src/lib/project-schema.mjs";
import type { ProjectDefinition } from "../../src/lib/projects.mjs";
import { collectTargetRepositories } from "../../src/lib/repositories.mjs";
import { REWARD_ASSET_NETWORKS } from "../../src/lib/reward-asset.mjs";
import { VALUE_DIMENSIONS } from "../../src/lib/value-assessment";
import { applicationManifest } from "../../tests/project-application-manifest";
import { sqliteD1 } from "../../tests/sqlite-d1";
import { fixture, policy, repository } from "../../tests/value-binary-fixture";
import worker from "./index";
import { recordDelivery } from "./persistence";
import { consumeValueRequest, type ValueConsumerEnv } from "./value-consumer";
import { readValueDecision } from "./value-consumption";
import { collectValueEvidence } from "./value-evidence";
import { recordValueRequest } from "./value-requests";

const registered = "branch-change/toolkit";
const DAY = 24 * 60 * 60 * 1000;

afterEach(() => setSystemTime());

function registry() {
  const { db, sqlite } = sqliteD1();
  const clock = sqlite
    .query("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS at")
    .get() as { at: string };
  const now = Date.parse(clock.at) + 60_000;
  const day = (offset: number) => new Date(now + offset * DAY).toISOString();
  const project = assertProjectDefinition(
    applicationManifest("branch-change", registered, {
      status: "active",
      rewardAsset: {
        network: "ethereum",
        kind: "stablecoin",
        ...REWARD_ASSET_NETWORKS.ethereum.stablecoin,
        chainId: REWARD_ASSET_NETWORKS.ethereum.chainId,
        cluster: null,
        verifiedAt: day(-60),
      },
    }),
  );
  const revise = (
    revision: number,
    manifest: ProjectDefinition,
    at: string,
  ) => {
    const text = JSON.stringify(manifest);
    const digest = createHash("sha256").update(text).digest("hex");
    sqlite.run(
      "INSERT INTO project_revisions(project_id,revision,manifest,manifest_sha256,reason,created_at) VALUES (?,?,?,?,?,?)",
      [project.id, revision, text, digest, `revision ${revision}`, at],
    );
    if (revision === 1)
      sqlite.run(
        "INSERT INTO projects(id,status,revision,manifest,manifest_sha256,created_at,updated_at) VALUES (?,?,1,?,?,?,?)",
        [project.id, manifest.status, text, digest, at, at],
      );
    else
      sqlite.run(
        "UPDATE projects SET status=?,revision=?,manifest=?,manifest_sha256=?,updated_at=? WHERE id=?",
        [manifest.status, revision, text, digest, at, project.id],
      );
  };
  revise(1, project, day(-60));
  sqlite.run(
    "INSERT INTO project_repositories(repository_key,project_id) VALUES (?,?)",
    [registered, project.id],
  );
  sqlite.run(
    "INSERT INTO slopbot_installations(installation_id,account_id,account_login,account_type,created_at) VALUES (1,77,'branch-change','Organization',?)",
    [day(-60)],
  );
  const amend = (
    revision: number,
    change: { branch?: string; status?: "active" | "paused"; name?: string },
    at = day(-1),
  ) =>
    revise(
      revision,
      {
        ...project,
        name: change.name ?? project.name,
        status: change.status ?? project.status,
        repositories: [
          {
            ...project.repositories[0],
            integrationBranch:
              change.branch ?? project.repositories[0].integrationBranch,
          },
          ...project.repositories.slice(1),
        ],
      },
      at,
    );
  const assessed = async (number: number, occurred: number) => {
    const deliveryId = `webhook-${number}`;
    const occurredAt = day(occurred);
    await recordDelivery(db, deliveryId, "pull_request");
    sqlite.run(
      `INSERT INTO slopbot_value_requests
       (delivery_id,installation_id,repository_id,owner,repo,pull_request_number,received_at)
       VALUES (?,1,123456789,'branch-change','toolkit',?,?)`,
      [deliveryId, number, occurredAt],
    );
    sqlite.run(
      `INSERT INTO slopbot_value_processing
       (delivery_id,claim_token,lease_until,state,outcome,retry_at,updated_at)
       VALUES (?,'done',?,'completed','published',NULL,?)`,
      [deliveryId, occurredAt, occurredAt],
    );
    const digest = (label: string) =>
      createHash("sha256").update(`${label}:${number}`).digest("hex");
    const record = {
      schemaVersion: "slop-value-record:v1",
      projectId: project.id,
      repository: registered,
      repositoryNodeId: "R_example",
      repositoryDatabaseId: 123456789,
      pullRequestNodeId: `PR_${number}`,
      pullRequestNumber: number,
      headSha: "b".repeat(40),
      mergeSha: "c".repeat(40),
      occurredAt,
      sourceDigest: digest("source"),
      supersedes: null,
      revisionReason: null,
    };
    sqlite.run(
      `INSERT INTO slopbot_value_journal
       (record_digest,assessment_key,repository_node_id,pull_request_node_id,head_sha,source_digest,supersedes,record_json,recorded_at)
       VALUES (?,?,?,?,?,?,NULL,?,?)`,
      [
        digest("record"),
        digest("assessment"),
        "R_example",
        `PR_${number}`,
        record.headSha,
        record.sourceDigest,
        JSON.stringify(record),
        day(occurred + 0.1),
      ],
    );
  };
  const env = {
    SLOPBOT_DB: db,
    SLOPBOT_APP_ID: "1",
    SLOPBOT_APP_PRIVATE_KEY: "unused",
    SLOPBOT_APP_USER_ID: "2",
    SLOPBOT_WEBHOOK_SECRET: "unused",
    SLOPBOT_QUEUE: { send: async () => {} },
    SLOPBOT_VALUE_MONTHLY_BUDGET_MICRO_USD: "alerts-only",
    SLOPBOT_VALUE_START_AT: day(-10),
  };
  const scheduled = async (offset = 0) => {
    setSystemTime(new Date(day(offset)));
    await (
      worker as unknown as {
        scheduled(event: unknown, env: unknown): Promise<void>;
      }
    ).scheduled({}, env);
  };
  const requeued = () =>
    sqlite
      .query<{ delivery_id: string; pull_request_number: number }, []>(
        `SELECT r.delivery_id,r.pull_request_number FROM slopbot_value_requests r
         JOIN slopbot_deliveries d ON d.delivery_id=r.delivery_id
         WHERE d.event='integration_branch'
         ORDER BY r.pull_request_number`,
      )
      .all();
  return { sqlite, amend, assessed, scheduled, requeued, day };
}

test("an owner branch change queues a value request for each assessed pull request in the scoring window", async () => {
  const { amend, assessed, scheduled, requeued, sqlite } = registry();
  await assessed(7, -5);
  await assessed(8, -4);
  amend(2, { branch: "vibestack" });
  await scheduled();
  expect(requeued()).toEqual([
    {
      delivery_id: "integration-branch:branch-change:2:123456789:7",
      pull_request_number: 7,
    },
    {
      delivery_id: "integration-branch:branch-change:2:123456789:8",
      pull_request_number: 8,
    },
  ]);
  expect(
    sqlite
      .query(
        "SELECT installation_id,repository_id,owner,repo FROM slopbot_value_requests WHERE delivery_id=?",
      )
      .get("integration-branch:branch-change:2:123456789:7"),
  ).toEqual({
    installation_id: 1,
    repository_id: 123456789,
    owner: "branch-change",
    repo: "toolkit",
  });
  await scheduled(0.01);
  expect(requeued()).toHaveLength(2);
});

test("records before the API start or assessed after the change stay unqueued, and the window ends the scan", async () => {
  const { amend, assessed, scheduled, requeued } = registry();
  await assessed(1, -12);
  await assessed(2, -0.5);
  await assessed(3, -3);
  amend(2, { branch: "vibestack" });
  await scheduled();
  expect(requeued().map((row) => row.pull_request_number)).toEqual([3]);
  await assessed(4, -2);
  await scheduled(36);
  expect(requeued().map((row) => row.pull_request_number)).toEqual([3]);
});

test("an unchanged branch queues nothing", async () => {
  const { assessed, scheduled, requeued, amend } = registry();
  await assessed(4, -3);
  amend(2, { name: "Renamed toolkit" });
  await scheduled();
  expect(requeued()).toEqual([]);
});

test("a paused project waits and its requests are queued once it is active", async () => {
  const { amend, assessed, scheduled, requeued, day } = registry();
  await assessed(5, -3);
  amend(2, { branch: "vibestack" });
  amend(3, { branch: "vibestack", status: "paused" }, day(-0.9));
  await scheduled();
  expect(requeued()).toEqual([]);
  amend(4, { branch: "vibestack" }, day(0.5));
  await scheduled(0.6);
  expect(requeued().map((row) => row.delivery_id)).toEqual([
    "integration-branch:branch-change:2:123456789:5",
  ]);
});

test("each run queues at most ten requests and later runs continue", async () => {
  const { amend, assessed, scheduled, requeued } = registry();
  for (let number = 10; number < 22; number++) await assessed(number, -3);
  amend(2, { branch: "vibestack" });
  await scheduled();
  expect(requeued()).toHaveLength(10);
  await scheduled(0.01);
  expect(requeued()).toHaveLength(12);
});

test("the queued request replaces the stale record with a current successor under the new branch", async () => {
  const start = new Date().toISOString();
  setSystemTime(new Date(start));
  const { db, sqlite } = sqliteD1();
  const source = fixture({ direct: true, textOnly: true });
  const mergedAt = new Date(Date.parse(start) - 2 * DAY)
    .toISOString()
    .replace(/\.\d{3}Z$/u, "Z");
  Object.assign(source.pull, {
    user: { node_id: "U_author", login: "author", type: "User" },
    merged_at: mergedAt,
    closed_at: mergedAt,
    updated_at: mergedAt,
  });
  const project = assertProjectDefinition(
    applicationManifest("fixture", repository.id, {
      rewardAsset: {
        network: "ethereum",
        kind: "stablecoin",
        ...REWARD_ASSET_NETWORKS.ethereum.stablecoin,
        chainId: REWARD_ASSET_NETWORKS.ethereum.chainId,
        cluster: null,
        verifiedAt: start,
      },
      status: "active",
      authority: {
        state: "unverified",
        reason: "missing-repository-proof",
        role: "project-steward",
        repositoryId: "42",
        repositoryNodeId: "R_binary",
        proof: null,
      },
    }),
  );
  const moved = assertProjectDefinition({
    ...project,
    repositories: [
      { ...project.repositories[0], integrationBranch: "vibestack" },
      ...project.repositories.slice(1),
    ],
  });
  const write = (revision: number, manifest: ProjectDefinition, at: string) => {
    const text = JSON.stringify(manifest);
    const digest = createHash("sha256").update(text).digest("hex");
    sqlite.run(
      "INSERT INTO project_revisions(project_id,revision,manifest,manifest_sha256,reason,created_at) VALUES (?,?,?,?,?,?)",
      [project.id, revision, text, digest, `revision ${revision}`, at],
    );
    if (revision === 1)
      sqlite.run(
        "INSERT INTO projects(id,status,revision,manifest,manifest_sha256,created_at,updated_at) VALUES (?,'active',1,?,?,?,?)",
        [project.id, text, digest, at, at],
      );
    else
      sqlite.run(
        "UPDATE projects SET revision=?,manifest=?,manifest_sha256=?,updated_at=? WHERE id=?",
        [revision, text, digest, at, project.id],
      );
  };
  write(1, project, start);
  sqlite.run(
    "INSERT INTO project_repositories(repository_key,project_id) VALUES (?,?)",
    [repository.id, project.id],
  );
  sqlite.run(
    "INSERT INTO slopbot_installations(installation_id,account_id,account_login,account_type,created_at) VALUES (1,1,'example','Organization',?)",
    [start],
  );
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const input = (await request.json()) as { model: string };
      return Response.json(
        {
          id: "local-branch-change",
          type: "message",
          role: "assistant",
          model: input.model,
          content: [
            {
              type: "text",
              text: JSON.stringify({
                status: "assessed",
                tier: "small",
                rationale: "This local fixture records a small accepted fix.",
                dimensions: Object.fromEntries(
                  VALUE_DIMENSIONS.map((dimension) => [
                    dimension,
                    "Fixture dimension.",
                  ]),
                ),
                evidenceUrls: [source.pull.html_url],
                relatedPullRequests: [],
                limitations: [],
                reviews: [],
              }),
            },
          ],
          stop_reason: "end_turn",
          stop_sequence: null,
          usage: { input_tokens: 10, output_tokens: 10, buyer_cost_micro: 1 },
        },
        { headers: { "x-request-id": "local-branch-change-request" } },
      );
    },
  });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    if (url.origin === `http://127.0.0.1:${server.port}`)
      return originalFetch(input, init);
    if (url.origin !== "https://api.github.com")
      throw new Error("Unexpected external transport");
    if (url.pathname === "/app/installations/1/access_tokens")
      return Response.json({ token: "local-installation" });
    if (url.pathname === "/installation/repositories")
      return Response.json({
        repositories: [{ id: 42, node_id: "R_binary", private: false }],
      });
    if (url.pathname === "/graphql") {
      const body = JSON.parse(String(init?.body ?? "{}")) as { query: string };
      if (body.query.includes("search(query:"))
        return Response.json({
          data: {
            search: {
              issueCount: 1,
              pageInfo: { hasNextPage: false, endCursor: null },
              nodes: [
                {
                  id: source.pull.node_id,
                  number: source.pull.number,
                  url: source.pull.html_url,
                  mergedAt: source.pull.merged_at,
                  repository: {
                    id: "R_binary",
                    databaseId: 42,
                    nameWithOwner: repository.id,
                    isPrivate: false,
                  },
                  mergeCommit: { oid: source.pull.merge_commit_sha },
                  commits: {
                    totalCount: 1,
                    nodes: [{ commit: { oid: source.pull.head.sha } }],
                    pageInfo: { hasNextPage: false, endCursor: null },
                  },
                },
              ],
            },
          },
        });
      return Response.json({ data: await source.client.graphql() });
    }
    if (/\/commits\/[a-f0-9]{40}\/pulls$/.test(url.pathname))
      return Response.json([
        {
          node_id: source.pull.node_id,
          number: source.pull.number,
          html_url: source.pull.html_url,
          state: "closed",
          merged_at: source.pull.merged_at,
          base: {
            repo: {
              id: 42,
              node_id: "R_binary",
              full_name: repository.id,
              private: false,
            },
          },
        },
      ]);
    if (url.pathname.endsWith("/contents/CONTRIBUTING.md"))
      return Response.json({
        type: "file",
        encoding: "base64",
        size: "Retain accepted source evidence.".length,
        content: Buffer.from("Retain accepted source evidence.").toString(
          "base64",
        ),
      });
    const result = await source.client.get(url.pathname + url.search);
    return Response.json(result, { status: result === null ? 404 : 200 });
  }) as typeof fetch;
  const env = {
    SLOPBOT_DB: db,
    SLOPBOT_APP_ID: "1",
    SLOPBOT_APP_PRIVATE_KEY: generateKeyPairSync("rsa", {
      modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
      publicKeyEncoding: { type: "spki", format: "pem" },
    }).privateKey,
    SLOPBOT_APP_USER_ID: "2",
    SURPLUS_API_KEY: "local-only",
    SURPLUS_BASE_URL: `http://127.0.0.1:${server.port}`,
    SLOPBOT_PRODUCER_REVISION: "a".repeat(40),
    SLOPBOT_VALUE_MONTHLY_BUDGET_MICRO_USD: "alerts-only",
  } as ValueConsumerEnv;
  const decision = async (manifest: ProjectDefinition) => {
    const repositories = collectTargetRepositories([manifest]);
    return readValueDecision(
      db,
      await collectValueEvidence(
        source.client,
        repository.id,
        1,
        repositories[0],
      ),
      policy,
      repositories,
    );
  };
  try {
    await recordDelivery(db, "webhook-1", "pull_request");
    await recordValueRequest(db, {
      deliveryId: "webhook-1",
      installationId: 1,
      repositoryId: 42,
      owner: "example",
      repo: "binary-proof",
      number: 1,
    });
    expect(
      await consumeValueRequest(env, {
        deliveryId: "webhook-1",
        installationId: 1,
        repositoryId: 42,
        owner: "example",
        repo: "binary-proof",
        number: 1,
      }),
    ).toBe("published");
    expect((await decision(project)).status).toBe("assessed");
    const changedAt = new Date(Date.parse(start) + 60_000).toISOString();
    setSystemTime(new Date(changedAt));
    write(2, moved, changedAt);
    expect((await decision(moved)).status).toBe("stale");
    setSystemTime(new Date(Date.parse(start) + 120_000));
    await (
      worker as unknown as {
        scheduled(event: unknown, env: unknown): Promise<void>;
      }
    ).scheduled(
      {},
      {
        SLOPBOT_DB: db,
        SLOPBOT_APP_ID: "1",
        SLOPBOT_APP_PRIVATE_KEY: "unused",
        SLOPBOT_APP_USER_ID: "2",
        SLOPBOT_WEBHOOK_SECRET: "unused",
        SLOPBOT_QUEUE: { send: async () => {} },
        SLOPBOT_VALUE_MONTHLY_BUDGET_MICRO_USD: "alerts-only",
      },
    );
    const queued = sqlite
      .query<
        {
          deliveryId: string;
          installationId: number;
          repositoryId: number;
          owner: string;
          repo: string;
          number: number;
        },
        []
      >(
        `SELECT delivery_id AS deliveryId,installation_id AS installationId,
         repository_id AS repositoryId,owner,repo,pull_request_number AS number
         FROM slopbot_value_requests WHERE delivery_id LIKE 'integration-branch:%'`,
      )
      .all();
    expect(queued).toEqual([
      {
        deliveryId: "integration-branch:fixture:2:42:1",
        installationId: 1,
        repositoryId: 42,
        owner: "example",
        repo: "binary-proof",
        number: 1,
      },
    ]);
    setSystemTime(new Date(Date.parse(start) + 3 * 60 * 60 * 1000));
    expect(await consumeValueRequest(env, queued[0])).toBe("published");
    const records = sqlite
      .query<{ record_digest: string; supersedes: string | null }, []>(
        "SELECT record_digest,supersedes FROM slopbot_value_journal ORDER BY sequence",
      )
      .all();
    expect(records).toHaveLength(2);
    expect(records[1].supersedes).toBe(records[0].record_digest);
    const current = await decision(moved);
    expect(current.status).toBe("assessed");
    expect(current.latestDigest).toBe(records[1].record_digest);
  } finally {
    globalThis.fetch = originalFetch;
    server.stop(true);
    sqlite.close();
  }
});
