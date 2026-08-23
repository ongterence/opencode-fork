import { describe, expect, test } from "bun:test"
import type { OpencodeClient, Project } from "@opencode-ai/sdk/v2/client"
import type {
  McpListInput,
  McpResourceCatalogInput,
  SessionApi,
  SessionInfo,
  SessionListInput,
} from "@opencode-ai/client/promise"
import { QueryClient } from "@tanstack/solid-query"
import { canDisposeDirectory, pickDirectoriesToEvict } from "./global-sync/eviction"
import { estimateRootSessionTotal, loadRootSessions } from "./global-sync/session-load"
import {
  cleanupDeletedProjectClientState,
  findStaleProjectRows,
  loadActiveSessionsQuery,
  loadMcpQuery,
  loadMcpResourcesQuery,
  seedActiveSessionStatuses,
} from "./server-sync"
import { ServerScope } from "@/utils/server-scope"
import { createServerSession } from "./server-session"
import type { ServerApi } from "@/utils/server"

type McpApi = ServerApi["mcp"]

describe("deleted project reconciliation", () => {
  test("cleans target aliases without sweeping a nested independent project", () => {
    const calls: string[] = []
    const persisted = [
      { worktree: "/repo", expanded: true },
      { worktree: "/repo/packages/app", expanded: false },
      { worktree: "/tmp/repo-sandbox/packages/ui", expanded: true },
      { worktree: "/repo/vendor/nested", expanded: true },
      { worktree: "/repo/vendor/nested/src", expanded: true },
      { worktree: "/keep", expanded: true },
    ]
    const owner = {
      id: "project-1",
      worktree: "/repo",
      sandboxes: ["/tmp/repo-sandbox"],
      time: { created: 1, updated: 1 },
    } satisfies Project
    const nested = {
      id: "project-2",
      worktree: "/repo/vendor/nested",
      sandboxes: [],
      time: { created: 1, updated: 1 },
    } satisfies Project

    const removed = cleanupDeletedProjectClientState({
      project: { projectID: "project-1", worktree: "/repo" },
      persisted: [...persisted],
      owner,
      catalog: [owner, nested],
      removePersisted(worktree) {
        calls.push("remove:" + worktree)
      },
      forgetProject(worktree) {
        calls.push("forget:" + worktree)
      },
      removeCatalog(projectID) {
        calls.push("catalog:" + projectID)
      },
    })

    expect(calls).toEqual([
      "remove:/repo",
      "forget:/repo",
      "remove:/repo/packages/app",
      "forget:/repo/packages/app",
      "remove:/tmp/repo-sandbox/packages/ui",
      "forget:/tmp/repo-sandbox/packages/ui",
      "catalog:project-1",
    ])
    expect(removed).toEqual(["/repo", "/repo/packages/app", "/tmp/repo-sandbox/packages/ui"])
  })

  test("cleans a nested independent project and all of its aliases", () => {
    const calls: string[] = []
    const parent = {
      id: "project-1",
      worktree: "/repo",
      sandboxes: [],
      time: { created: 1, updated: 1 },
    } satisfies Project
    const owner = {
      id: "project-2",
      worktree: "/repo/vendor/nested",
      sandboxes: [],
      time: { created: 1, updated: 1 },
    } satisfies Project

    const removed = cleanupDeletedProjectClientState({
      project: { projectID: "project-2", worktree: owner.worktree },
      persisted: [
        { worktree: "/repo", expanded: true },
        { worktree: "/repo/vendor/nested", expanded: true },
        { worktree: "/repo/vendor/nested/src", expanded: false },
      ],
      owner,
      catalog: [parent, owner],
      removePersisted(worktree) {
        calls.push("remove:" + worktree)
      },
      forgetProject(worktree) {
        calls.push("forget:" + worktree)
      },
      removeCatalog(projectID) {
        calls.push("catalog:" + projectID)
      },
    })

    expect(calls).toEqual([
      "remove:/repo/vendor/nested",
      "forget:/repo/vendor/nested",
      "remove:/repo/vendor/nested/src",
      "forget:/repo/vendor/nested/src",
      "catalog:project-2",
    ])
    expect(removed).toEqual(["/repo/vendor/nested", "/repo/vendor/nested/src"])
  })

  test("stale cleanup leaves rows owned by a nested catalog project", () => {
    const calls: string[] = []
    const nested = {
      id: "project-2",
      worktree: "/deleted/nested",
      sandboxes: [],
      time: { created: 1, updated: 1 },
    } satisfies Project

    const removed = cleanupDeletedProjectClientState({
      project: { projectID: "", worktree: "/deleted" },
      persisted: [
        { worktree: "/deleted", expanded: true },
        { worktree: "/deleted/nested", expanded: true },
        { worktree: "/deleted/nested/src", expanded: false },
      ],
      catalog: [nested],
      removePersisted(worktree) {
        calls.push("remove:" + worktree)
      },
      forgetProject(worktree) {
        calls.push("forget:" + worktree)
      },
      removeCatalog(projectID) {
        calls.push("catalog:" + projectID)
      },
    })

    expect(calls).toEqual(["remove:/deleted", "forget:/deleted"])
    expect(removed).toEqual(["/deleted"])
  })

  test("retains nested project and sandbox aliases during authoritative reconciliation", () => {
    expect(
      findStaleProjectRows({
        persisted: [
          { worktree: "/repo/packages/app", expanded: true },
          { worktree: "/tmp/repo-sandbox/packages/ui", expanded: true },
          { worktree: "/repo-other", expanded: true },
        ],
        catalog: [
          {
            id: "keep",
            worktree: "/repo/",
            sandboxes: ["/tmp/repo-sandbox/"],
            time: { created: 1, updated: 1 },
          },
        ] satisfies Project[],
      }),
    ).toEqual([{ projectID: "", worktree: "/repo-other" }])
  })

  test("preserves POSIX case and folds Windows drive and UNC case", () => {
    expect(
      findStaleProjectRows({
        persisted: [
          { worktree: "/Repo/packages/app", expanded: true },
          { worktree: "c:\\repo\\packages\\app", expanded: true },
          { worktree: "\\\\SERVER\\SHARE\\REPO\\packages\\app", expanded: true },
        ],
        catalog: [
          { id: "posix", worktree: "/repo", sandboxes: [], time: { created: 1, updated: 1 } },
          { id: "drive", worktree: "C:\\Repo", sandboxes: [], time: { created: 1, updated: 1 } },
          { id: "unc", worktree: "\\\\server\\share\\repo", sandboxes: [], time: { created: 1, updated: 1 } },
        ] satisfies Project[],
      }),
    ).toEqual([{ projectID: "", worktree: "/Repo/packages/app" }])
  })
})

describe("MCP queries", () => {
  test("loads current servers for the requested location", async () => {
    const calls: unknown[] = []
    const queryClient = new QueryClient()
    const result = await queryClient.fetchQuery(
      loadMcpQuery(ServerScope.local, "/project", {
        list: async (input: McpListInput = {}) => {
          calls.push(input)
          return {
            location: { directory: "/project", project: { id: "project", directory: "/project" } },
            data: [
              { name: "docs", status: { status: "connected" } },
              { name: "search", status: { status: "pending" } },
            ],
          }
        },
      } as unknown as McpApi),
    )

    expect(calls).toEqual([{ location: { directory: "/project" } }])
    expect(result).toEqual({ docs: { status: "connected" }, search: { status: "pending" } })
  })

  test("loads and keys the current resource catalog", async () => {
    const calls: unknown[] = []
    const queryClient = new QueryClient()
    const result = await queryClient.fetchQuery(
      loadMcpResourcesQuery(ServerScope.local, "/project", {
        resource: {
          catalog: async (input: McpResourceCatalogInput = {}) => {
            calls.push(input)
            return {
              location: { directory: "/project", project: { id: "project", directory: "/project" } },
              data: {
                resources: [{ server: "docs", name: "Guide", uri: "docs://guide" }],
                templates: [],
              },
            }
          },
        },
      } as unknown as McpApi),
    )

    expect(calls).toEqual([{ location: { directory: "/project" } }])
    expect(result).toEqual({ "docs:docs://guide": { server: "docs", name: "Guide", uri: "docs://guide" } })
  })
})

describe("active session query", () => {
  test("loads active sessions immediately and once per server cache", async () => {
    let calls = 0
    const queryClient = new QueryClient()
    const options = loadActiveSessionsQuery(ServerScope.local, {
      active: async () => {
        calls++
        return { ses_running: { type: "running" } }
      },
    })

    expect(await queryClient.fetchQuery(options)).toEqual({ ses_running: { type: "running" } })
    expect(await queryClient.fetchQuery(options)).toEqual({ ses_running: { type: "running" } })
    expect(calls).toBe(1)
    expect(options.enabled).toBe(true)
    expect([...options.queryKey]).toEqual([ServerScope.local, "activeSessions"])
  })

  test("does not overwrite statuses already written by events", () => {
    const session = createServerSession({} as OpencodeClient)
    session.set("session_status", "ses_retry", { type: "retry", attempt: 2, message: "retrying", next: 10 })

    seedActiveSessionStatuses(session, {
      ses_running: { type: "running" },
      ses_retry: { type: "running" },
    })

    expect(session.data.session_status.ses_running).toEqual({ type: "busy" })
    expect(session.data.session_status.ses_retry).toEqual({
      type: "retry",
      attempt: 2,
      message: "retrying",
      next: 10,
    })
  })
})

describe("pickDirectoriesToEvict", () => {
  test("keeps pinned stores and evicts idle stores", () => {
    const now = 5_000
    const picks = pickDirectoriesToEvict({
      stores: ["a", "b", "c", "d"],
      state: new Map([
        ["a", { lastAccessAt: 1_000 }],
        ["b", { lastAccessAt: 4_900 }],
        ["c", { lastAccessAt: 4_800 }],
        ["d", { lastAccessAt: 3_000 }],
      ]),
      pins: new Set(["a"]),
      max: 2,
      ttl: 1_500,
      now,
    })

    expect(picks).toEqual(["d", "c"])
  })
})

describe("loadRootSessions", () => {
  test("loads and normalizes a limited page of root sessions", async () => {
    const calls: SessionListInput[] = []

    const result = await loadRootSessions({
      api: {
        list: async (query = {}) => {
          calls.push(query)
          return { data: [sessionInfo("session-1")], cursor: {} }
        },
      } satisfies Pick<SessionApi, "list">,
      directory: "dir",
      limit: 10,
    })

    expect(result.data).toEqual([
      expect.objectContaining({ id: "session-1", directory: "dir", slug: "session-1", version: "" }),
    ])
    expect(result.limited).toBe(true)
    expect(calls).toEqual([{ directory: "dir", parentID: null, limit: 10, order: "desc" }])
  })

  test("propagates list failures", () => {
    expect(
      loadRootSessions({
        api: {
          list: async () => {
            throw new Error("failed")
          },
        } satisfies Pick<SessionApi, "list">,
        directory: "dir",
        limit: 25,
      }),
    ).rejects.toThrow("failed")
  })
})

function sessionInfo(id: string) {
  return {
    id,
    projectID: "project-1",
    agent: "build",
    model: { id: "model-1", providerID: "provider-1" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, updated: 1 },
    title: id,
    location: { directory: "dir" },
  } as SessionInfo
}

describe("estimateRootSessionTotal", () => {
  test("keeps exact total for full fetches", () => {
    expect(estimateRootSessionTotal({ count: 42, limit: 10, limited: false })).toBe(42)
  })

  test("marks has-more for full-limit limited fetches", () => {
    expect(estimateRootSessionTotal({ count: 10, limit: 10, limited: true })).toBe(11)
  })

  test("keeps exact total when limited fetch is under limit", () => {
    expect(estimateRootSessionTotal({ count: 9, limit: 10, limited: true })).toBe(9)
  })
})

describe("canDisposeDirectory", () => {
  test("rejects pinned or inflight directories", () => {
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: true,
        booting: false,
        loadingSessions: false,
      }),
    ).toBe(false)
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: true,
        loadingSessions: false,
      }),
    ).toBe(false)
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: false,
        loadingSessions: true,
      }),
    ).toBe(false)
  })

  test("accepts idle unpinned directory store", () => {
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: false,
        loadingSessions: false,
      }),
    ).toBe(true)
  })
})
