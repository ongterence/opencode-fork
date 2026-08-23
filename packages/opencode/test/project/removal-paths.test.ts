import { describe, expect, test } from "bun:test"
import * as fs from "fs/promises"
import os from "os"
import path from "path"
import {
  deletionTarget,
  legacyDeletionTarget,
  opaqueStorageKey,
  requireLegacyLeaf,
  requireStrictDescendant,
} from "../../src/project/removal-paths"

describe("project removal paths", () => {
  test("rejects unsafe legacy leaves under both path implementations", () => {
    for (const pathApi of [path.posix, path.win32]) {
      expect(() => requireLegacyLeaf("../outside", "session")).toThrow()
      expect(() => requireLegacyLeaf("a/b", "message")).toThrow()
      expect(() => requireLegacyLeaf("a\\b", "part")).toThrow()
      expect(opaqueStorageKey("../cached-project-id")).toMatch(/^[a-f0-9]{64}$/)
      expect(() =>
        legacyDeletionTarget({
          pathApi,
          dataRoot: pathApi.join(pathApi.sep, "data"),
          category: "storage-project",
          projectID: "../outside",
        }),
      ).toThrow()
    }
  })

  test("allows only strict descendants and preserves posix case", () => {
    expect(requireStrictDescendant(path.posix, "/data/worktree/proj_a", "/data/worktree/proj_a/ws_a")).toBe(
      "/data/worktree/proj_a/ws_a",
    )
    expect(() => requireStrictDescendant(path.posix, "/data/worktree/proj_a", "/data/worktree/proj_a-personal")).toThrow()
    expect(() => requireStrictDescendant(path.posix, "/data", "/Data/project")).toThrow()
    expect(requireStrictDescendant(path.win32, "C:\\Data", "c:\\data\\project")).toBe("c:\\data\\project")
  })

  test("keeps a malicious cached project id inside an opaque namespace", async () => {
    const outside = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-removal-outside-"))
    const sentinel = path.join(outside, "sentinel.txt")
    await fs.writeFile(sentinel, "do not delete")

    try {
      const target = deletionTarget({
        pathApi: path.posix,
        dataRoot: "/data",
        category: "snapshot",
        projectID: "../outside",
      })

      expect(target).toBe(
        `/data/project-artifacts/v1/${opaqueStorageKey("../outside")}/snapshot`,
      )
      expect(await fs.readFile(sentinel, "utf8")).toBe("do not delete")
    } finally {
      await fs.rm(outside, { recursive: true, force: true })
    }
  })

  test("maps ordinary legacy identities to the established storage layout", () => {
    const root = "/data"
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "storage-project", projectID: "proj_abc" }),
    ).toBe("/data/storage/project/proj_abc.json")
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "storage-session", projectID: "proj_abc" }),
    ).toBe("/data/storage/session/proj_abc")
    expect(
      legacyDeletionTarget({
        pathApi: path.posix,
        dataRoot: root,
        category: "storage-session-diff",
        relatedID: "ses_abc",
      }),
    ).toBe("/data/storage/session_diff/ses_abc.json")
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "storage-message", relatedID: "msg_abc" }),
    ).toBe("/data/storage/message/msg_abc")
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "storage-part", relatedID: "prt_abc" }),
    ).toBe("/data/storage/part/prt_abc")
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "snapshot", projectID: "proj_abc" }),
    ).toBe("/data/snapshot/proj_abc")
    expect(
      legacyDeletionTarget({ pathApi: path.posix, dataRoot: root, category: "worktree", projectID: "proj_abc" }),
    ).toBe("/data/worktree/proj_abc")
  })

  test("rejects traversal, separators, absolute prefixes, NUL, and Windows drives before path construction", () => {
    for (const value of ["../outside", "a/b", "a\\b", "/absolute", "\u0000", "C:\\outside"]) {
      expect(() => requireLegacyLeaf(value, "project")).toThrow()
    }
  })
})
