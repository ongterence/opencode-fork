import { describe, expect, test } from "bun:test"
import type { HomeProjectSelection } from "@/context/layout"
import { ServerConnection } from "@/context/server"
import { completeProjectDelete } from "./home-projects-controller"

describe("home project deletion", () => {
  test("cleans client state before the DELETE success resolves", async () => {
    const calls: string[] = []
    const serverKey = ServerConnection.Key.make("server-1")
    await completeProjectDelete({
      request: async () => {
        calls.push("delete")
      },
      cleanup(project) {
        expect(project).toEqual({ projectID: "project-1", worktree: "/project" })
        calls.push("projects.remove")
        calls.push("sync.forgetProject")
      },
      project: { projectID: "project-1", worktree: "/project" },
      serverKey,
      selection: () => ({ server: serverKey, directory: "/project" }),
      setSelection(next) {
        expect(next).toEqual({ server: serverKey })
        calls.push("selection.reset")
      },
    })

    expect(calls).toEqual(["delete", "projects.remove", "sync.forgetProject", "selection.reset"])
  })

  test("retains selection for a different listed project", async () => {
    const serverKey = ServerConnection.Key.make("server-1")
    let selection: HomeProjectSelection = { server: serverKey, directory: "/keep" }
    await completeProjectDelete({
      request: async () => {},
      cleanup: () => {},
      project: { projectID: "project-1", worktree: "/deleted" },
      serverKey,
      selection: () => selection,
      setSelection(next) {
        selection = next
      },
    })

    expect(selection).toEqual({ server: serverKey, directory: "/keep" })
  })
})
