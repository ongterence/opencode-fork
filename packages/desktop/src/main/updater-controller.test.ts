import { describe, expect, test } from "bun:test"
import { createUpdaterController, type UpdaterBackend, type UpdaterReadyRecord } from "./updater-controller"

function setup(input?: {
  currentVersion?: string
  ready?: UpdaterReadyRecord
  prepareShutdown?: () => Promise<void>
}) {
  const calls: string[] = []
  const backend: UpdaterBackend = {
    async checkForUpdates() {
      calls.push("check")
      return { isUpdateAvailable: true, updateInfo: { version: "2.0.0" } }
    },
    async downloadUpdate() {
      calls.push("download")
    },
    quitAndInstall() {
      calls.push("install")
    },
  }
  let ready = input?.ready
  const controller = createUpdaterController({
    enabled: true,
    currentVersion: input?.currentVersion ?? "1.0.0",
    backend,
    persistence: {
      get: () => ready,
      set: (value) => {
        ready = value
      },
      clear: () => {
        ready = undefined
      },
    },
    prepareShutdown: input?.prepareShutdown ?? (async () => calls.push("prepare")),
    stop: async () => {
      calls.push("stop")
    },
  })
  return { controller, calls, getReady: () => ready }
}

describe("updater controller", () => {
  test("checks, downloads, persists, and publishes one authoritative ready state", async () => {
    const app = setup()
    const states: ReturnType<typeof app.controller.getState>[] = []
    app.controller.subscribe((state) => states.push(state))

    await app.controller.start()

    expect(app.calls).toEqual(["check", "download"])
    expect(app.getReady()).toEqual({ version: "2.0.0" })
    expect(states.map((state) => state.status)).toEqual(["idle", "checking", "downloading", "ready"])
    expect(app.controller.getState()).toEqual({ status: "ready", version: "2.0.0" })
  })

  test("revalidates a persisted target through the updater cache on launch", async () => {
    const app = setup({ ready: { version: "2.0.0" } })

    await app.controller.start()

    expect(app.calls).toEqual(["check", "download"])
    expect(app.controller.getState()).toEqual({ status: "ready", version: "2.0.0" })
  })

  test("clears a target already installed before checking", async () => {
    const app = setup({ currentVersion: "2.0.0", ready: { version: "2.0.0" } })

    await app.controller.start()

    expect(app.getReady()).toBeUndefined()
    expect(app.calls).toEqual(["check"])
  })

  test("coalesces concurrent checks", async () => {
    const app = setup()

    await Promise.all([app.controller.check(), app.controller.check(), app.controller.check()])

    expect(app.calls).toEqual(["check", "download"])
  })

  test("returns to ready when quitAndInstall returns without exiting", async () => {
    const app = setup()
    await app.controller.start()

    await app.controller.install()

    expect(app.calls).toEqual(["check", "download", "prepare", "stop", "install"])
    expect(app.controller.getState()).toEqual({ status: "ready", version: "2.0.0" })
  })

  test("waits for the deletion shutdown boundary before stopping or installing", async () => {
    let resolve!: () => void
    const boundary = new Promise<void>((done) => {
      resolve = done
    })
    const app = setup({ prepareShutdown: () => boundary })
    await app.controller.start()

    const installing = app.controller.install()
    await Promise.resolve()
    expect(app.calls).toEqual(["check", "download"])

    resolve()
    await installing

    expect(app.calls).toEqual(["check", "download", "stop", "install"])
  })

  test("keeps the installer ready and sidecars running when shutdown preparation fails", async () => {
    const app = setup({
      prepareShutdown: async () => {
        throw new Error("Project deletion could not reach a durable shutdown boundary")
      },
    })
    await app.controller.start()

    await expect(app.controller.install()).rejects.toThrow("durable shutdown boundary")
    expect(app.calls).toEqual(["check", "download"])
    expect(app.getReady()).toEqual({ version: "2.0.0" })
    expect(app.controller.getState()).toEqual({ status: "ready", version: "2.0.0" })
  })
})
