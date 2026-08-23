import { describe, expect, test } from "bun:test"
import {
  createUpdaterController,
  isUpdaterEnabled,
  requireNewerVersion,
  type UpdaterBackend,
  type UpdaterReadyRecord,
} from "./updater-controller"

function setup(input?: {
  currentVersion?: string
  candidateVersion?: string
  enabled?: boolean
  forkUpdate?: boolean
  ready?: UpdaterReadyRecord
  prepareShutdown?: () => Promise<void>
  downloadUpdate?: () => Promise<unknown>
}) {
  const calls: string[] = []
  const backend: UpdaterBackend = {
    async checkForUpdates() {
      calls.push("check")
      return { isUpdateAvailable: true, updateInfo: { version: input?.candidateVersion ?? "2.0.0" } }
    },
    async downloadUpdate() {
      calls.push("download")
      return input?.downloadUpdate?.()
    },
    quitAndInstall() {
      calls.push("install")
    },
  }
  let ready = input?.ready
  const controller = createUpdaterController({
    enabled: input?.enabled ?? true,
    forkUpdate: input?.forkUpdate ?? false,
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
  test("accepts only strictly newer valid fork versions", () => {
    expect(requireNewerVersion("1.18.18-fork.102", "1.18.18-fork.101")).toBeTrue()
    expect(requireNewerVersion("1.18.18-fork.101", "1.18.18-fork.101")).toBeFalse()
    expect(requireNewerVersion("1.18.18-fork.100", "1.18.18-fork.101")).toBeFalse()
    expect(requireNewerVersion("1.18.19-fork.1", "1.18.18-fork.101")).toBeTrue()
    expect(requireNewerVersion("1.18.19", "1.18.18-fork.101")).toBeFalse()
    expect(requireNewerVersion("1.18.19-fork.0", "1.18.18-fork.101")).toBeFalse()
  })

  test("enables fork updates only for packaged Windows builds with a valid fork version", () => {
    const base = { packaged: true, channel: "prod" as const, updatesDisabled: false, forkUpdate: true }
    expect(isUpdaterEnabled({ ...base, platform: "win32", currentVersion: "1.18.18-fork.101" })).toBeTrue()
    expect(isUpdaterEnabled({ ...base, platform: "linux", currentVersion: "1.18.18-fork.101" })).toBeFalse()
    expect(isUpdaterEnabled({ ...base, platform: "darwin", currentVersion: "1.18.18-fork.101" })).toBeFalse()
    expect(isUpdaterEnabled({ ...base, platform: "win32", currentVersion: "1.18.18" })).toBeFalse()
    expect(isUpdaterEnabled({ ...base, platform: "win32", currentVersion: "1.18.18-fork.0" })).toBeFalse()
  })

  test("keeps official non-fork updater availability behavior on supported channels", () => {
    expect(
      isUpdaterEnabled({
        packaged: true,
        channel: "beta",
        updatesDisabled: false,
        forkUpdate: false,
        platform: "darwin",
        currentVersion: "1.18.18",
      }),
    ).toBeTrue()
  })

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

  test.each(["1.18.18-fork.101", "1.18.18-fork.100"])(
    "rejects an updater backend claim for non-newer fork version %s",
    async (candidateVersion) => {
      const app = setup({
        currentVersion: "1.18.18-fork.101",
        candidateVersion,
        forkUpdate: true,
      })

      await app.controller.start()

      expect(app.calls).toEqual(["check"])
      expect(app.getReady()).toBeUndefined()
      expect(app.controller.getState()).toEqual({ status: "up-to-date" })
    },
  )

  test("a disabled non-Windows fork never checks for updates", async () => {
    const app = setup({ enabled: false, currentVersion: "1.18.18-fork.101", forkUpdate: true })

    await app.controller.start()

    expect(app.calls).toEqual([])
    expect(app.controller.getState()).toEqual({ status: "disabled" })
  })

  test("does not persist or install an update when Windows installer verification fails", async () => {
    const app = setup({
      currentVersion: "1.18.18-fork.101",
      candidateVersion: "1.18.18-fork.102",
      forkUpdate: true,
      downloadUpdate: async () => {
        throw new Error("publisher signature verification failed")
      },
    })

    await app.controller.start()

    expect(app.calls).toEqual(["check", "download"])
    expect(app.getReady()).toBeUndefined()
    expect(app.controller.getState()).toEqual({
      status: "error",
      message: "publisher signature verification failed",
    })
    await expect(app.controller.install()).rejects.toThrow("Update is not ready to install")
    expect(app.calls).not.toContain("install")
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
