import type { UpdaterState } from "@opencode-ai/app/updater"

export type { UpdaterState } from "@opencode-ai/app/updater"

export type UpdaterReadyRecord = { version: string }

export type ForkVersion = `${number}.${number}.${number}-fork.${number}`

const forkVersionPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)-fork\.([1-9]\d*)$/

export function requireNewerVersion(candidate: string, current: string) {
  const next = candidate.match(forkVersionPattern)?.slice(1).map(BigInt)
  const installed = current.match(forkVersionPattern)?.slice(1).map(BigInt)
  if (!next || !installed) return false
  const difference = next.findIndex((value, index) => value !== installed[index])
  if (difference === -1) return false
  return next[difference]! > installed[difference]!
}

export function isUpdaterEnabled(input: {
  packaged: boolean
  channel: "dev" | "beta" | "prod"
  updatesDisabled: boolean
  forkUpdate: boolean
  platform: NodeJS.Platform
  currentVersion: string
}) {
  if (!input.packaged || input.channel === "dev" || input.updatesDisabled) return false
  if (!input.forkUpdate) return true
  if (input.platform !== "win32") return false
  return forkVersionPattern.test(input.currentVersion)
}

export type LocalServerConnection = {
  url: string
  username: string | null
  password: string | null
}

export async function prepareServerShutdown(connection: LocalServerConnection, fetcher: typeof fetch = fetch) {
  const headers = new Headers()
  if (connection.username !== null && connection.password !== null)
    headers.set("authorization", `Basic ${btoa(`${connection.username}:${connection.password}`)}`)
  const response = await fetcher(new URL("/global/project/delete/prepare-shutdown", connection.url), {
    method: "POST",
    headers,
  })
  if (response.ok) return
  const body = (await response.json().catch(() => undefined)) as { message?: unknown } | undefined
  const message = typeof body?.message === "string" ? body.message : `Shutdown preparation failed (${response.status})`
  throw new Error(message)
}

export async function installWithErrorSurface(
  install: () => Promise<void>,
  surface: (message: string) => Promise<void>,
) {
  try {
    await install()
  } catch (error) {
    await surface(error instanceof Error ? error.message : String(error))
  }
}

export type UpdaterBackend = {
  checkForUpdates(): Promise<{ isUpdateAvailable?: boolean; updateInfo?: { version?: string } } | null | undefined>
  downloadUpdate(): Promise<unknown>
  quitAndInstall(): void
}

type UpdaterPersistence = {
  get(): UpdaterReadyRecord | undefined | Promise<UpdaterReadyRecord | undefined>
  set(value: UpdaterReadyRecord): void | Promise<void>
  clear(): void | Promise<void>
}

export function createUpdaterController(input: {
  enabled: boolean
  forkUpdate: boolean
  currentVersion: string
  backend: UpdaterBackend
  persistence: UpdaterPersistence
  prepareShutdown: () => Promise<void>
  stop: () => Promise<void>
  log?: (message: string, data?: object) => void
}) {
  let state: UpdaterState = input.enabled ? { status: "idle" } : { status: "disabled" }
  let pending: Promise<UpdaterState> | undefined
  const listeners = new Set<(state: UpdaterState) => void>()

  const transition = (next: UpdaterState) => {
    input.log?.("updater state changed", { from: state.status, to: next.status })
    state = next
    listeners.forEach((listener) => listener(state))
    return state
  }

  const check = () => {
    if (!input.enabled) return Promise.resolve(state)
    if (state.status === "ready") return Promise.resolve(state)
    if (pending) return pending

    pending = (async () => {
      transition({ status: "checking" })
      const result = await input.backend.checkForUpdates()
      const version = result?.updateInfo?.version
      if (
        !result?.isUpdateAvailable ||
        !version ||
        version === input.currentVersion ||
        (input.forkUpdate && !requireNewerVersion(version, input.currentVersion))
      ) {
        await input.persistence.clear()
        return transition({ status: "up-to-date" })
      }

      transition({ status: "downloading", version })
      await input.backend.downloadUpdate()
      await input.persistence.set({ version })
      return transition({ status: "ready", version })
    })()
      .catch((error) =>
        transition({ status: "error", message: error instanceof Error ? error.message : String(error) }),
      )
      .finally(() => {
        pending = undefined
      })
    return pending
  }

  return {
    getState: () => state,
    subscribe(listener: (state: UpdaterState) => void) {
      listeners.add(listener)
      listener(state)
      return () => listeners.delete(listener)
    },
    async start() {
      const ready = await input.persistence.get()
      if (ready?.version === input.currentVersion) await input.persistence.clear()
      return check()
    },
    check,
    async install() {
      if (state.status !== "ready") throw new Error("Update is not ready to install")
      const version = state.version
      transition({ status: "installing", version })
      await input
        .prepareShutdown()
        .then(input.stop)
        .then(() => {
          input.backend.quitAndInstall()
          transition({ status: "ready", version })
        })
        .catch((error) => {
          transition({ status: "ready", version })
          throw error
        })
    },
  }
}

export type UpdaterController = ReturnType<typeof createUpdaterController>
