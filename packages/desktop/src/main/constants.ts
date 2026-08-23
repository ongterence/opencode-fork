import { existsSync, readFileSync } from "node:fs"
import { join } from "node:path"
import { app } from "electron"
import { isUpdaterEnabled } from "./updater-controller"

type Channel = "dev" | "beta" | "prod"
const raw = import.meta.env.OPENCODE_CHANNEL
export const CHANNEL: Channel = raw === "dev" || raw === "beta" || raw === "prod" ? raw : "dev"

export const APP_IDS: Record<Channel, string> = {
  dev: "ai.opencode.desktop.dev",
  beta: "ai.opencode.desktop.beta",
  prod: "ai.opencode.desktop",
}

// Empty marker file in the app-data directory keeps electron-updater idle so a
// locally built package is not auto-replaced by an official release.
const UPDATES_DISABLED_MARKER = join(app.getPath("appData"), APP_IDS[CHANNEL], ".disable-updates")

const forkUpdate = (() => {
  if (!app.isPackaged) return false
  try {
    const value = JSON.parse(readFileSync(join(app.getAppPath(), "package.json"), "utf8")) as { forkUpdate?: unknown }
    return value.forkUpdate === true
  } catch {
    return false
  }
})()

export const FORK_UPDATE = forkUpdate
export const UPDATER_ENABLED = isUpdaterEnabled({
  packaged: app.isPackaged,
  channel: CHANNEL,
  updatesDisabled: existsSync(UPDATES_DISABLED_MARKER),
  forkUpdate,
  platform: process.platform,
  currentVersion: app.getVersion(),
})
