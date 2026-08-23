import { existsSync } from "node:fs"
import { join } from "node:path"
import { app } from "electron"

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

export const UPDATER_ENABLED = app.isPackaged && CHANNEL !== "dev" && !existsSync(UPDATES_DISABLED_MARKER)
