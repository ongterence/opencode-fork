import { expect, test } from "bun:test"
import type { Configuration } from "electron-builder"

const legacyDesktopEntry = "resources/linux/opencode-desktop.desktop"

const channels = [
  { channel: "dev", appId: "ai.opencode.desktop.dev" },
  { channel: "beta", appId: "ai.opencode.desktop.beta" },
  { channel: "prod", appId: "ai.opencode.desktop" },
] as const

for (const channel of channels) {
  test(`uses one Linux desktop identity for ${channel.channel}`, async () => {
    const previous = process.env.OPENCODE_CHANNEL
    process.env.OPENCODE_CHANNEL = channel.channel

    const module = await import(`./electron-builder.config.ts?channel=${channel.channel}`)
    const config = module.default as Configuration

    if (previous === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previous

    expect(config.appId).toBe(channel.appId)
    expect(config.extraMetadata?.desktopName).toBe(`${channel.appId}.desktop`)
    expect(config.linux?.executableName).toBe(channel.appId)
    expect(config.linux?.desktop?.entry?.StartupWMClass).toBe(channel.appId)
    expect(config.deb?.fpm).toContainEqual(expect.stringContaining(`/usr/share/metainfo/${channel.appId}.metainfo.xml`))
    expect(config.rpm?.fpm).toContainEqual(expect.stringContaining(`/usr/share/metainfo/${channel.appId}.metainfo.xml`))
  })
}

test("keeps a hidden prod launcher for old Linux pins", async () => {
  const previous = process.env.OPENCODE_CHANNEL
  process.env.OPENCODE_CHANNEL = "prod"

  const module = await import("./electron-builder.config.ts?compat=prod")
  const config = module.default as Configuration

  if (previous === undefined) delete process.env.OPENCODE_CHANNEL
  else process.env.OPENCODE_CHANNEL = previous

  expect(
    config.deb?.fpm?.some((entry) =>
      entry.endsWith("opencode-desktop.desktop=/usr/share/applications/opencode-desktop.desktop"),
    ),
  ).toBe(true)
  expect(
    config.rpm?.fpm?.some((entry) =>
      entry.endsWith("opencode-desktop.desktop=/usr/share/applications/opencode-desktop.desktop"),
    ),
  ).toBe(true)

  const desktop = await Bun.file(legacyDesktopEntry).text()
  expect(desktop).toContain("Exec=/opt/OpenCode/ai.opencode.desktop %U")
  expect(desktop).toContain("Icon=ai.opencode.desktop")
  expect(desktop).toContain("StartupWMClass=ai.opencode.desktop")
  expect(desktop).toContain("NoDisplay=true")
})

test("bundles the CLI outside the dev app archive", async () => {
  const previous = process.env.OPENCODE_CHANNEL
  process.env.OPENCODE_CHANNEL = "dev"
  const module = await import("./electron-builder.config.ts?cli-resource")
  const config = module.default as Configuration
  if (previous === undefined) delete process.env.OPENCODE_CHANNEL
  else process.env.OPENCODE_CHANNEL = previous

  expect(config.files).toContain("!resources/opencode-cli*")
  expect(config.extraResources).toContainEqual({
    from: "resources/",
    to: "",
    filter: ["opencode-cli*"],
  })
})

test("rejects GitHub Actions release packaging without a Windows publisher", async () => {
  const previousChannel = process.env.OPENCODE_CHANNEL
  const previousGithubActions = process.env.GITHUB_ACTIONS
  const previousPublisherName = process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
  const previousForkUpdate = process.env.OPENCODE_FORK_UPDATE
  process.env.OPENCODE_CHANNEL = "prod"
  process.env.GITHUB_ACTIONS = "true"
  delete process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
  delete process.env.OPENCODE_FORK_UPDATE

  try {
    const config = import("./electron-builder.config.ts?missing-release-publisher")
    if (process.platform === "win32") {
      await expect(config).rejects.toThrow("Release packaging requires OPENCODE_WINDOWS_PUBLISHER_NAME")
    } else {
      await expect(config).resolves.toBeDefined()
    }
  } finally {
    if (previousChannel === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previousChannel
    if (previousGithubActions === undefined) delete process.env.GITHUB_ACTIONS
    else process.env.GITHUB_ACTIONS = previousGithubActions
    if (previousPublisherName === undefined) delete process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
    else process.env.OPENCODE_WINDOWS_PUBLISHER_NAME = previousPublisherName
    if (previousForkUpdate === undefined) delete process.env.OPENCODE_FORK_UPDATE
    else process.env.OPENCODE_FORK_UPDATE = previousForkUpdate
  }
})

test("writes the approved Windows publisher into release update metadata", async () => {
  const previousChannel = process.env.OPENCODE_CHANNEL
  const previousGithubActions = process.env.GITHUB_ACTIONS
  const previousPublisherName = process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
  const previousForkUpdate = process.env.OPENCODE_FORK_UPDATE
  process.env.OPENCODE_CHANNEL = "prod"
  process.env.GITHUB_ACTIONS = "true"
  process.env.OPENCODE_WINDOWS_PUBLISHER_NAME = "CN=OpenCode Test"
  delete process.env.OPENCODE_FORK_UPDATE

  try {
    const module = await import("./electron-builder.config.ts?release-publisher-name")
    const config = module.default as Configuration

    expect(config.win?.signtoolOptions?.publisherName).toEqual(["CN=OpenCode Test"])
    expect(config.win?.verifyUpdateCodeSignature).toBe(true)
  } finally {
    if (previousChannel === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previousChannel
    if (previousGithubActions === undefined) delete process.env.GITHUB_ACTIONS
    else process.env.GITHUB_ACTIONS = previousGithubActions
    if (previousPublisherName === undefined) delete process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
    else process.env.OPENCODE_WINDOWS_PUBLISHER_NAME = previousPublisherName
    if (previousForkUpdate === undefined) delete process.env.OPENCODE_FORK_UPDATE
    else process.env.OPENCODE_FORK_UPDATE = previousForkUpdate
  }
})

test("allows unsigned fork packaging on GitHub Actions without a publisher", async () => {
  const previousChannel = process.env.OPENCODE_CHANNEL
  const previousGithubActions = process.env.GITHUB_ACTIONS
  const previousForkUpdate = process.env.OPENCODE_FORK_UPDATE
  const previousForkStage = process.env.OPENCODE_FORK_PACKAGE_STAGE
  const previousWorkflowRef = process.env.GITHUB_WORKFLOW_REF
  const previousPublisherName = process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
  process.env.OPENCODE_CHANNEL = "prod"
  process.env.GITHUB_ACTIONS = "true"
  process.env.OPENCODE_FORK_UPDATE = "true"
  process.env.OPENCODE_FORK_PACKAGE_STAGE = "unsigned"
  process.env.GITHUB_WORKFLOW_REF = "ongterence/opencode-fork/.github/workflows/fork-update.yml@refs/heads/main"
  delete process.env.OPENCODE_WINDOWS_PUBLISHER_NAME

  try {
    const module = await import("./electron-builder.config.ts?fork-unsigned-policy")
    const config = module.default as Configuration

    expect(config.win?.verifyUpdateCodeSignature).toBe(false)
    expect(config.win?.signtoolOptions?.publisherName).toBeUndefined()
  } finally {
    if (previousChannel === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previousChannel
    if (previousGithubActions === undefined) delete process.env.GITHUB_ACTIONS
    else process.env.GITHUB_ACTIONS = previousGithubActions
    if (previousForkUpdate === undefined) delete process.env.OPENCODE_FORK_UPDATE
    else process.env.OPENCODE_FORK_UPDATE = previousForkUpdate
    if (previousForkStage === undefined) delete process.env.OPENCODE_FORK_PACKAGE_STAGE
    else process.env.OPENCODE_FORK_PACKAGE_STAGE = previousForkStage
    if (previousWorkflowRef === undefined) delete process.env.GITHUB_WORKFLOW_REF
    else process.env.GITHUB_WORKFLOW_REF = previousWorkflowRef
    if (previousPublisherName === undefined) delete process.env.OPENCODE_WINDOWS_PUBLISHER_NAME
    else process.env.OPENCODE_WINDOWS_PUBLISHER_NAME = previousPublisherName
  }
})

for (const channel of ["beta", "prod"] as const) {
  test(`does not bundle the CLI in ${channel} builds`, async () => {
    const previous = process.env.OPENCODE_CHANNEL
    process.env.OPENCODE_CHANNEL = channel
    const module = await import(`./electron-builder.config.ts?no-cli-resource=${channel}`)
    const config = module.default as Configuration
    if (previous === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previous

    expect(config.extraResources).not.toContainEqual({
      from: "resources/",
      to: "",
      filter: ["opencode-cli*"],
    })
  })
}
