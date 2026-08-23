import { describe, expect, test } from "bun:test"
import { installWithErrorSurface, prepareServerShutdown } from "./updater-controller"

describe("updater shutdown preparation", () => {
  test("calls the authenticated local deletion drain endpoint", async () => {
    const seen: Array<{ url: string; init?: RequestInit }> = []
    await prepareServerShutdown(
      { url: "http://127.0.0.1:4096", username: "opencode", password: "secret" },
      async (url, init) => {
        seen.push({ url: String(url), init })
        return new Response(null, { status: 204 })
      },
    )

    expect(seen).toHaveLength(1)
    expect(seen[0]?.url).toBe("http://127.0.0.1:4096/global/project/delete/prepare-shutdown")
    expect(seen[0]?.init?.method).toBe("POST")
    expect(new Headers(seen[0]?.init?.headers).get("authorization")).toBe(`Basic ${btoa("opencode:secret")}`)
  })

  test("surfaces the server message when the durable boundary is unavailable", async () => {
    await expect(
      prepareServerShutdown(
        { url: "http://127.0.0.1:4096", username: "opencode", password: "secret" },
        async () =>
          new Response(
            JSON.stringify({ message: "Project deletion could not reach a durable shutdown boundary" }),
            { status: 409, headers: { "content-type": "application/json" } },
          ),
      ),
    ).rejects.toThrow("Project deletion could not reach a durable shutdown boundary")
  })

  test("surfaces an install failure message and consumes the rejection", async () => {
    const messages: string[] = []

    await expect(
      installWithErrorSurface(
        async () => {
          throw new Error("Project deletion could not reach a durable shutdown boundary")
        },
        async (message) => {
          messages.push(message)
        },
      ),
    ).resolves.toBeUndefined()
    expect(messages).toEqual(["Project deletion could not reach a durable shutdown boundary"])
  })
})
