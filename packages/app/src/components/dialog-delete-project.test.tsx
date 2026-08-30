import { describe, expect, test } from "bun:test"
import { createDeleteProjectDialogController as createController } from "./dialog-delete-project"

describe("delete project dialog", () => {
  test("enables the delete button immediately in the confirming state", () => {
    const controller = createController({
      remove: async () => {},
      retry: async () => {},
      onDeleted: () => {},
      close: () => {},
      focusRetry: () => {},
      onFailure: () => {},
    })

    expect(controller.state()).toBe("confirming")
    expect(controller.canDelete()).toBe(true)
  })

  test("blocks dismissal and duplicate submission while deletion is active", async () => {
    let resolve!: () => void
    const pending = new Promise<void>((done) => {
      resolve = done
    })
    let deleted = 0
    let closed = 0
    const controller = createController({
      remove: () => pending,
      retry: async () => {},
      onDeleted: () => deleted++,
      close: () => closed++,
      focusRetry: () => {},
      onFailure: () => {},
    })

    const first = controller.submit()
    const duplicate = controller.submit()
    expect(controller.state()).toBe("deleting")
    expect(controller.canDismiss()).toBe(false)
    expect(controller.canDelete()).toBe(false)

    resolve()
    await Promise.all([first, duplicate])
    expect(deleted).toBe(1)
    expect(closed).toBe(1)
  })

  test("focuses Retry for a typed retryable conflict and completes through retry", async () => {
    let focused = 0
    let retried = 0
    let deleted = 0
    const controller = createController({
      remove: async () => {
        throw {
          _tag: "ProjectDeletionRetryableError",
          projectID: "project-1",
          code: "project_deletion_retryable",
          phase: "share_failed",
          retry: true,
          message: "Remote share revocation is still required",
        }
      },
      retry: async () => {
        retried++
      },
      onDeleted: () => deleted++,
      close: () => {},
      focusRetry: () => focused++,
      onFailure: () => {},
    })

    await controller.submit()
    expect(controller.state()).toBe("retryable_error")
    expect(controller.failure()).toBe("Remote share revocation is still required")
    expect(controller.canDismiss()).toBe(true)
    expect(focused).toBe(1)

    await controller.submit()
    expect(retried).toBe(1)
    expect(deleted).toBe(1)
  })

  test("unwraps an SDK-wrapped retryable conflict", async () => {
    let focused = 0
    const controller = createController({
      remove: async () => {
        throw new Error("Project deletion is retryable", {
          cause: {
            body: {
              _tag: "ProjectDeletionRetryableError",
              projectID: "project-1",
              code: "project_deletion_retryable",
              phase: "share_failed",
              retry: true,
              message: "Remote share revocation is still required",
            },
            status: 409,
          },
        })
      },
      retry: async () => {},
      onDeleted: () => {},
      close: () => {},
      focusRetry: () => focused++,
      onFailure: () => {},
    })

    await controller.submit()
    expect(controller.state()).toBe("retryable_error")
    expect(controller.failure()).toBe("Remote share revocation is still required")
    expect(focused).toBe(1)
  })

  const rejectedContracts = [
    [
      "wrong error tag",
      {
        _tag: "ProjectDeletionInProgressError",
        projectID: "project-1",
        code: "project_deletion_retryable",
        phase: "share_failed",
        retry: true,
      },
    ],
    [
      "missing error tag",
      {
        projectID: "project-1",
        code: "project_deletion_retryable",
        phase: "share_failed",
        retry: true,
      },
    ],
    [
      "empty project ID",
      {
        _tag: "ProjectDeletionRetryableError",
        projectID: "",
        code: "project_deletion_retryable",
        phase: "share_failed",
        retry: true,
      },
    ],
    [
      "wrong HTTP status",
      new Error("Project deletion conflict", {
        cause: {
          body: {
            _tag: "ProjectDeletionRetryableError",
            projectID: "project-1",
            code: "project_deletion_retryable",
            phase: "share_failed",
            retry: true,
          },
          status: 500,
        },
      }),
    ],
  ] as const

  for (const [name, error] of rejectedContracts) {
    test(`rejects a retryable-looking value with ${name}`, async () => {
      let failed = 0
      let focused = 0
      const controller = createController({
        remove: async () => {
          throw error
        },
        retry: async () => {},
        onDeleted: () => {},
        close: () => {},
        focusRetry: () => focused++,
        onFailure: () => failed++,
      })

      await controller.submit()
      expect(controller.state()).toBe("confirming")
      expect(failed).toBe(1)
      expect(focused).toBe(0)
    })
  }

  test("does not offer Retry for a conflict outside share_failed", async () => {
    let failed = 0
    let focused = 0
    const controller = createController({
      remove: async () => {
        throw {
          _tag: "ProjectDeletionRetryableError",
          projectID: "project-1",
          code: "project_deletion_retryable",
          phase: "deleting_local_data",
          retry: true,
        }
      },
      retry: async () => {},
      onDeleted: () => {},
      close: () => {},
      focusRetry: () => focused++,
      onFailure: () => failed++,
    })

    await controller.submit()
    expect(controller.state()).toBe("confirming")
    expect(failed).toBe(1)
    expect(focused).toBe(0)
  })
})
