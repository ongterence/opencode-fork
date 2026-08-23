import { ButtonV2 } from "@opencode-ai/ui/v2/button-v2"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { DialogBody, DialogFooter, DialogHeader, DialogTitleGroup, DialogV2 } from "@opencode-ai/ui/v2/dialog-v2"
import { TextInputV2 } from "@opencode-ai/ui/v2/text-input-v2"
import { createEffect, createSignal, onCleanup, Show } from "solid-js"
import { useLanguage } from "@/context/language"
import { errorMessage } from "@/pages/layout/helpers"
import { showToast } from "@/utils/toast"

export type DeleteDialogState = "confirming" | "deleting" | "retryable_error"

type RetryableDeleteError = { message?: unknown }

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function extractRetryableDeleteError(error: unknown): RetryableDeleteError | undefined {
  let value = error
  if (error instanceof Error) {
    if (!record(error.cause) || error.cause.status !== 409) return
    value = error.cause.body
  }
  if (!record(value)) return
  if (
    value._tag !== "ProjectDeletionRetryableError" ||
    typeof value.projectID !== "string" ||
    value.projectID.length === 0 ||
    value.code !== "project_deletion_retryable" ||
    value.phase !== "share_failed" ||
    value.retry !== true
  ) {
    return
  }
  return value
}

export function createDeleteProjectDialogController(input: {
  remove: () => Promise<void>
  retry: () => Promise<void>
  onDeleted: () => void
  close: () => void
  focusRetry: () => void
  onFailure: (error: unknown) => void
}) {
  const [state, setState] = createSignal<DeleteDialogState>("confirming")
  const [acknowledgement, setAcknowledgementValue] = createSignal("")
  const [failure, setFailure] = createSignal<string>()

  const canDelete = () => state() === "retryable_error" || (state() === "confirming" && acknowledgement() === "DELETE")
  const canDismiss = () => state() !== "deleting"

  const submit = async () => {
    const current = state()
    if (current === "deleting") return
    if (current === "confirming" && acknowledgement() !== "DELETE") return
    setState("deleting")
    try {
      await (current === "retryable_error" ? input.retry() : input.remove())
    } catch (error) {
      const retryable = extractRetryableDeleteError(error)
      if (!retryable) {
        setState("confirming")
        input.onFailure(error)
        return
      }
      setFailure(typeof retryable.message === "string" ? retryable.message : undefined)
      setState("retryable_error")
      input.focusRetry()
      return
    }
    input.onDeleted()
    input.close()
  }

  return {
    state,
    acknowledgement,
    failure,
    setAcknowledgement(value: string) {
      if (state() === "deleting") return
      setAcknowledgementValue(value)
    },
    canDelete,
    canDismiss,
    submit,
  }
}

export function DialogDeleteProject(props: {
  name: string
  remove: () => Promise<void>
  retry: () => Promise<void>
  onDeleted: () => void
}) {
  const dialog = useDialog()
  const language = useLanguage()
  let retryButton: HTMLButtonElement | undefined

  const controller = createDeleteProjectDialogController({
    remove: props.remove,
    retry: props.retry,
    onDeleted: props.onDeleted,
    close: () => {
      dialog.setCloseBlocked(false)
      dialog.close()
    },
    focusRetry: () => queueMicrotask(() => retryButton?.focus()),
    onFailure: (error) =>
      showToast({
        title: language.t("project.delete.failed.title"),
        description: errorMessage(error, language.t("common.requestFailed")),
      }),
  })

  createEffect(() => dialog.setCloseBlocked(!controller.canDismiss()))
  onCleanup(() => dialog.setCloseBlocked(false))

  const cancel = () => {
    if (!controller.canDismiss()) return
    dialog.close()
  }

  return (
    <DialogV2 fit>
      <div aria-busy={controller.state() === "deleting" ? "true" : undefined}>
        <DialogHeader hideClose>
          <DialogTitleGroup
            title={language.t("project.delete.title")}
            description={language.t("project.delete.confirm", { name: props.name })}
          />
        </DialogHeader>
        <DialogBody class="flex flex-col gap-3">
          <Show when={controller.state() !== "retryable_error"}>
            <label class="flex flex-col gap-1 text-12-regular text-text-strong">
              <span>{language.t("project.delete.acknowledge")}</span>
              <TextInputV2
                autofocus
                aria-label={language.t("project.delete.acknowledge")}
                value={controller.acknowledgement()}
                disabled={controller.state() === "deleting"}
                onInput={(event) => controller.setAcknowledgement(event.currentTarget.value)}
              />
            </label>
          </Show>
          <Show when={controller.state() === "deleting"}>
            <div role="status" aria-live="polite">
              {language.t("project.delete.progress")}
            </div>
          </Show>
          <Show when={controller.state() === "retryable_error"}>
            <div role="alert">
              <div>{language.t("project.delete.retryable")}</div>
              <Show when={controller.failure()}>{(message) => <div>{message()}</div>}</Show>
            </div>
          </Show>
        </DialogBody>
        <DialogFooter>
          <ButtonV2 variant="ghost" disabled={!controller.canDismiss()} onClick={cancel}>
            {language.t("common.cancel")}
          </ButtonV2>
          <Show
            when={controller.state() === "retryable_error"}
            fallback={
              <ButtonV2 variant="danger" disabled={!controller.canDelete()} onClick={() => void controller.submit()}>
                {language.t("project.delete.button")}
              </ButtonV2>
            }
          >
            <ButtonV2
              ref={(element: HTMLButtonElement) => (retryButton = element)}
              variant="danger"
              onClick={() => void controller.submit()}
            >
              {language.t("project.delete.retry")}
            </ButtonV2>
          </Show>
        </DialogFooter>
      </div>
    </DialogV2>
  )
}
