import { ButtonV2 } from "@opencode-ai/ui/v2/button-v2"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { DialogFooter, DialogHeader, DialogTitleGroup, DialogV2 } from "@opencode-ai/ui/v2/dialog-v2"
import { createSignal } from "solid-js"
import { useLanguage } from "@/context/language"
import { errorMessage } from "@/pages/layout/helpers"
import { showToast } from "@/utils/toast"

export function DialogDeleteProject(props: {
  name: string
  remove: () => Promise<void>
  onDeleted: () => void
}) {
  const dialog = useDialog()
  const language = useLanguage()
  const [busy, setBusy] = createSignal(false)

  const handleDelete = async () => {
    if (busy()) return
    setBusy(true)
    try {
      await props.remove()
    } catch (error) {
      showToast({
        title: language.t("project.delete.failed.title"),
        description: errorMessage(error, language.t("common.requestFailed")),
      })
      setBusy(false)
      return
    }
    props.onDeleted()
    dialog.close()
  }

  return (
    <DialogV2 fit>
      <DialogHeader hideClose>
        <DialogTitleGroup
          title={language.t("project.delete.title")}
          description={language.t("project.delete.confirm", { name: props.name })}
        />
      </DialogHeader>
      <DialogFooter>
        <ButtonV2 variant="ghost" onClick={() => dialog.close()}>
          {language.t("common.cancel")}
        </ButtonV2>
        <ButtonV2 variant="danger" disabled={busy()} onClick={handleDelete}>
          {language.t("project.delete.button")}
        </ButtonV2>
      </DialogFooter>
    </DialogV2>
  )
}
