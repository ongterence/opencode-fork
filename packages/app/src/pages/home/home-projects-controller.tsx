import { useDirectoryPicker } from "@/components/directory-picker"
import { useServerManagementController } from "@/components/dialog-select-server"
import { useSettingsCommand } from "@/components/settings-dialog"
import { DialogServerV2 } from "@/components/settings-v2/dialog-server-v2"
import { type LocalProject } from "@/context/layout"
import { useLanguage } from "@/context/language"
import { useNotification } from "@/context/notification"
import { usePlatform } from "@/context/platform"
import { ServerConnection } from "@/context/server"
import { closeHomeProject, errorMessage, homeProjectDirectories } from "@/pages/layout/helpers"
import { pathKey } from "@/utils/path-key"
import { Persist, persisted } from "@/utils/persist"
import { showToast } from "@/utils/toast"
import { useDialog } from "@opencode-ai/ui/context/dialog"
import { createEffect, createResource } from "solid-js"
import { createStore } from "solid-js/store"
import type { HomeController } from "./home-controller"
import type { HomeProjectSelection } from "@/context/layout"
import { findProjectDirectoryOwner, projectOwnsDirectory } from "@/context/server-sync"

type DeleteProjectClientResult = { worktree: string; projectID: string }

export async function completeProjectDelete(input: {
  request: () => Promise<void>
  cleanup: (project: DeleteProjectClientResult) => string[]
  project: DeleteProjectClientResult
  serverKey: ServerConnection.Key
  selection: () => HomeProjectSelection
  setSelection: (next: HomeProjectSelection) => void
}) {
  await input.request()
  const roots = input.cleanup(input.project)
  resetDeletedProjectSelection({ ...input, roots })
}

function resetDeletedProjectSelection(input: {
  roots: string[]
  serverKey: ServerConnection.Key
  selection: () => HomeProjectSelection
  setSelection: (next: HomeProjectSelection) => void
}) {
  const selection = input.selection()
  if (selection.server !== input.serverKey) return
  if (!input.roots.some((root) => projectOwnsDirectory(root, selection.directory ?? ""))) return
  input.setSelection({ server: input.serverKey })
}

export function createHomeProjectsController(home: HomeController) {
  const platform = usePlatform()
  const pickDirectory = useDirectoryPicker()
  const dialog = useDialog()
  const language = useLanguage()
  const notification = useNotification()
  const openSettings = useSettingsCommand()
  const serverManagement = useServerManagementController({ navigateOnAdd: false })
  const [_state, setState, _, ready] = persisted(
    Persist.global("home.servers", ["home.servers.v1"]),
    createStore({ collapsed: {} as Record<string, boolean> }),
  )
  const [state] = createResource(
    () => ready.promise ?? Promise.resolve(),
    (promise) => promise.then(() => _state),
    { initialValue: _state },
  )
  function directories(project: LocalProject) {
    return [project.worktree, ...(project.sandboxes ?? [])]
  }

  function canRevealProject(conn: ServerConnection.Any) {
    return platform.platform === "desktop" && !!platform.openPath && ServerConnection.local(conn)
  }

  createEffect(() => {
    const selection = home.selection.value()
    const directory = selection.directory
    if (!directory) return
    const conn = home.server.list().find((candidate) => ServerConnection.key(candidate) === selection.server)
    if (!conn) return
    const ctx = home.server.context(conn)
    if (!ctx.sync.data.ready) return
    if (ctx.projects.list().some((project) => pathKey(project.worktree) === pathKey(directory))) return
    home.selection.set({ server: selection.server })
  })

  return {
    copy: {
      language,
    },
    selection: {
      value: home.selection.value,
    },
    server: {
      list: home.server.list,
      health: home.server.health,
      projects: home.project.forServer,
      collapsed: (conn: ServerConnection.Any) => state().collapsed[ServerConnection.key(conn)] ?? false,
      toggleCollapsed: (conn: ServerConnection.Any) => {
        const key = ServerConnection.key(conn)
        setState("collapsed", key, !state().collapsed[key])
      },
      canDefault: serverManagement.canDefault,
      defaultKey: serverManagement.defaultKey,
      setDefault: (conn: ServerConnection.Any | undefined) =>
        serverManagement.setDefault(conn ? ServerConnection.key(conn) : null),
      remove: (conn: ServerConnection.Any) => serverManagement.handleRemove(ServerConnection.key(conn)),
      edit: (conn: ServerConnection.Http) => dialog.show(() => <DialogServerV2 mode="edit" server={conn} />),
      focus: home.selection.focusServer,
    },
    project: {
      list: home.project.list,
      recentlyClosed: home.project.recentlyClosed,
      homedir: home.project.homedir,
      select: home.project.select,
      add: home.project.add,
      openNewSession: home.project.openProjectNewSession,
      edit: (conn: ServerConnection.Any, project: LocalProject) => {
        void import("@/components/dialog-edit-project-v2").then(({ DialogEditProjectV2 }) => {
          void dialog.show(() => <DialogEditProjectV2 server={conn} project={project} />)
        })
      },
      unseenCount: (conn: ServerConnection.Any, project: LocalProject) => {
        const state = notification.ensureServerState(ServerConnection.key(conn))
        return directories(project).reduce((total, directory) => total + state.project.unseenCount(directory), 0)
      },
      clearNotifications: (conn: ServerConnection.Any, project: LocalProject) => {
        const state = notification.ensureServerState(ServerConnection.key(conn))
        directories(project)
          .filter((directory) => state.project.unseenCount(directory) > 0)
          .forEach((directory) => state.project.markViewed(directory))
      },
      choose: (conn: ServerConnection.Any) => {
        if (home.server.health(conn)?.healthy === false) return
        pickDirectory({
          server: conn,
          title: language.t("command.project.open"),
          multiple: true,
          onSelect: (result) => home.project.add(conn, homeProjectDirectories(result)),
        })
      },
      close: (conn: ServerConnection.Any, directory: string) => {
        const next = closeHomeProject(
          home.selection.value(),
          ServerConnection.key(conn),
          home.server.context(conn).projects,
          directory,
        )
        if (next) home.selection.set(next)
      },
      delete: (conn: ServerConnection.Any, project: LocalProject) => {
        const ctx = home.server.context(conn)
        const projectID = project.id
        if (projectID === "global") return
        if (!projectID) {
          // Stale row: the server no longer knows this project (its data was
          // already purged, e.g. after a lost response). Only clean it up once
          // the catalog has loaded and confirms the project is really gone,
          // otherwise an in-flight catalog would make healthy rows vanish.
          if (!ctx.sync.data.ready) return
          const known = findProjectDirectoryOwner(ctx.sync.data.project, project.worktree)
          if (known) return
          const roots = ctx.cleanupProject({ projectID: "", worktree: project.worktree })
          resetDeletedProjectSelection({
            roots,
            serverKey: ServerConnection.key(conn),
            selection: home.selection.value,
            setSelection: home.selection.set,
          })
          return
        }
        void import("@/components/dialog-delete-project").then(({ DialogDeleteProject }) => {
          void dialog.show(() => (
            <DialogDeleteProject
              name={project.name ?? project.worktree}
              remove={async () => {
                await completeProjectDelete({
                  request: async () => {
                    await ctx.sdk.client.global.project.delete({ projectID })
                  },
                  cleanup: ctx.cleanupProject,
                  project: { projectID, worktree: project.worktree },
                  serverKey: ServerConnection.key(conn),
                  selection: home.selection.value,
                  setSelection: home.selection.set,
                })
              }}
              onDeleted={() => {}}
            />
          ))
        })
      },
      move: (conn: ServerConnection.Any, worktree: string, index: number) => {
        home.server.context(conn).projects.move(worktree, index)
      },
      canReveal: canRevealProject,
      reveal: (conn: ServerConnection.Any, project: LocalProject) => {
        if (!platform.openPath || !canRevealProject(conn)) return
        platform.openPath(project.worktree).catch((cause: unknown) =>
          showToast({
            title: language.t("common.requestFailed"),
            description: errorMessage(cause, language.t("common.requestFailed")),
          }),
        )
      },
    },
    utility: {
      settings: openSettings,
      help: () => platform.openExternal("https://opencode.ai/desktop-feedback"),
    },
  }
}

export type HomeProjectsController = ReturnType<typeof createHomeProjectsController>
