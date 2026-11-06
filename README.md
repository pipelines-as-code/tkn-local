# tkn-local

Run a Tekton pipeline using your local files, including changes you haven't
committed or pushed. The pipeline runs on your Kubernetes cluster, with logs
streamed to your terminal.

## Install

You need [tkn](https://tekton.dev/docs/cli/), Git, and a Kubernetes cluster with
Tekton and persistent storage.

```sh
go install github.com/pipelines-as-code/tkn-local/cmd/tkn-local@latest
```

Put your Go binary directory on `PATH` to use it as a `tkn` plugin.

## Run

From your Git working tree root:

```sh
tkn local run --file examples/local.yaml
```

The command uses your current kubeconfig context and namespace. `--context` and
`--namespace/-n` are optional overrides. Use `--source` to select another working
tree root.

The Tekton installation namespace defaults to `openshift-pipelines` on OpenShift
and `tekton-pipelines` elsewhere. Use `--tekton-namespace` for a custom install.

Select a YAML file containing exactly one PipelineRun. Tasks and Pipelines can be
inline, in the same file, or referenced by name from definitions in `.tekton`.
The command runs the selected PipelineRun and leaves your YAML unchanged.

Your Pipeline must declare a workspace for the uploaded source and pass it to the
Tasks that read your files. The workspace defaults to `source`; use `--workspace`
for another name. Disable Git checkout so it doesn't replace your uploaded files.
The command configures shared workspace permissions automatically, using the
namespace's assigned IDs on OpenShift. Explicit user and group settings must be
compatible with the namespace policy. See [examples/local.yaml](examples/local.yaml).

Set Tekton string parameters with `-p`:

```sh
tkn local run --file examples/local.yaml -p message=hello
```

Each run includes tracked files and untracked files allowed by Git ignore rules.
Git metadata is excluded. Symlinks and submodules are unsupported. Task and
Pipeline references must resolve from local definitions; remote resolvers and
Custom Tasks are unsupported.

## Use the same PipelineRun in CI

[examples/git-or-local.yaml](examples/git-or-local.yaml) checks out Git by
default. Run it normally with:

```sh
kubectl create -f examples/git-or-local.yaml
```

Use your local files with the same PipelineRun:

```sh
tkn local run --file examples/git-or-local.yaml -p local=true
```

The `local` parameter skips the `git-clone` Task. Subsequent Tasks read the
`source` workspace in both modes. Set `url` and `revision` for your repository.
Tasks that consume checkout results, such as a commit SHA, need another source
for those values when checkout is skipped.

## Debug

Press Ctrl+C to cancel. The command removes its run and temporary storage when
finished. Keep them for inspection with:

```sh
tkn local run --file examples/local.yaml --keep-resources
```

The command prints inspection and cleanup commands. Use `--yes` to skip the
confirmation prompt in scripts. Tasks execute with their configured service
accounts and credentials, so use a development namespace.

The execution limit defaults to one hour; change it with `--timeout`. Temporary
storage defaults to `1Gi` using the cluster's default StorageClass. Override it
with `--storage-size` and `--storage-class`.

## Develop

```sh
make build
make test
```

`make e2e` runs against your current cluster, creates a temporary namespace, and
removes it afterward. It requires `tkn`, Tekton, and a default StorageClass.
