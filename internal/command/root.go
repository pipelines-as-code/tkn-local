package command

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pipelines-as-code/tkn-local/internal/cluster"
	"github.com/pipelines-as-code/tkn-local/internal/iostreams"
	"github.com/pipelines-as-code/tkn-local/pkg/localexecution"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/apimachinery/pkg/api/resource"
)

func New(streams *iostreams.IOStreams) *cobra.Command {
	connection := &cluster.Options{}
	root := &cobra.Command{
		Use:          "tkn-local",
		Short:        "Run Tekton pipelines using your local working tree",
		SilenceUsage: true,
	}
	root.SetIn(streams.In)
	root.SetOut(streams.Out)
	root.SetErr(streams.ErrOut)
	root.PersistentFlags().StringVar(&connection.Kubeconfig, "kubeconfig", "", "Path to kubeconfig (defaults to KUBECONFIG or ~/.kube/config)")
	root.PersistentFlags().StringVar(&connection.Context, "context", "", "Kubeconfig context (defaults to the current context)")
	root.PersistentFlags().StringVarP(&connection.Namespace, "namespace", "n", "", "Namespace (defaults to the selected context's namespace)")
	root.AddCommand(runCommand(connection, streams))
	return root
}

func runCommand(connection *cluster.Options, streams *iostreams.IOStreams) *cobra.Command {
	var filename, source, workspace, storageSize string
	var parameters []string
	var yes bool
	options := localexecution.RunOptions{}
	command := &cobra.Command{
		Use:   "run",
		Short: "Upload local source and run one PipelineRun on your Tekton cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) (returnErr error) {
			if filename == "" {
				return fmt.Errorf("--file is required")
			}
			if workspace == "" {
				return fmt.Errorf("--workspace must not be empty")
			}
			if options.Timeout <= 0 {
				return fmt.Errorf("--timeout must be positive")
			}
			quantity, err := resource.ParseQuantity(storageSize)
			if err != nil || quantity.Sign() <= 0 {
				return fmt.Errorf("--storage-size must be a positive Kubernetes storage quantity")
			}
			options.StorageSize = quantity
			values, err := parseParams(parameters)
			if err != nil {
				return err
			}
			input, interactive := streams.In.(*os.File)
			interactive = interactive && term.IsTerminal(int(input.Fd()))
			if !yes && !interactive {
				return fmt.Errorf("noninteractive local execution requires --yes to acknowledge source upload and the displayed target")
			}
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			prepareCtx, cancelPrepare := context.WithTimeout(ctx, 5*time.Minute)
			defer cancelPrepare()
			binary, err := localexecution.CheckTkn(prepareCtx)
			if err != nil {
				return err
			}
			snapshot, err := localexecution.Capture(prepareCtx, source, quantity.Value())
			if err != nil {
				return err
			}
			defer func() { returnErr = errors.Join(returnErr, snapshot.Close()) }()
			clients, err := cluster.New(connection)
			if err != nil {
				return err
			}
			prepared, err := localexecution.Resolve(prepareCtx, snapshot, filename, connection.Namespace, workspace, values)
			if err != nil {
				return err
			}
			options.Namespace = connection.Namespace
			options.Context = connection.Context
			options.Kubeconfig = connection.Kubeconfig
			execution, err := localexecution.Prepare(prepareCtx, clients, streams, snapshot, prepared, options, binary)
			if err != nil {
				return err
			}
			execution.Preview()
			if !yes {
				fmt.Fprint(streams.ErrOut, "Upload this source and execute this PipelineRun? [y/N] ")
				answer := make(chan bool, 1)
				go func() {
					line, err := bufio.NewReader(streams.In).ReadString('\n')
					answer <- err == nil && (strings.EqualFold(strings.TrimSpace(line), "y") || strings.EqualFold(strings.TrimSpace(line), "yes"))
				}()
				select {
				case accepted := <-answer:
					if !accepted {
						return fmt.Errorf("local execution declined")
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			cancelPrepare()
			return execution.Run(ctx)
		},
	}
	command.Flags().StringVarP(&filename, "file", "f", "", "YAML file containing exactly one PipelineRun inside the source root")
	command.Flags().StringVar(&source, "source", ".", "Git working tree root to upload")
	command.Flags().StringVar(&workspace, "workspace", "source", "Pipeline workspace that receives local source")
	command.Flags().StringArrayVarP(&parameters, "param", "p", nil, "Tekton string parameter key=value (repeatable)")
	command.Flags().BoolVar(&yes, "yes", false, "Confirm source upload and task execution without prompting")
	command.Flags().StringVar(&storageSize, "storage-size", "1Gi", "Temporary source volume capacity")
	command.Flags().StringVar(&options.StorageClass, "storage-class", "", "StorageClass (defaults to the cluster default)")
	command.Flags().StringVar(&options.TektonNamespace, "tekton-namespace", "", "Tekton installation namespace (defaults to openshift-pipelines on OpenShift, tekton-pipelines elsewhere)")
	command.Flags().DurationVar(&options.Timeout, "timeout", time.Hour, "Maximum pipeline execution time")
	command.Flags().BoolVar(&options.KeepResources, "keep-resources", false, "Keep the run and uploaded source for debugging")
	return command
}

func parseParams(parameters []string) (map[string]string, error) {
	values := map[string]string{}
	for _, parameter := range parameters {
		key, value, ok := strings.Cut(parameter, "=")
		if !ok || strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("invalid --param %q: expected key=value", parameter)
		}
		values[key] = value
	}
	return values, nil
}
