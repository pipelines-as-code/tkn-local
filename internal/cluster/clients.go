package cluster

import (
	"fmt"

	tekton "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Options struct {
	Kubeconfig string
	Context    string
	Namespace  string
}

type Clients struct {
	Kube   kubernetes.Interface
	Tekton tekton.Interface
	Config *rest.Config
}

func New(options *Options) (*Clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = options.Kubeconfig
	overrides := &clientcmd.ConfigOverrides{CurrentContext: options.Context}
	overrides.Context.Namespace = options.Namespace
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)
	config, err := loader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	namespace, _, err := loader.Namespace()
	if err != nil {
		return nil, fmt.Errorf("resolve namespace: %w", err)
	}
	raw, err := loader.RawConfig()
	if err != nil {
		return nil, err
	}
	if options.Context == "" {
		options.Context = raw.CurrentContext
	}
	options.Namespace = namespace
	config.QPS, config.Burst = 50, 50
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	tektonClient, err := tekton.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Clients{Kube: kube, Tekton: tektonClient, Config: config}, nil
}
