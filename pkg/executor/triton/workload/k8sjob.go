package workload

import (
	"fmt"
	"os"
	"path/filepath"
	"poseidon/pkg/api"
	"poseidon/pkg/context"
	"poseidon/pkg/worker"
	"strings"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const defaultNamespace = "default"

//K8SJobWorkloadConfig defines configuration for K8sJobWorkload
type K8SJobWorkloadConfig struct {
	OutOfCluster bool   `json:"outOfCluster"` // Out of cluster means the controller is started outside the cluster, the client will connect through the .kube/config file
	KubeConfig   string `json:"kubeconfig"`
	Namespace    string `json:"namespace"`
	BrokerSecret string `json:"brokerSecret"`
}

type k8sJob struct {
	clientset *kubernetes.Clientset
	config    K8SJobWorkloadConfig
}

func NewK8SJobWorkload(config K8SJobWorkloadConfig) (Workload, error) {
	// Kubernetes client
	var conf *rest.Config
	if config.OutOfCluster { // Out of cluster configuration, get config from $HOME/.kube/config or specified kubeconfig
		var kubeconfig string
		if config.KubeConfig != "" {
			kubeconfig = config.KubeConfig
		} else {
			//Get config from $HOME/.kube/config
			homedir, err := os.UserHomeDir()
			if err != nil {
				return nil, errors.Wrap(err, "cannot get user's home directory")
			}
			kubeconfig = filepath.Join(homedir, ".kube", "config")
		}
		c, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get kubernetes configuration from %s", kubeconfig)
		}
		conf = c
	} else {
		c, err := rest.InClusterConfig()
		if err != nil {
			return nil, errors.Wrap(err, "cannot get kubernetes configuration")
		}
		conf = c
	}
	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create kubernetes client")
	}

	if config.Namespace == "" {
		config.Namespace = defaultNamespace
	}

	return k8sJob{
		clientset: clientset,
		config:    config,
	}, nil
}

func (k k8sJob) Schedule(ctx context.Context, spec api.NodeSpec, n int) error {
	var p int32 = int32(parallelism(n, spec.Parallelism))
	ctx.Logger().Tracef("scheduling workload for node %s with parallelism at %d", spec.Name, p)
	jobClient := k.clientset.BatchV1().Jobs(k.config.Namespace)

	// Env
	containerEnv := []apiv1.EnvVar{
		apiv1.EnvVar{
			Name:  worker.EnvProcessID,
			Value: ctx.ProcessID(),
		},
		apiv1.EnvVar{
			Name:  worker.EnvNodeName,
			Value: ctx.NodeName(),
		},
		apiv1.EnvVar{
			Name:  worker.EnvPublishQName,
			Value: "poseidon.ex.events",
		},
	}
	//Create actual Job
	var backoffLimit int32 = 5
	job := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName(ctx),
			Labels: map[string]string{
				api.HeaderCorrelationID: ctx.CorrelationID(),
				api.HeaderProcessID:     ctx.ProcessID(),
				api.HeaderNodename:      ctx.NodeName(),
			},
		},
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						apiv1.Container{
							Name:  "worker",
							Image: spec.Image,
							Env:   containerEnv,
						},
					},
					RestartPolicy: apiv1.RestartPolicyOnFailure,
				},
			},
			Parallelism:  &p,
			BackoffLimit: &backoffLimit,
		},
	}

	// Inject broker
	if k.config.BrokerSecret != "" {
		secret := apiv1.SecretEnvSource{
			LocalObjectReference: apiv1.LocalObjectReference{
				Name: k.config.BrokerSecret,
			},
		}
		job.Spec.Template.Spec.Containers[0].EnvFrom = []apiv1.EnvFromSource{
			apiv1.EnvFromSource{
				SecretRef: &secret,
			},
		}
	}
	if _, err := jobClient.Create(&job); err != nil {
		return errors.Wrapf(err, "cannot start kubernetes job %s", job.ObjectMeta.Name)
	}
	ctx.Logger().Tracef("kubernetes job %s started", job.ObjectMeta.Name)
	return nil
}

func (k k8sJob) Delete(ctx context.Context, nodename string) error {
	ctx.Logger().Tracef("deleting workload for node %s", nodename)
	jobClient := k.clientset.BatchV1().Jobs(k.config.Namespace)
	name := jobName(ctx)
	propagationPolicy := metav1.DeletePropagationBackground
	if err := jobClient.Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}); err != nil {
		return errors.Wrapf(err, "cannot delete kubernetes job %s", name)
	}
	return nil
}

func jobName(ctx context.Context) string {
	// Uppercase in kubernetes artifact name are not allowed
	return fmt.Sprintf("j-%s-%s", strings.ToLower(ctx.NodeName()), ctx.ProcessID())
}
