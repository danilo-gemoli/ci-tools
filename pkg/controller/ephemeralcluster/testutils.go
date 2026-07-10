package ephemeralcluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bombsimon/logrusr/v3"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializerjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	prowv1 "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"
	prowconfig "sigs.k8s.io/prow/pkg/config"

	"github.com/openshift/ci-tools/pkg/api"
	ephemeralclusterv1 "github.com/openshift/ci-tools/pkg/api/ephemeralcluster/v1"
	"github.com/openshift/ci-tools/pkg/load/agents"
)

const (
	ProwJobNamespace   = "ci"
	EnvTestCtxKey      = "envtest"
	kubebuilderVersion = "1.32.0"
)

type EnvTestContext struct {
	metadataClient metadata.Interface
	dynClient      dynamic.Interface
	clientSet      *clientset.Clientset
}

func envTestContextFrom(ctx context.Context) *EnvTestContext {
	return ctx.Value(EnvTestCtxKey).(*EnvTestContext)
}

type EnvTestPayload func(ctx context.Context, t *testing.T, client ctrlclient.Client)

func toProwConfigAgent(c *prowconfig.Config) *prowconfig.Agent {
	a := prowconfig.Agent{}
	a.Set(c)
	return &a
}

type fakeRegistryAgent2 struct {
	agents.RegistryAgent
	clusterProfiles map[string]*api.ClusterProfile
}

func (f *fakeRegistryAgent2) ResolveClusterProfile(name string) (api.ClusterProfile, error) {
	cp, ok := f.clusterProfiles[name]
	if !ok {
		return api.ClusterProfile{}, fmt.Errorf("cluster profile %q not found", name)
	}
	return *cp, nil
}

func runTest(t *testing.T, payload EnvTestPayload) {
	envTestCtx := &EnvTestContext{}
	ctx := context.WithValue(t.Context(), EnvTestCtxKey, envTestCtx)

	if err := os.RemoveAll(testArtifactsBaseDir(t)); err != nil {
		t.Fatalf("remove all %s: %s", testArtifactsBaseDir(t), err)
	}

	loggerOut, err := newRealtimeFileWriter(pathFor(t, "log"))
	if err != nil {
		t.Fatalf("realtime file writer for %s: %s", pathFor(t, "log"), err)
	}

	stdLogger := logrus.StandardLogger()
	stdLoggerOut := io.MultiWriter(stdLogger.Out, loggerOut)
	stdLogger.Out = stdLoggerOut
	ctrl.SetLogger(logrusr.New(stdLogger))

	scheme := runtime.NewScheme()
	sb := runtime.NewSchemeBuilder(corev1.AddToScheme, ephemeralclusterv1.AddToScheme, prowv1.AddToScheme)
	if err := sb.AddToScheme(scheme); err != nil {
		t.Fatal("build scheme")
	}

	homePath := os.Getenv("HOME")
	kubebuilderAssets := homePath + "/.local/share/kubebuilder-envtest/k8s/" + kubebuilderVersion + "-linux-amd64/"

	if _, err := os.Stat(kubebuilderAssets); err != nil {
		if os.IsNotExist(err) {
			msg := kubebuilderAssets + " not found, download setup-envtest and install it: "
			msg += "https://github.com/kubernetes-sigs/controller-runtime/tree/main/tools/setup-envtest\n"
			msg += "$ setup-envtest use " + kubebuilderVersion + "\n"
			t.Fatal(msg)
		} else {
			t.Fatalf("stat %q: %s", kubebuilderAssets, err.Error())
		}
	}

	apiServerOut, err := newRealtimeFileWriter(pathFor(t, "apiserver/out"))
	if err != nil {
		t.Fatalf("realtime file writer for %s: %s", pathFor(t, "apiserver/out"), err)
	}

	apiServer := envtest.APIServer{
		Out: apiServerOut,
		Err: apiServerOut,
	}
	apiServer.Configure().Set("v", "6")

	etcdOut, err := newRealtimeFileWriter(pathFor(t, "etcd/out"))
	if err != nil {
		t.Fatalf("realtime file writer for %s: %s", pathFor(t, "apiserver/out"), err)
	}

	testEnv := envtest.Environment{
		ControlPlane: envtest.ControlPlane{
			APIServer: &apiServer,
			Etcd: &envtest.Etcd{
				Out: etcdOut,
				Err: etcdOut,
			},
		},
		BinaryAssetsDirectory: kubebuilderAssets,
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{
				homePath + "/dev/src/github.com/danilo-gemoli/ci-tools/pkg/api/ephemeralcluster/v1/ci.openshift.io_ephemeralclusters.yaml",
				homePath + "/dev/src/github.com/danilo-gemoli/ci-tools/pkg/api/ephemeralcluster/v1/ci.openshift.io_ephemeralclusterstatuses.yaml",
				homePath + "/dev/src/github.com/danilo-gemoli/prow/config/prow/cluster/prowjob-crd/prowjob_customresourcedefinition.yaml",
			},
		},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("envtest start: %s", err)
	}

	metadataClient, err := metadata.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("new metadata client: %s", err)
	}
	envTestCtx.metadataClient = metadataClient

	cset, err := clientset.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("new clientset client: %s", err)
	}
	envTestCtx.clientSet = cset

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("new dynamic client: %s", err)
	}
	envTestCtx.dynClient = dynClient

	adminClient, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("new admin client: %s", err)
	}

	testAdminUser, err := testEnv.AddUser(envtest.User{Name: "test-admin", Groups: []string{"system:masters"}}, &rest.Config{})
	if err != nil {
		t.Fatalf("add user: %s", err)
	}

	adminKubectl, err := testAdminUser.Kubectl()
	if err != nil {
		t.Fatalf("admin kubectl: %s", err)
	}

	adminKubeconfig, err := testAdminUser.KubeConfig()
	if err != nil {
		t.Fatalf("get admin kubeconfig: %s", err)
	}
	writeFile(t, "admin-kubeconfig.yaml", string(adminKubeconfig))

	kubeCtrlMgr, kubeCtrlMgrOut := kubeControllerManager(t, pathFor(t, "admin-kubeconfig.yaml"))
	if err := kubeCtrlMgr.Start(kubeCtrlMgrOut, kubeCtrlMgrOut); err != nil {
		t.Fatalf("start kube-controller-manager: %s", err)
	}

	t.Cleanup(func() {
		dumpAllObjects(t, envTestContextFrom(ctx), adminClient)

		if err := kubeCtrlMgr.Stop(); err != nil {
			t.Logf("kube-controller-manager stop: %s", err)
		}
		if err := kubeCtrlMgrOut.Close(); err != nil {
			t.Logf("close kube-controller-manager out: %s", err)
		}
		if err := testEnv.Stop(); err != nil {
			t.Logf("envtest stop: %s", err)
		}
		if err := loggerOut.Close(); err != nil {
			t.Logf("close logger output: %s", err)
		}
		if err := apiServerOut.Close(); err != nil {
			t.Logf("close apiserver output: %s", err)
		}
		if err := etcdOut.Close(); err != nil {
			t.Logf("close etcd output: %s", err)
		}
	})

	kubectlRun(t, func() (io.Reader, io.Reader, error) {
		return adminKubectl.Run("apply", "-f", "manifests/single-cluster.yaml")
	})
	saToken := kubectlRun(t, func() (io.Reader, io.Reader, error) {
		return adminKubectl.Run("-n", "ci", "create", "token", "dptp-controller-manager")
	})
	saCfg := restConfig(t, testEnv, "dptp-controller-manager", saToken)

	mgr, err := ctrl.NewManager(saCfg, manager.Options{
		Scheme:     scheme,
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatalf("create manager: %s", err)
	}

	entrypoint(ctx, t, stdLogger, mgr, adminClient, payload)
}

func kubeControllerManager(t *testing.T, kubeconfigPath string) (*ProcState, *realtimeFileWriter) {
	kubeCtrlMgrBin := "/usr/local/bin/kube-controller-manager"
	if _, err := os.Stat(kubeCtrlMgrBin); err != nil && os.IsNotExist(err) {
		t.Fatalf("Download kube-controller-manager from https://dl.k8s.io/v" + kubebuilderVersion + "/bin/linux/amd64/kube-controller-manager " +
			"and store it into /usr/local/bin")
	}

	args := fmt.Sprintf("--kubeconfig=%s --controllers=namespace,garbagecollector --leader-elect=false", kubeconfigPath)
	kubeCtrlMgr := NewProcState(kubeCtrlMgrBin, strings.Split(args, " "))

	u, err := url.Parse("https://localhost:10257/healthz")
	if err != nil {
		t.Fatalf("Parse URL https://localhost:10257/healthz: %s", err)
	}
	kubeCtrlMgr.HealthCheck.URL = *u

	kubeCtrlMgrOut, err := newRealtimeFileWriter(pathFor(t, "kube-controller-manager/out"))
	if err != nil {
		t.Fatalf("realtime file writer for %s: %s", pathFor(t, "kube-controller-manager/out"), err)
	}

	return kubeCtrlMgr, kubeCtrlMgrOut
}

func entrypoint(ctx context.Context, t *testing.T, logger *logrus.Logger, mgr manager.Manager, adminClient ctrlclient.Client, payload EnvTestPayload) {
	ctx, cancel := context.WithCancelCause(ctx)

	onManagerStart(logger, t, mgr)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := mgr.Start(ctx); err != nil {
			cancel(err)
		}
	})

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatalf("couldn't sync manager cache")
	}

	prepareEnv(ctx, t, adminClient)
	payload(ctx, t, adminClient)

	cancel(errors.New("test done"))
	wg.Wait()
}

func prepareEnv(ctx context.Context, t *testing.T, client ctrlclient.Client) {
	ecNS := corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: EphemeralClusterNamespace}}
	createObj(ctx, t, client, &ecNS)
}

func onManagerStart(logger *logrus.Logger, t *testing.T, mgr manager.Manager) {
	pc := prowconfig.Config{
		ProwConfig: prowconfig.ProwConfig{
			ProwJobNamespace: ProwJobNamespace,
			PodNamespace:     ProwJobNamespace,
			InRepoConfig:     prowconfig.InRepoConfig{AllowedClusters: map[string][]string{"": {"default"}}},
			Plank: prowconfig.Plank{
				DefaultDecorationConfigs: []*prowconfig.DefaultDecorationConfigEntry{{
					Config: &prowv1.DecorationConfig{
						GCSConfiguration: &prowv1.GCSConfiguration{
							DefaultOrg:   "org",
							DefaultRepo:  "repo",
							PathStrategy: prowv1.PathStrategySingle,
						},
						UtilityImages: &prowv1.UtilityImages{
							CloneRefs:  "clonerefs",
							InitUpload: "initupload",
							Entrypoint: "entrypoint",
							Sidecar:    "sidecar",
						},
					},
				}},
			},
		},
	}

	prowConfigAgent := toProwConfigAgent(&pc)
	fakeRegistryAgent := &fakeRegistryAgent2{
		clusterProfiles: map[string]*api.ClusterProfile{
			"cluster-profile": {
				Name:        "cluster-profile",
				ClusterType: "aws",
				Owners: []api.ClusterProfileOwners{{
					Konflux: &api.ClusterProfileKonfluxOwner{
						Tenant:           "ktenant",
						ClustersResolved: []string{"kcluster"},
					},
				}},
			},
		},
	}

	if err := AddToManager(logrus.NewEntry(logger), mgr,
		map[string]manager.Manager{"build01": mgr}, prowConfigAgent, fakeRegistryAgent,
		WithCLIISTagRef("ocp/4.21:cli"),
		WithPrivilegedTenants([]string{"ktenant-privileged"})); err != nil {
		t.Fatalf("build controller: %s", err)
	}
}

func waitFor(ctx context.Context, t *testing.T, f func(context.Context) (bool, error)) {
	if err := wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
		Steps:    10,
		Duration: 2 * time.Second,
		Factor:   1.0,
		Jitter:   0.1,
	}, f); err != nil {
		debug.PrintStack()
		t.Fatalf("wait obj: %s - ctx: %s", err, context.Cause(ctx))
	}
}

func createObj(ctx context.Context, t *testing.T, c ctrlclient.Client, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
	if err := c.Create(ctx, obj, opts...); err != nil {
		debug.PrintStack()
		t.Fatalf("create obj %s/%s: %s", obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

func updateObj(ctx context.Context, t *testing.T, c ctrlclient.Client, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
	if err := c.Update(ctx, obj, opts...); err != nil {
		debug.PrintStack()
		t.Fatalf("udpate obj %s/%s: %s", obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

func deleteObj(ctx context.Context, t *testing.T, c ctrlclient.Client, obj ctrlclient.Object, opts ...ctrlclient.DeleteOption) error {
	if err := c.Delete(ctx, obj, opts...); err != nil {
		debug.PrintStack()
		t.Fatalf("delete obj %s/%s: %s", obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

func kubectlRun(t *testing.T, f func() (io.Reader, io.Reader, error)) string {
	stdout, stderr, err := f()
	if err != nil {
		bytes, err := io.ReadAll(io.MultiReader(stdout, stderr))
		debug.PrintStack()
		if err != nil {
			t.Fatalf("kubectl read: %s", err)
		}
		t.Fatalf("kubectl run: %s", bytes)
	}

	bytes, err := io.ReadAll(io.MultiReader(stdout, stderr))
	if err != nil {
		debug.PrintStack()
		t.Fatalf("kubectl read: %s", err)
	}

	return string(bytes)
}

func kubeconfig(t *testing.T, testEnv envtest.Environment, username, token string) string {
	apiServer := testEnv.ControlPlane.APIServer
	const contextName = "envtest"
	const clusterName = "envtest"

	config := clientcmdapi.Config{
		Kind: "Config",
		Clusters: map[string]*clientcmdapi.Cluster{
			clusterName: {
				InsecureSkipTLSVerify: true,
				Server:                fmt.Sprintf("https://%s:%s", apiServer.Address, apiServer.Port),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			clusterName: {
				Cluster:  clusterName,
				AuthInfo: username,
			},
		},
		CurrentContext: clusterName,
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			username: {
				Token: token,
			},
		},
	}

	kubeconfig, err := clientcmd.Write(config)
	if err != nil {
		t.Fatalf("write kube config: %s", err)
	}

	writeFile(t, username+"-kubeconfig.yaml", string(kubeconfig))
	return string(kubeconfig)
}

func restConfig(t *testing.T, testEnv envtest.Environment, username, token string) *rest.Config {
	kubeconfig := kubeconfig(t, testEnv, username, token)

	clientConfig, err := clientcmd.NewClientConfigFromBytes([]byte(kubeconfig))
	if err != nil {
		debug.PrintStack()
		t.Fatalf("new client config: %s", err)
	}

	cfg, err := clientConfig.ClientConfig()
	if err != nil {
		debug.PrintStack()
		t.Fatalf("new cfg: %s", err)
	}

	return cfg
}

func readFile(t *testing.T, filename string) string {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		debug.PrintStack()
		t.Fatalf("read file %s: %s", filename, err)
	}
	return string(bytes)
}

func writeFile(t *testing.T, name string, data string) {
	fullpath := pathFor(t, name)

	base := path.Dir(fullpath)
	if err := os.MkdirAll(base, 0744); err != nil {
		if !os.IsExist(err) {
			debug.PrintStack()
			t.Fatalf("mkdir all %s: %s", fullpath, err)
		}
	}

	if err := os.WriteFile(fullpath, []byte(data), 0644); err != nil {
		debug.PrintStack()
		t.Fatalf("write file %s: %s", fullpath, err)
	}
}

func openFile(filePath string, flag int) (*os.File, error) {
	base := path.Dir(filePath)
	if err := os.MkdirAll(base, 0744); err != nil {
		if !os.IsExist(err) {
			return nil, fmt.Errorf("mkdir all %s: %w", filePath, err)
		}
	}
	return os.OpenFile(filePath, flag, 0644)
}

func testArtifactsBaseDir(t *testing.T) string {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %s", err)
	}
	return path.Join(cwd, "testartifacts", t.Name())
}

func pathFor(t *testing.T, name string) string {
	return path.Join(testArtifactsBaseDir(t), name)
}

func dumpAllObjects(t *testing.T, envTestCtx *EnvTestContext, adminClient ctrlclient.Client) {
	ctx := context.TODO()

	addCIOperatorNS := func(ns []string) []string {
		ret := ns

		nsList := corev1.NamespaceList{}
		if err := adminClient.List(ctx, &nsList, &ctrlclient.ListOptions{}); err != nil {
			t.Fatalf("list namespaces: %s", err)
		}

		for _, ns := range nsList.Items {
			if strings.HasPrefix(ns.Name, "ci-op-") {
				ret = append(ret, ns.Name)
			}
		}

		return ret
	}

	apiResLists, err := envTestCtx.clientSet.Discovery().ServerPreferredResources()
	if err != nil {
		t.Fatalf("server preferred resources: %s", err)
	}

	serializerScheme := runtime.NewScheme()
	listableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list"}}, apiResLists)
	namespaces := addCIOperatorNS([]string{EphemeralClusterNamespace, ProwJobNamespace})

listableResLoop:
	for _, apiResList := range listableResources {
		if apiResList == nil {
			continue listableResLoop
		}

		gv, err := schema.ParseGroupVersion(apiResList.GroupVersion)
		if err != nil {
			t.Fatalf("parse group version %s: %s", apiResList.GroupVersion, err)
		}

	resLoop:
		for _, apiRes := range apiResList.APIResources {
			if !apiRes.Namespaced {
				continue resLoop
			}

			gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: apiRes.Name}

		nsLoop:
			for _, ns := range namespaces {
				l, err := envTestCtx.dynClient.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
				if err != nil {
					if !apierrors.IsNotFound(err) {
						t.Fatalf("list gvr %s resources: %s", gvr.String(), err)
					}
				}

				if l == nil || len(l.Items) == 0 {
					continue nsLoop
				}

				buf := strings.Builder{}
				if _, err := buf.Write([]byte("---\n")); err != nil {
					t.Fatalf("write ---: %s", err)
				}

				metav1.AddToGroupVersion(serializerScheme, gv)

				ySerializer := serializerjson.NewYAMLSerializer(serializerjson.DefaultMetaFactory, serializerScheme, serializerScheme)
				if err := ySerializer.Encode(l, &buf); err != nil {
					t.Fatalf("encode %s: %s", gvr.String(), err)
				}

				writeFile(t, "objects/"+ns+"/"+apiRes.Name+".yaml", buf.String())
			}
		}
	}
}

func newRealtimeFileWriter(filePath string) (*realtimeFileWriter, error) {
	f, err := openFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_SYNC|os.O_TRUNC)
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", filePath, err)
	}

	return &realtimeFileWriter{
		f:        f,
		filePath: filePath,
		m:        sync.Mutex{},
	}, nil
}

type realtimeFileWriter struct {
	f        *os.File
	filePath string
	m        sync.Mutex
}

func (r *realtimeFileWriter) Write(p []byte) (n int, err error) {
	r.m.Lock()
	defer r.m.Unlock()
	return r.f.Write(p)
}

func (r *realtimeFileWriter) Close() error {
	return r.f.Close()
}
