package ephemeralcluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	prowv1 "sigs.k8s.io/prow/pkg/apis/prowjobs/v1"

	"github.com/openshift/ci-tools/pkg/api"
	ephemeralclusterv1 "github.com/openshift/ci-tools/pkg/api/ephemeralcluster/v1"
	"github.com/openshift/ci-tools/pkg/steps"
)

func Test_EnvTest_CreateECFetchKubeconfigDeleteEC(t *testing.T) {
	runTest(t, func(ctx context.Context, t *testing.T, client ctrlclient.Client) {
		const ecName = "foo"

		ec := ephemeralclusterv1.EphemeralCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					ephemeralclusterv1.KonfluxClusterAnnotation: "kcluster",
					ephemeralclusterv1.KonfluxTenantAnnotation:  "ktenant",
				},
				Name:      ecName,
				Namespace: EphemeralClusterNamespace,
			},
			Spec: ephemeralclusterv1.EphemeralClusterSpec{
				CIOperator: ephemeralclusterv1.CIOperatorSpec{
					Releases: map[string]api.UnresolvedRelease{
						"latest": {Integration: &api.Integration{Name: "4.17", Namespace: "ocp"}},
					},
					Test: ephemeralclusterv1.TestSpec{
						Workflow:       "ipi-aws",
						Env:            make(map[string]string),
						ClusterProfile: "cluster-profile",
					},
				},
			},
		}
		createObj(ctx, t, client, &ec)

		pjs := prowv1.ProwJobList{}
		waitFor(ctx, t, func(context.Context) (bool, error) {
			if err := client.List(ctx, &pjs, ctrlclient.MatchingLabels{EphemeralClusterLabel: ec.Name}); err != nil {
				return true, err
			}
			return len(pjs.Items) > 0, nil
		})
		pj := pjs.Items[0]

		testNS := "ci-op-xxxx"
		createObj(ctx, t, client, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{steps.LabelJobID: pj.Name},
			Name:   testNS,
		}})

		wantKubeconfig := "fake-kubeconfig"
		createObj(ctx, t, client, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: EphemeralClusterTestName, Namespace: testNS},
			Data:       map[string][]byte{"kubeconfig": []byte(wantKubeconfig)},
		})

		kubeconfigSecret := corev1.Secret{}
		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			name := credentialsSecretName(&ec)
			if err := client.Get(ctx, types.NamespacedName{Namespace: EphemeralClusterNamespace, Name: name}, &kubeconfigSecret); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				t.Errorf("wait kubeconfig secret: %s", err)
				return true, err
			}
			return true, nil
		})

		gotKubeconfig := string(kubeconfigSecret.Data["kubeconfig"])
		if gotKubeconfig != wantKubeconfig {
			t.Errorf("want kubeconfig %s but got %s", wantKubeconfig, gotKubeconfig)
		}

		ec = ephemeralclusterv1.EphemeralCluster{}
		waitFor(ctx, t, func(context.Context) (bool, error) {
			if err := client.Get(ctx, types.NamespacedName{Namespace: EphemeralClusterNamespace, Name: ecName}, &ec); err != nil {
				t.Errorf("wait ec: %s", err)
				return true, err
			}
			return true, nil
		})

		ec.Spec.TearDownCluster = true
		updateObj(ctx, t, client, &ec)

		waitFor(ctx, t, func(context.Context) (bool, error) {
			err := client.Get(ctx, types.NamespacedName{Namespace: testNS, Name: api.EphemeralClusterTestDoneSignalSecretName}, &corev1.Secret{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return true, err
		})

		// Delete the EphemeralCluster
		deleteObj(ctx, t, client, &ec)

		// The ProwJob should be aborted
		waitFor(ctx, t, func(context.Context) (bool, error) {
			pj := prowv1.ProwJob{}
			err := client.Get(ctx, types.NamespacedName{Namespace: ProwJobNamespace, Name: ec.Status.ProwJobID}, &pj)
			if err != nil {
				return true, err
			}
			if pj.Status.State == prowv1.AbortedState {
				return true, nil
			}
			return false, nil
		})

		// Make sure no EphemeralClusters still exist
		waitFor(ctx, t, func(context.Context) (bool, error) {
			el := ephemeralclusterv1.EphemeralClusterList{}
			err := client.List(ctx, &el)
			if err != nil {
				return true, err
			}
			if len(el.Items) == 0 {
				return true, nil
			}
			return false, nil
		})
	})
}

func Test_EnvTest_CreateECHiveFetchKubeconfigDeleteEC(t *testing.T) {
	runTest(t, func(ctx context.Context, t *testing.T, client ctrlclient.Client) {
		const ecName = "foo"

		ec := ephemeralclusterv1.EphemeralCluster{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					ephemeralclusterv1.KonfluxClusterAnnotation: "kcluster",
					ephemeralclusterv1.KonfluxTenantAnnotation:  "ktenant",
				},
				Name:      ecName,
				Namespace: EphemeralClusterNamespace,
			},
			Spec: ephemeralclusterv1.EphemeralClusterSpec{
				CIOperator: ephemeralclusterv1.CIOperatorSpec{
					Test: ephemeralclusterv1.TestSpec{
						Workflow:       "generic-claim",
						Env:            make(map[string]string),
						ClusterProfile: "cluster-profile",
						ClusterClaim: &api.ClusterClaim{
							As:           "claim",
							Product:      "ocp",
							Version:      "4.22",
							Architecture: api.ReleaseArchitectureAMD64,
							Cloud:        "aws",
							Owner:        "test-platform",
							Labels:       map[string]string{"region": "us-east-1"},
							Timeout:      &prowv1.Duration{},
						},
					},
				},
			},
		}
		createObj(ctx, t, client, &ec)

		pjs := prowv1.ProwJobList{}
		waitFor(ctx, t, func(context.Context) (bool, error) {
			if err := client.List(ctx, &pjs, ctrlclient.MatchingLabels{EphemeralClusterLabel: ec.Name}); err != nil {
				return true, err
			}
			return len(pjs.Items) > 0, nil
		})
		pj := pjs.Items[0]

		testNS := "ci-op-xxxx"
		createObj(ctx, t, client, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{steps.LabelJobID: pj.Name},
			Name:   testNS,
		}})

		wantKubeconfig := "fake-kubeconfig"
		createObj(ctx, t, client, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: HiveKubeconfigSecret, Namespace: testNS},
			Data:       map[string][]byte{api.HiveAdminKubeconfigSecretKey: []byte(wantKubeconfig)},
		})

		wantAdminPasswd := "admin-passwd"
		createObj(ctx, t, client, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: HiveAdminPasswdSecret, Namespace: testNS},
			Data:       map[string][]byte{api.HiveAdminPasswordSecretKey: []byte(wantAdminPasswd)},
		})

		kubeconfigSecret := corev1.Secret{}
		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			name := credentialsSecretName(&ec)
			if err := client.Get(ctx, types.NamespacedName{Namespace: EphemeralClusterNamespace, Name: name}, &kubeconfigSecret); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				t.Errorf("wait kubeconfig secret: %s", err)
				return true, err
			}
			return true, nil
		})

		gotKubeconfig := string(kubeconfigSecret.Data["kubeconfig"])
		if gotKubeconfig != wantKubeconfig {
			t.Errorf("want kubeconfig %s but got %s", wantKubeconfig, gotKubeconfig)
		}

		gotAdminPasswd := string(kubeconfigSecret.Data["kubeAdminPassword"])
		if gotAdminPasswd != wantAdminPasswd {
			t.Errorf("want kube admin passwd %s but got %s", wantAdminPasswd, gotAdminPasswd)
		}

		ec = ephemeralclusterv1.EphemeralCluster{}
		waitFor(ctx, t, func(context.Context) (bool, error) {
			if err := client.Get(ctx, types.NamespacedName{Namespace: EphemeralClusterNamespace, Name: ecName}, &ec); err != nil {
				t.Errorf("wait ec: %s", err)
				return true, err
			}
			return true, nil
		})

		ec.Spec.TearDownCluster = true
		updateObj(ctx, t, client, &ec)

		waitFor(ctx, t, func(context.Context) (bool, error) {
			err := client.Get(ctx, types.NamespacedName{Namespace: testNS, Name: api.EphemeralClusterTestDoneSignalSecretName}, &corev1.Secret{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return true, err
		})

		// Delete the EphemeralCluster
		deleteObj(ctx, t, client, &ec)

		// The ProwJob should be aborted
		waitFor(ctx, t, func(context.Context) (bool, error) {
			pj := prowv1.ProwJob{}
			err := client.Get(ctx, types.NamespacedName{Namespace: ProwJobNamespace, Name: ec.Status.ProwJobID}, &pj)
			if err != nil {
				return true, err
			}
			if pj.Status.State == prowv1.AbortedState {
				return true, nil
			}
			return false, nil
		})

		// Make sure no EphemeralClusters still exist
		waitFor(ctx, t, func(context.Context) (bool, error) {
			el := ephemeralclusterv1.EphemeralClusterList{}
			err := client.List(ctx, &el)
			if err != nil {
				return true, err
			}
			if len(el.Items) == 0 {
				return true, nil
			}
			return false, nil
		})
	})
}

func Test_EnvTest_ECMissingAbortProwJob(t *testing.T) {
	runTest(t, func(ctx context.Context, t *testing.T, client ctrlclient.Client) {
		const pjName = "pj"

		createObj(ctx, t, client, &prowv1.ProwJob{
			ObjectMeta: metav1.ObjectMeta{
				Labels:    map[string]string{EphemeralClusterLabel: pjName},
				Name:      pjName,
				Namespace: ProwJobNamespace,
			},
			Spec: prowv1.ProwJobSpec{
				Cluster: "build01",
				Job:     "foo-pj",
				Type:    prowv1.PeriodicJob,
			},
			Status: prowv1.ProwJobStatus{State: prowv1.TriggeredState},
		})

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			pj := prowv1.ProwJob{}
			nn := types.NamespacedName{Namespace: ProwJobNamespace, Name: pjName}
			if err := client.Get(ctx, nn, &pj); err != nil {
				return true, err
			}
			return pj.Status.State == prowv1.AbortedState, nil
		})
	})
}

func Test_EnvTest_ECMissingGracefullyTerminateCIOperator(t *testing.T) {
	runTest(t, func(ctx context.Context, t *testing.T, client ctrlclient.Client) {
		const (
			pjName = "pj"
			nsName = "ci-op-xxx"
		)

		createObj(ctx, t, client, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{steps.LabelJobID: pjName},
			Name:   nsName,
		}})

		createObj(ctx, t, client, &prowv1.ProwJob{
			ObjectMeta: metav1.ObjectMeta{
				Labels:    map[string]string{EphemeralClusterLabel: pjName},
				Name:      pjName,
				Namespace: ProwJobNamespace,
			},
			Spec: prowv1.ProwJobSpec{
				Job:     "foo-pj",
				Cluster: "build01",
				Type:    prowv1.PeriodicJob,
			},
			Status: prowv1.ProwJobStatus{State: prowv1.TriggeredState},
		})

		waitFor(ctx, t, func(ctx context.Context) (bool, error) {
			secret := corev1.Secret{}
			nn := types.NamespacedName{Namespace: nsName, Name: api.EphemeralClusterTestDoneSignalSecretName}
			if err := client.Get(ctx, nn, &secret); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return true, err
			}
			return true, nil
		})
	})
}
