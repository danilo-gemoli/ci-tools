package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/prow/cmd/generic-autobumper/bumper"
	prowconfig "sigs.k8s.io/prow/pkg/config"
	"sigs.k8s.io/prow/pkg/config/secret"
	"sigs.k8s.io/prow/pkg/flagutil"
	"sigs.k8s.io/yaml"

	"github.com/openshift/ci-tools/pkg/api"
	"github.com/openshift/ci-tools/pkg/dispatcher"
	"github.com/openshift/ci-tools/pkg/github/prcreation"
	"github.com/openshift/ci-tools/pkg/rehearse"
	"github.com/openshift/ci-tools/pkg/util/gzip"
)

type ClusterMap map[string]string

const (
	blocked        = "blocked"
	githubOrg      = "openshift"
	githubRepo     = "release"
	githubLogin    = "openshift-bot"
	matchTitle     = "Automate prow job dispatcher"
	upstreamBranch = "master"
)

type options struct {
	prowJobConfigDir  string
	configPath        string
	clusterConfigPath string
	jobsStoragePath   string
	daemonize         bool

	maxConcurrency       int
	prometheusDaysBefore int

	createPR    bool
	githubLogin string
	targetDir   string
	assign      string

	enableClusters  flagutil.Strings
	disableClusters flagutil.Strings
	defaultCluster  string

	bumper.GitAuthorOptions
	dispatcher.PrometheusOptions
	prcreation.PRCreationOptions
}

func gatherOptions() options {
	o := options{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	fs.StringVar(&o.prowJobConfigDir, "prow-jobs-dir", "", "Path to a root of directory structure with Prow job config files (ci-operator/jobs in openshift/release)")
	fs.StringVar(&o.configPath, "config-path", "", "Path to the config file (core-services/sanitize-prow-jobs/_config.yaml in openshift/release)")
	fs.StringVar(&o.clusterConfigPath, "cluster-config-path", "core-services/sanitize-prow-jobs/_clusters.yaml", "Path to the config file (core-services/sanitize-prow-jobs/_clusters.yaml in openshift/release)")
	fs.StringVar(&o.jobsStoragePath, "jobs-storage-path", "", "Path to the file holding only job assignments in Gob format")
	fs.BoolVar(&o.daemonize, "daemonize", false, "Run dispatcher in daemon mode")
	fs.IntVar(&o.prometheusDaysBefore, "prometheus-days-before", 1, "Number [1,15] of days before. Time 00-00-00 of that day will be used as time to query Prometheus. E.g., 1 means 00-00-00 of yesterday.")
	fs.IntVar(&o.maxConcurrency, "concurrency", 0, "Maximum number of concurrent in-flight goroutines to handle files.")

	fs.BoolVar(&o.createPR, "create-pr", false, "Create a pull request to the change made with this tool.")
	fs.StringVar(&o.githubLogin, "github-login", githubLogin, "The GitHub username to use.")
	fs.StringVar(&o.targetDir, "target-dir", "", "The directory containing the target repo.")
	fs.StringVar(&o.assign, "assign", "ghost", "The github username or group name to assign the created pull request to.")

	fs.Var(&o.enableClusters, "enable-cluster", "Enable this cluster. Does nothing if the cluster is enabled. Can be passed multiple times and must be disjoint with all --disable-cluster values.")
	fs.Var(&o.disableClusters, "disable-cluster", "Disable this cluster. Does nothing if the cluster is disabled. Can be passed multiple times and must be disjoint with all --enable-cluster values.")
	fs.StringVar(&o.defaultCluster, "default-cluster", "", "If passed, changes the default cluster to the specified value.")

	o.GitAuthorOptions.AddFlags(fs)
	o.PrometheusOptions.AddFlags(fs)
	o.PRCreationOptions.AddFlags(fs)

	o.AllowAnonymous = true
	if err := fs.Parse(os.Args[1:]); err != nil {
		logrus.WithError(err).Fatal("could not parse input")
	}
	return o
}

func (o *options) validate() error {
	if o.maxConcurrency == 0 {
		o.maxConcurrency = runtime.GOMAXPROCS(0)
	}
	if o.prowJobConfigDir == "" {
		return fmt.Errorf("mandatory argument --prow-jobs-dir wasn't set")
	}
	if o.configPath == "" {
		return fmt.Errorf("mandatory argument --config-path wasn't set")
	}

	if o.prometheusDaysBefore < 1 || o.prometheusDaysBefore > 15 {
		return fmt.Errorf("--prometheus-days-before must be between 1 and 15")
	}

	enabled := o.enableClusters.StringSet()
	disabled := o.disableClusters.StringSet()
	if enabled.Intersection(disabled).Len() > 0 {
		return fmt.Errorf("--enable-cluster and --disable-cluster values must be disjoint sets")
	}

	if disabled.Has(o.defaultCluster) {
		return fmt.Errorf("--default-cluster value cannot be also be in --disable-cluster")
	}

	if o.createPR {
		if o.githubLogin == "" {
			return fmt.Errorf("--github-login cannot be empty string")
		}
		if err := o.GitAuthorOptions.Validate(); err != nil {
			return err
		}
		if o.targetDir == "" {
			return fmt.Errorf("--target-dir is mandatory")
		}
		if o.assign == "" {
			return fmt.Errorf("--assign is mandatory")
		}
		if err := o.PRCreationOptions.GitHubOptions.Validate(false); err != nil {
			return err
		}
	}
	return o.PrometheusOptions.Validate()
}

func loadClusterConfig(filePath string) (ClusterMap, sets.Set[string], error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}

	var clusters map[string][]string
	err = yaml.Unmarshal(data, &clusters)
	if err != nil {
		return nil, nil, err
	}

	blockedClusters := sets.New[string]()
	clusterMap := make(ClusterMap)
	for provider, clusterList := range clusters {
		if provider != blocked {
			for _, cluster := range clusterList {
				clusterMap[cluster] = provider
			}
		}
		if provider == blocked {
			blockedClusters.Insert(clusterList...)
		}
	}

	return clusterMap, blockedClusters, nil
}

// getCloudProvidersForE2ETests returns a set of cloud providers where a cluster is hosted for an e2e test defined in the given Prow job config.
func getCloudProvidersForE2ETests(jc *prowconfig.JobConfig) sets.Set[string] {
	cloudProviders := sets.New[string]()
	for k := range jc.PresubmitsStatic {
		for _, job := range jc.PresubmitsStatic[k] {
			if cloud := dispatcher.DetermineCloud(job.JobBase); cloud != "" {
				cloudProviders.Insert(cloud)
			}
		}
	}
	for k := range jc.PostsubmitsStatic {
		for _, job := range jc.PostsubmitsStatic[k] {
			if cloud := dispatcher.DetermineCloud(job.JobBase); cloud != "" {
				cloudProviders.Insert(cloud)
			}
		}
	}
	for _, job := range jc.Periodics {
		if cloud := dispatcher.DetermineCloud(job.JobBase); cloud != "" {
			cloudProviders.Insert(cloud)
		}
	}
	return cloudProviders
}

type clusterVolume struct {
	// [cloudProvider][cluster]volume
	clusterVolumeMap map[string]map[string]float64
	// only needed for stable tests: traverse the above map by sorted key list
	cloudProviders sets.Set[string]
	pjs            map[string]string
	blocked        sets.Set[string]
	mutex          sync.Mutex
}

// findClusterForJobConfig finds a cluster running on a preferred cloud provider for the jobs in a Prow job config.
// The chosen cluster will be the one with minimal workload with the given cloud provider.
// If the cluster provider is empty string, it will choose the one with minimal workload across all cloud providers.
func (cv *clusterVolume) findClusterForJobConfig(cloudProvider string, jc *prowconfig.JobConfig, path string, config *dispatcher.Config, jobVolumes map[string]float64) (string, error) {
	// no cluster in the build farm is from the targeting cloud provider
	if _, ok := cv.clusterVolumeMap[cloudProvider]; !ok {
		cloudProvider = ""
	}
	var cluster, rCloudProvider string
	min := float64(-1)
	cv.mutex.Lock()
	for _, cp := range sets.List(cv.cloudProviders) {
		m := cv.clusterVolumeMap[cp]
		for c, v := range m {
			if cloudProvider == "" || cloudProvider == cp {
				if min < 0 || min > v {
					min = v
					cluster = c
					rCloudProvider = cp
				}
			}
		}
	}

	var errs []error
	for k := range jc.PresubmitsStatic {
		for _, job := range jc.PresubmitsStatic[k] {
			if err := cv.addToVolume(rCloudProvider, cluster, job.JobBase, path, config, jobVolumes); err != nil {
				errs = append(errs, err)
			}
		}
	}

	for k := range jc.PostsubmitsStatic {
		for _, job := range jc.PostsubmitsStatic[k] {
			if err := cv.addToVolume(rCloudProvider, cluster, job.JobBase, path, config, jobVolumes); err != nil {
				errs = append(errs, err)
			}
		}
	}
	for _, job := range jc.Periodics {
		if err := cv.addToVolume(rCloudProvider, cluster, job.JobBase, path, config, jobVolumes); err != nil {
			errs = append(errs, err)
		}
	}

	cv.mutex.Unlock()
	return cluster, utilerrors.NewAggregate(errs)
}

func (cv *clusterVolume) addToVolume(cloudProvider, cluster string, jobBase prowconfig.JobBase, path string, config *dispatcher.Config, jobVolumes map[string]float64) error {
	determinedCluster, canBeRelocated, err := config.DetermineClusterForJob(jobBase, path)
	if err != nil {
		return fmt.Errorf("failed to determine cluster for the job %s in path %q: %w", jobBase.Name, path, err)
	}
	if cluster == string(determinedCluster) || canBeRelocated {
		cv.clusterVolumeMap[cloudProvider][cluster] = cv.clusterVolumeMap[cloudProvider][cluster] + jobVolumes[jobBase.Name]
	} else if determinedCloudProvider := config.IsInBuildFarm(determinedCluster); determinedCloudProvider != "" {
		cv.clusterVolumeMap[string(determinedCloudProvider)][string(determinedCluster)] = cv.clusterVolumeMap[string(determinedCloudProvider)][string(determinedCluster)] + jobVolumes[jobBase.Name]
	}

	cv.pjs[jobBase.Name] = cv.determineCluster(cluster, string(determinedCluster), string(config.Default), canBeRelocated)
	return nil
}

func (cv *clusterVolume) determineCluster(cluster, determinedCluster, defaultCluster string, canBeRelocated bool) string {
	var targetCluster string
	if cluster == determinedCluster || canBeRelocated {
		targetCluster = cluster
	} else if _, isBlocked := cv.blocked[determinedCluster]; !isBlocked {
		targetCluster = determinedCluster
	} else {
		targetCluster = cluster
	}

	if _, isBlocked := cv.blocked[targetCluster]; isBlocked {
		return defaultCluster
	}
	return targetCluster
}

// dispatchJobConfig dispatches the jobs defined in a Prow jon config
func (cv *clusterVolume) dispatchJobConfig(jc *prowconfig.JobConfig, path string, config *dispatcher.Config, jobVolumes map[string]float64) (string, error) {
	cloudProvidersForE2ETests := getCloudProvidersForE2ETests(jc)
	var cloudProvider, cluster string
	var err error
	if cloudProvidersForE2ETests.Len() == 1 {
		cloudProvider, _ = cloudProvidersForE2ETests.PopAny()
	}
	if cluster, err = cv.findClusterForJobConfig(cloudProvider, jc, path, config, jobVolumes); err != nil {
		return "", fmt.Errorf("fail to find cluster for job config: %w", err)
	}
	return cluster, nil
}

type configResult struct {
	cluster  string
	filename string
	path     string
}

// dispatchJobs loads the Prow jobs and chooses a cluster in the build farm if possible.
// The current implementation walks through the Prow Job config files.
// For each file, it tries to assign all jobs in it to a cluster in the build farm.
//   - When all the e2e tests are targeting the same cloud provider, we run the test pod on the that cloud provider too.
//   - When the e2e tests are targeting different cloud providers, or there is no e2e tests at all, we can run the tests
//     on any cluster in the build farm. Those jobs are used to load balance the workload of clusters in the build farm.
func dispatchJobs(ctx context.Context, prowJobConfigDir string, maxConcurrency int, config *dispatcher.Config, jobVolumes map[string]float64, blocked sets.Set[string]) (map[string]string, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	// cv stores the volume for each cluster in the build farm
	cv := &clusterVolume{clusterVolumeMap: map[string]map[string]float64{}, cloudProviders: sets.New[string](), pjs: map[string]string{}, blocked: blocked}
	for cloudProvider, v := range config.BuildFarm {
		for cluster := range v {
			cloudProviderString := string(cloudProvider)
			if _, ok := cv.clusterVolumeMap[cloudProviderString]; !ok {
				cv.clusterVolumeMap[cloudProviderString] = map[string]float64{}
			}
			cv.clusterVolumeMap[cloudProviderString][string(cluster)] = 0
		}
		if len(cv.clusterVolumeMap) > 0 {
			cv.cloudProviders.Insert(string(cloudProvider))
		}
	}

	// no clusters in the build farm
	if len(cv.clusterVolumeMap) == 0 {
		return nil, nil
	}

	sem := semaphore.NewWeighted(int64(maxConcurrency))
	objChan := make(chan interface{})
	var errs []error
	results := map[string][]string{}

	readingDone := make(chan struct{})
	go func() {
		for o := range objChan {
			switch o := o.(type) {
			case configResult:
				if !config.MatchingPathRegEx(o.path) {
					results[o.cluster] = append(results[o.cluster], o.filename)
				}
			case error:
				errs = append(errs, o)
			default:
				// this should never happen
				logrus.Errorf("Received unknown type %T of o with value %v", o, o)
			}
		}
		close(readingDone)
	}()

	if err := filepath.WalkDir(prowJobConfigDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			objChan <- fmt.Errorf("failed to walk file/directory '%s'", path)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			objChan <- fmt.Errorf("failed to acquire semaphore for path %s: %w", path, err)
			return nil
		}
		go func(path string) {
			defer sem.Release(1)

			data, err := gzip.ReadFileMaybeGZIP(path)
			if err != nil {
				objChan <- fmt.Errorf("failed to read file %q: %w", path, err)
				return
			}

			jobConfig := &prowconfig.JobConfig{}
			if err := yaml.Unmarshal(data, jobConfig); err != nil {
				objChan <- fmt.Errorf("failed to unmarshal file %q: %w", path, err)
				return
			}

			cluster, err := cv.dispatchJobConfig(jobConfig, path, config, jobVolumes)
			if err != nil {
				objChan <- fmt.Errorf("failed to dispatch job config %q: %w", path, err)
			}
			objChan <- configResult{cluster: cluster, path: path, filename: info.Name()}
		}(path)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to dispatch all Prow jobs: %w", err)
	}

	if err := sem.Acquire(ctx, int64(maxConcurrency)); err != nil {
		objChan <- fmt.Errorf("failed to acquire semaphore while wating all workers to finish: %w", err)
	}
	close(objChan)
	<-readingDone

	for cloudProvider, m := range cv.clusterVolumeMap {
		for cluster, volume := range m {
			logrus.WithField("cloudProvider", cloudProvider).WithField("cluster", cluster).WithField("volume", volume).Info("dispatched the volume on the cluster")
		}
	}

	for cloudProvider, jobGroups := range config.BuildFarm {
		for cluster := range jobGroups {
			config.BuildFarm[cloudProvider][cluster] = &dispatcher.BuildFarmConfig{FilenamesRaw: results[string(cluster)]}
		}
	}

	return cv.pjs, utilerrors.NewAggregate(errs)
}

// getClusterProvider gets information using get request what is the current cloud provider for the given cluster
func getClusterProvider(cluster string) (api.Cloud, error) {
	type pageData struct {
		Data []map[string]string `json:"data"`
	}
	resp, err := http.Get("https://cluster-display.ci.openshift.org/api/v1/clusters")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var page pageData
	if err := json.Unmarshal(body, &page); err != nil {
		return "", err
	}
	for _, data := range page.Data {
		if errMsg, exists := data["error"]; exists && errMsg == "cannot reach cluster" {
			continue
		}
		if c, exists := data["cluster"]; exists && c == cluster {
			if provider, exists := data["cloud"]; exists {
				return api.Cloud(strings.ToLower(provider)), nil
			}
		}
	}
	return "", fmt.Errorf("have not found provider for cluster %s", cluster)
}

// removeDisabledClusters removes disabled clusters from BuildFarm and BuildFarmConfig
func removeDisabledClusters(config *dispatcher.Config, disabled sets.Set[string]) {
	for provider := range config.BuildFarm {
		for cluster := range config.BuildFarm[provider] {
			if disabled.Has(string(cluster)) {
				delete(config.BuildFarm[provider], cluster)
				if clusters, ok := config.BuildFarmCloud[provider]; ok {
					c := sets.New[string](clusters...)
					c = c.Delete(string(cluster))
					config.BuildFarmCloud[provider] = sets.List(c)
				}
			}
		}
	}
}

type clusterProviderGetter func(cluster string) (api.Cloud, error)

// addEnabledClusters adds enabled clusters to the BuildFarm and BuildFarmConfig
func addEnabledClusters(config *dispatcher.Config, enabled sets.Set[string], getter clusterProviderGetter) {
	if len(enabled) > 0 && config.BuildFarm == nil {
		config.BuildFarm = make(map[api.Cloud]map[api.Cluster]*dispatcher.BuildFarmConfig)
	}
	for cluster := range enabled {
		provider, err := getter(cluster)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to get cluster cloud provider information")
		}
		if _, exists := config.BuildFarm[provider][api.Cluster(cluster)]; !exists {
			if config.BuildFarm[provider] == nil {
				config.BuildFarm[provider] = make(map[api.Cluster]*dispatcher.BuildFarmConfig)
			}
			config.BuildFarm[provider][api.Cluster(cluster)] = &dispatcher.BuildFarmConfig{FilenamesRaw: []string{}, Filenames: sets.New[string]()}
		}
		if clusters, ok := config.BuildFarmCloud[provider]; ok {
			clusters = append(clusters, cluster)
			config.BuildFarmCloud[provider] = clusters
		} else {
			if config.BuildFarmCloud == nil {
				config.BuildFarmCloud = make(map[api.Cloud][]string)
			}
			config.BuildFarmCloud[provider] = []string{cluster}
		}
	}
}

func getEnabledClusters(config *dispatcher.Config) sets.Set[string] {
	enabled := sets.New[string]()
	for _, clusters := range config.BuildFarm {
		for cluster := range clusters {
			enabled.Insert(string(cluster))
		}
	}
	return enabled
}

func getDiffClusters(enabledClusters, clustersFromConfig sets.Set[string]) (clustersToAdd, clustersToRemove sets.Set[string]) {
	return clustersFromConfig.Difference(enabledClusters), enabledClusters.Difference(clustersFromConfig)
}

func clustersMapToSet(clusterMap ClusterMap) sets.Set[string] {
	clusterSet := sets.Set[string]{}
	for cluster := range clusterMap {
		clusterSet.Insert(cluster)
	}
	return clusterSet
}

func createPR(o options) {
	logrus.WithField("targetDir", o.targetDir).Info("Changing working directory ...")
	if err := os.Chdir(o.targetDir); err != nil {
		logrus.WithError(err).Fatal("Failed to change to root dir")
	}

	command := "/usr/bin/sanitize-prow-jobs"
	arguments := []string{"--prow-jobs-dir", "./ci-operator/jobs", "--config-path", "./core-services/sanitize-prow-jobs/_config.yaml"}
	fullCommand := fmt.Sprintf("%s %s", filepath.Base(command), strings.Join(arguments, " "))
	logrus.WithField("fullCommand", fullCommand).Infof("Running the command ...")

	combinedOutput, err := exec.Command(command, arguments...).CombinedOutput()
	if err != nil {
		logrus.WithError(err).WithField("combinedOutput", string(combinedOutput)).Fatal("failed to run the command")
	}

	title := fmt.Sprintf("%s at %s", matchTitle, time.Now().Format(time.RFC1123))
	if err := o.PRCreationOptions.UpsertPR(o.targetDir, githubOrg, githubRepo, upstreamBranch, title, prcreation.PrAssignee(o.assign), prcreation.MatchTitle(matchTitle), prcreation.AdditionalLabels([]string{rehearse.RehearsalsAckLabel})); err != nil {
		logrus.WithError(err).Fatalf("failed to upsert PR")
	}
}

func runAsDaemon(o options, promVolumes *prometheusVolumes) {
	if o.clusterConfigPath == "" {
		logrus.Fatal("mandatory argument --cluster-config-path wasn't set")
	}

	if o.jobsStoragePath == "" {
		logrus.Fatal("mandatory argument --cluster-config-path wasn't set")
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		logrus.Info("Ctrl+C pressed. Exiting immediately.")
		os.Exit(0)
	}()

	var dispatchWrapper func(cron bool)
	prowjobs := newProwjobs(o.jobsStoragePath)
	c := cron.New()

	{
		savedBlocked := sets.Set[string]{}
		var mu sync.Mutex

		dispatchWrapper = func(cron bool) {
			mu.Lock()
			defer mu.Unlock()

			config, err := dispatcher.LoadConfig(o.configPath)
			if err != nil {
				logrus.WithError(err).Errorf("failed to load config from %q", o.configPath)
				return
			}

			configClusterMap, blocked, err := loadClusterConfig(o.clusterConfigPath)
			if err != nil {
				logrus.WithError(err).Error("failed to load cluster config")
				return
			}
			clustersFromConfig := clustersMapToSet(configClusterMap)

			enabled, disabled := getDiffClusters(getEnabledClusters(config), clustersFromConfig)
			if len(disabled) > 0 {
				removeDisabledClusters(config, disabled)
			}

			firstRunOrBlockedChange := !savedBlocked.Equal(blocked)
			if (!cron && enabled.Len() == 0 && disabled.Len() == 0) && !firstRunOrBlockedChange {
				return
			}
			savedBlocked = blocked

			jobVolumes, err := promVolumes.GetJobVolumes()
			if err != nil {
				logrus.WithError(err).Fatal("failed to get job volumes")
			}

			addEnabledClusters(config, enabled,
				func(cluster string) (api.Cloud, error) {
					provider, exists := configClusterMap[cluster]
					if !exists {
						return "", fmt.Errorf("have not found provider for cluster %s", cluster)
					}
					return api.Cloud(provider), nil
				})

			pjs, err := dispatchJobs(context.TODO(), o.prowJobConfigDir, o.maxConcurrency, config, jobVolumes, blocked)
			if err != nil {
				logrus.WithError(err).Error("failed to dispatch")
				return
			}
			prowjobs.regenerate(pjs)

			if err := dispatcher.SaveConfig(config, o.configPath); err != nil {
				logrus.WithError(err).Fatalf("Failed to save config file to %s", o.configPath)
			}

			if err := writeGob(o.jobsStoragePath, pjs); err != nil {
				logrus.Errorf("continuing on cache memory, error writing Gob file: %v", err)
			}

			if o.createPR {
				createPR(o)
			}
		}
	}

	cronDispatchWrapper := func() {
		dispatchWrapper(true)
	}

	_, err := c.AddFunc("0 7 * * 0", cronDispatchWrapper)
	if err != nil {
		logrus.WithError(err).Error("error scheduling cron job")
		return
	}
	c.Start()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.WithError(err).Fatal("failed setup watcher")
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Remove) != 0 {
					dispatchWrapper(false)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				logrus.WithError(err)
			}
		}
	}()

	err = watcher.Add(o.clusterConfigPath)
	if err != nil {
		logrus.Fatal(err)
	}

	dispatchWrapper(false)

	server := newServer(prowjobs)
	http.HandleFunc("/", server.requestHandler)
	logrus.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	o := gatherOptions()
	if err := o.validate(); err != nil {
		logrus.WithError(err).Fatal("Failed to complete options.")
	}

	if o.createPR {
		if err := o.PRCreationOptions.Finalize(); err != nil {
			logrus.WithError(err).Fatal("Failed to finalize PR creation options")
		}
	}

	if o.PrometheusOptions.PrometheusPasswordPath != "" {
		if err := secret.Add(o.PrometheusOptions.PrometheusPasswordPath); err != nil {
			logrus.WithError(err).Fatal("Failed to start secrets agent")
		}
	}

	if o.PrometheusOptions.PrometheusBearerTokenPath != "" {
		if err := secret.Add(o.PrometheusOptions.PrometheusBearerTokenPath); err != nil {
			logrus.WithError(err).Fatal("Failed to start secrets agent")
		}
	}

	promVolumes, err := newPrometheusVolumes(o.PrometheusOptions, o.prometheusDaysBefore)
	if err != nil {
		logrus.WithError(err).Fatal("failed to create prometheus volumes")
	}

	if o.daemonize {
		runAsDaemon(o, &promVolumes)
		return
	}

	jobVolumes, err := promVolumes.GetJobVolumes()
	if err != nil {
		logrus.WithError(err).Fatal("failed to get job volumes")
	}

	config, err := dispatcher.LoadConfig(o.configPath)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to load config from %q", o.configPath)
	}

	if o.defaultCluster != "" {
		config.Default = api.Cluster(o.defaultCluster)
	}

	enabled := o.enableClusters.StringSet()
	disabled := o.disableClusters.StringSet()
	if len(disabled) > 0 {
		removeDisabledClusters(config, disabled)
	}
	addEnabledClusters(config, enabled, getClusterProvider)

	logrus.Info("Dispatching ...")
	if _, err := dispatchJobs(context.TODO(), o.prowJobConfigDir, o.maxConcurrency, config, jobVolumes, sets.Set[string](sets.NewString())); err != nil {
		logrus.WithError(err).Fatal("Failed to dispatch")
	}
	if err := dispatcher.SaveConfig(config, o.configPath); err != nil {
		logrus.WithError(err).Fatalf("Failed to save config file to %s", o.configPath)
	}

	if !o.createPR {
		logrus.Info("Finished dispatching and create no PR, exiting ...")
		os.Exit(0)
	}
	createPR(o)
}
