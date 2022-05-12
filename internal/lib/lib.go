package lib

import (
	"context"
	"fmt"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"strings"
)

const (
	jmxExporterPort  = 11001
	nodeExporterPort = 11002
)

type SDClient struct {
	JobPrefix  string
	AwsRegions []string
}

// (ClusterDetails).generateSDEntry generates a PrometheusSDEntry based on the cluster's details
func (sdc SDClient) generateSDEntry(cd clusterDetails) PrometheusSDEntry {
	ret := PrometheusSDEntry{}
	ret.Labels = labels{
		Job:         strings.Join([]string{sdc.JobPrefix, cd.ClusterName}, "-"),
		ClusterName: cd.ClusterName,
		ClusterArn:  cd.ClusterArn,
	}

	var targets []string
	for _, b := range cd.Brokers {
		if cd.JmxExporter {
			targets = append(targets, fmt.Sprintf("%s:%d", b, jmxExporterPort))
		}
		if cd.NodeExporter {
			targets = append(targets, fmt.Sprintf("%s:%d", b, nodeExporterPort))
		}
	}
	ret.Targets = targets
	return ret
}

func (sdc SDClient) DiscoverRegion(svc kafkaClient) ([]PrometheusSDEntry, error) {
	var entries []PrometheusSDEntry

	clusters, err := listClustersInRegion(svc)
	if err != nil {
		return entries, err
	}

	for _, clusterInfo := range clusters.ClusterInfoList {
		brokers, err := listClusterNodes(svc, *clusterInfo.ClusterArn)
		if err != nil {
			return entries, err
		}
		clusterDetails := buildClusterDetails(clusterInfo, brokers)
		if !clusterDetails.JmxExporter && !clusterDetails.NodeExporter {
			continue
		}
		entries = append(entries, sdc.generateSDEntry(clusterDetails))
	}

	return entries, nil
}

func (sdc SDClient) DiscoverAllRegions() ([]PrometheusSDEntry, error) {
	var output []PrometheusSDEntry

	for _, region := range sdc.AwsRegions {
		cfg, err := awsConfig.LoadDefaultConfig(context.TODO(), awsConfig.WithRegion(region))
		if err != nil {
			return nil, err
		}
		kafkaClient := kafka.NewFromConfig(cfg)

		regionOutput, err := sdc.DiscoverRegion(kafkaClient)
		if err != nil {
			return output, err
		}

		output = append(output, regionOutput...)
	}
	return output, nil
}

type kafkaClient interface {
	ListClusters(ctx context.Context, params *kafka.ListClustersInput, optFns ...func(*kafka.Options)) (*kafka.ListClustersOutput, error)
	ListNodes(ctx context.Context, params *kafka.ListNodesInput, optFns ...func(*kafka.Options)) (*kafka.ListNodesOutput, error)
}

type labels struct {
	Job         string `yaml:"job"`
	ClusterName string `yaml:"cluster_name"`
	ClusterArn  string `yaml:"cluster_arn"`
}

// PrometheusSDEntry is the final structure of a single static config that
// will be outputted to the Prometheus file service discovery config
type PrometheusSDEntry struct {
	Targets []string `yaml:"targets"`
	Labels  labels   `yaml:"labels"`
}

// clusterDetails holds details of cluster, each broker, and which OpenMetrics endpoints are enabled
type clusterDetails struct {
	ClusterName  string
	ClusterArn   string
	Brokers      []string
	JmxExporter  bool
	NodeExporter bool
}

// listClustersInRegion returns a ListClusterOutput of MSK cluster details
func listClustersInRegion(svc kafkaClient) (*kafka.ListClustersOutput, error) {
	input := &kafka.ListClustersInput{}
	output := &kafka.ListClustersOutput{}

	p := kafka.NewListClustersPaginator(svc, input)
	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return &kafka.ListClustersOutput{}, err
		}
		output.ClusterInfoList = append(output.ClusterInfoList, page.ClusterInfoList...)
	}
	return output, nil
}

// listClusterNodes returns a slice of broker hosts without ports
func listClusterNodes(svc kafkaClient, arn string) ([]string, error) {
	input := kafka.ListNodesInput{ClusterArn: &arn}
	var brokers []string

	p := kafka.NewListNodesPaginator(svc, &input)
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, b := range page.NodeInfoList {
			brokers = append(brokers, b.BrokerNodeInfo.Endpoints...)
		}
	}

	return brokers, nil
}

// buildClusterDetails extracts the relevant details from a ClusterInfo and returns a ClusterDetails
func buildClusterDetails(clusterInfo types.ClusterInfo, brokers []string) clusterDetails {
	cluster := clusterDetails{
		ClusterName:  *clusterInfo.ClusterName,
		ClusterArn:   *clusterInfo.ClusterArn,
		Brokers:      brokers,
		JmxExporter:  clusterInfo.OpenMonitoring.Prometheus.JmxExporter.EnabledInBroker,
		NodeExporter: clusterInfo.OpenMonitoring.Prometheus.NodeExporter.EnabledInBroker,
	}
	return cluster
}
