package repos

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/c12s/magnetar/internal/domain"
	"github.com/juliangruber/go-intersect"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel"
	"golang.org/x/exp/slices"
)

// data model
// for get operations
// key - nodes/pool/{nodeId} | nodes/orgs/{orgId}/{nodeId}
// value - protobuf node (id + org + labels)
// for query operations
// key - labels/pool/{labelKey}/{nodeId} | labels/orgs/{orgId}/{labelKey}/{nodeId}
// value - protobuf label (key + value)

type nodeEtcdRepo struct {
	etcd            *etcd.Client
	nodeMarshaller  domain.NodeMarshaller
	labelMarshaller domain.LabelMarshaller
}

func NewNodeEtcdRepo(cli *etcd.Client, nodeMarshaller domain.NodeMarshaller, labelMarshaller domain.LabelMarshaller) (domain.NodeRepo, error) {
	return &nodeEtcdRepo{
		etcd:            cli,
		nodeMarshaller:  nodeMarshaller,
		labelMarshaller: labelMarshaller,
	}, nil
}

func (n nodeEtcdRepo) Put(ctx context.Context, node domain.Node) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.Put")
	defer span.End()
	err := n.putNodeGetModel(ctx, node)
	if err != nil {
		return err
	}
	return n.putNodeQueryModel(ctx, node)
}

func (n nodeEtcdRepo) Delete(ctx context.Context, node domain.Node) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.Delete")
	defer span.End()
	err := n.deleteNodeGetModel(ctx, node)
	if err != nil {
		return err
	}
	return n.deleteNodeQueryModel(ctx, node)
}

func (n nodeEtcdRepo) Get(ctx context.Context, nodeId domain.NodeId, org string) (*domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.Get")
	defer span.End()
	key := getKey(domain.Node{Id: nodeId, Org: org})
	resp, err := n.etcd.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, errors.New("node not found")
	}
	return n.nodeMarshaller.Unmarshal(resp.Kvs[0].Value)
}

func (n nodeEtcdRepo) ListNodePool(ctx context.Context) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.ListNodePool")
	defer span.End()
	keyPrefix := fmt.Sprintf("%s/pool", getKeyPrefix)
	return n.listNodes(ctx, keyPrefix)
}

func (n nodeEtcdRepo) ListOrgOwnedNodes(ctx context.Context, org string) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.ListOrgOwnedNodes")
	defer span.End()
	keyPrefix := fmt.Sprintf("%s/orgs/%s", getKeyPrefix, org)
	return n.listNodes(ctx, keyPrefix)
}

func (n nodeEtcdRepo) ListAllNodes(ctx context.Context) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.ListAllNodes")
	defer span.End()
	return n.listNodes(ctx, getKeyPrefix)
}

func (n nodeEtcdRepo) QueryNodePool(ctx context.Context, query domain.Query) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.QueryNodePool")
	defer span.End()
	keyPrefix := fmt.Sprintf("%s/pool", queryKeyPrefix)
	if len(query) == 0 {
		return n.listNodes(ctx, keyPrefix)
	}
	nodeIds, err := n.queryNodes(ctx, query, keyPrefix)
	if err != nil {
		return nil, err
	}
	nodes := make([]domain.Node, 0)
	for _, nodeId := range nodeIds {
		node, err := n.Get(ctx, nodeId, "")
		if err != nil {
			log.Println(err)
			continue
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (n nodeEtcdRepo) QueryOrgOwnedNodes(ctx context.Context, query domain.Query, org string) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.QueryOrgOwnedNodes")
	defer span.End()
	keyPrefix := fmt.Sprintf("%s/orgs/%s", queryKeyPrefix, org)
	if len(query) == 0 {
		return n.ListOrgOwnedNodes(ctx, org)
	}
	nodeIds, err := n.queryNodes(ctx, query, keyPrefix)
	if err != nil {
		return nil, err
	}
	nodes := make([]domain.Node, 0)
	for _, nodeId := range nodeIds {
		node, err := n.Get(ctx, nodeId, org)
		if err != nil {
			log.Println(err)
			continue
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (n nodeEtcdRepo) listNodes(ctx context.Context, keyPrefix string) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.listNodes")
	defer span.End()
	nodes := make([]domain.Node, 0)
	resp, err := n.etcd.Get(ctx, keyPrefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range resp.Kvs {
		node, err := n.nodeMarshaller.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

func (n nodeEtcdRepo) PutLabel(ctx context.Context, node domain.Node, label domain.Label) (*domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.PutLabel")
	defer span.End()
	err := n.putLabelGetModel(ctx, node, label)
	if err != nil {
		return nil, err
	}
	err = n.putLabelQueryModel(ctx, node, label)
	if err != nil {
		return nil, err
	}
	return n.Get(ctx, node.Id, node.Org)
}

func (n nodeEtcdRepo) DeleteLabel(ctx context.Context, node domain.Node, labelKey string) (*domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.DeleteLabel")
	defer span.End()
	err := n.deleteLabelGetModel(ctx, node, labelKey)
	if err != nil {
		return nil, err
	}
	err = n.deleteLabelQueryModel(ctx, node, labelKey)
	if err != nil {
		return nil, err
	}
	return n.Get(ctx, node.Id, node.Org)
}

func (n nodeEtcdRepo) deleteNodeGetModel(ctx context.Context, node domain.Node) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.deleteNodeGetModel")
	defer span.End()
	key := getKey(node)
	_, err := n.etcd.Delete(ctx, key)
	return err
}

func (n nodeEtcdRepo) deleteNodeQueryModel(ctx context.Context, node domain.Node) (err error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.deleteNodeQueryModel")
	defer span.End()
	for _, label := range node.Labels {
		key := queryKey(node, label.Key())
		_, delErr := n.etcd.Delete(ctx, key)
		err = errors.Join(err, delErr)
	}
	return err
}

func (n nodeEtcdRepo) putNodeGetModel(ctx context.Context, node domain.Node) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.putNodeGetModel")
	defer span.End()
	nodeMarshalled, err := n.nodeMarshaller.Marshal(node)
	if err != nil {
		return err
	}
	key := getKey(node)
	_, err = n.etcd.Put(ctx, key, string(nodeMarshalled))
	return err
}

func (n nodeEtcdRepo) putLabelGetModel(ctx context.Context, node domain.Node, label domain.Label) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.putLabelGetModel")
	defer span.End()
	labelIndex := -1
	for i, nodeLabel := range node.Labels {
		if nodeLabel.Key() == label.Key() {
			labelIndex = i
		}
	}
	if labelIndex >= 0 {
		node.Labels[labelIndex] = label
	} else {
		node.Labels = append(node.Labels, label)
	}
	return n.putNodeGetModel(ctx, node)
}

func (n nodeEtcdRepo) deleteLabelGetModel(ctx context.Context, node domain.Node, labelKey string) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.deleteLabelGetModel")
	defer span.End()
	labelIndex := findIndexByLabelKey(node, labelKey)
	if labelIndex >= 0 {
		node.Labels = slices.Delete(node.Labels, labelIndex, labelIndex+1)
		return n.putNodeGetModel(ctx, node)
	}
	return domain.ErrNotFound("label")
}

func (n nodeEtcdRepo) putNodeQueryModel(ctx context.Context, node domain.Node) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.putNodeQueryModel")
	defer span.End()
	for _, label := range node.Labels {
		err := n.putLabelQueryModel(ctx, node, label)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n nodeEtcdRepo) putLabelQueryModel(ctx context.Context, node domain.Node, label domain.Label) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.putLabelQueryModel")
	defer span.End()
	labelMarshalled, err := n.labelMarshaller.Marshal(label)
	if err != nil {
		return err
	}
	key := queryKey(node, label.Key())
	_, err = n.etcd.Put(ctx, key, string(labelMarshalled))
	return err
}

func (n nodeEtcdRepo) deleteLabelQueryModel(ctx context.Context, node domain.Node, labelKey string) error {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.deleteLabelQueryModel")
	defer span.End()
	key := queryKey(node, labelKey)
	_, err := n.etcd.Delete(ctx, key)
	return err
}

func (n nodeEtcdRepo) queryNodes(ctx context.Context, query domain.Query, keyPrefix string) ([]domain.NodeId, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.DeletqueryNodeseLabel")
	defer span.End()
	nodeIds := make([]domain.NodeId, 0)
	for i, selector := range query {
		currNodes, err := n.selectNodes(ctx, selector, keyPrefix)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			nodeIds = currNodes
		} else {
			intersection := intersect.Simple(nodeIds, currNodes)
			nodeIds = make([]domain.NodeId, len(intersection))
			for i, node := range intersection {
				nodeIds[i] = node.(domain.NodeId)
			}
		}
	}
	return nodeIds, nil
}

func (n nodeEtcdRepo) selectNodes(ctx context.Context, selector domain.Selector, keyPrefix string) ([]domain.NodeId, error) {
	tracer := otel.Tracer("magnetar.NodeRepo")
	ctx, span := tracer.Start(ctx, "NodeRepo.selectNodes")
	defer span.End()
	prefix := fmt.Sprintf("%s/%s/", keyPrefix, selector.LabelKey)
	resp, err := n.etcd.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, domain.ErrNotFound("nodes")
	}

	nodeIds := make([]domain.NodeId, 0)
	for _, kv := range resp.Kvs {
		nodeLabel, err := n.labelMarshaller.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		cmpResult, err := nodeLabel.Compare(selector.Value)
		if err != nil {
			log.Println(err)
			continue
		}
		if slices.Contains(cmpResult, selector.ShouldBe) {
			nodeId := extractNodeIdFromQueryKey(string(kv.Key))
			nodeIds = append(nodeIds, domain.NodeId{
				Value: nodeId,
			})
		}
	}
	return nodeIds, nil
}

const (
	getKeyPrefix   = "nodes"
	queryKeyPrefix = "labels"
)

func getKey(node domain.Node) string {
	if node.Claimed() {
		return fmt.Sprintf("%s/orgs/%s/%s", getKeyPrefix, node.Org, node.Id.Value)
	}
	return fmt.Sprintf("%s/pool/%s", getKeyPrefix, node.Id.Value)
}

func queryKey(node domain.Node, labelKey string) string {
	if node.Claimed() {
		return fmt.Sprintf("%s/orgs/%s/%s/%s", queryKeyPrefix, node.Org, labelKey, node.Id.Value)
	}
	return fmt.Sprintf("%s/pool/%s/%s", queryKeyPrefix, labelKey, node.Id.Value)
}

func extractNodeIdFromQueryKey(key string) string {
	if strings.HasPrefix(key, fmt.Sprintf("%s/pool", queryKeyPrefix)) {
		return strings.Split(key, "/")[3]
	}
	return strings.Split(key, "/")[4]
}

func findIndexByLabelKey(node domain.Node, labelKey string) int {
	labelIndex := -1
	for i, nodeLabel := range node.Labels {
		if nodeLabel.Key() == labelKey {
			labelIndex = i
			break
		}
	}

	return labelIndex

}
