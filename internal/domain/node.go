package domain

import "context"

type Node struct {
	Id          NodeId
	Org         string
	Labels      []Label
	Resources   map[string]float64
	BindAddress string
}

func (n Node) Claimed() bool {
	return len(n.Org) > 0
}

type NodeId struct {
	Value string
}

type Query []Selector

type Selector struct {
	LabelKey string
	ShouldBe ComparisonResult
	Value    string
}

type NodeRepo interface {
	Put(ctx context.Context, node Node) error
	Get(ctx context.Context, nodeId NodeId, org string) (*Node, error)
	Delete(ctx context.Context, node Node) error
	ListNodePool(ctx context.Context) ([]Node, error)
	ListOrgOwnedNodes(ctx context.Context, org string) ([]Node, error)
	ListAllNodes(ctx context.Context) ([]Node, error)
	QueryNodePool(ctx context.Context, query Query) ([]Node, error)
	QueryOrgOwnedNodes(ctx context.Context, query Query, org string) ([]Node, error)
	PutLabel(ctx context.Context, node Node, label Label) (*Node, error)
	DeleteLabel(ctx context.Context, node Node, labelKey string) (*Node, error)
}

type NodeMarshaller interface {
	Marshal(node Node) ([]byte, error)
	Unmarshal(nodeMarshalled []byte) (*Node, error)
}

type GetFromNodePoolReq struct {
	Id NodeId
}

type GetFromNodePoolResp struct {
	Node Node
}

type GetFromOrgReq struct {
	Id  NodeId
	Org string
}

type GetFromOrgResp struct {
	Node Node
}

type ClaimOwnershipReq struct {
	Query Query
	Org   string
}

type ClaimOwnershipResp struct {
	Nodes []Node
}

type ListNodePoolReq struct {
}

type ListNodePoolResp struct {
	Nodes []Node
}

type ListOrgOwnedNodesReq struct {
	Org string
}

type ListOrgOwnedNodesResp struct {
	Nodes []Node
}

type QueryNodePoolReq struct {
	Query Query
}

type QueryNodePoolResp struct {
	Nodes []Node
}

type QueryOrgOwnedNodesReq struct {
	Query Query
	Org   string
}

type QueryOrgOwnedNodesResp struct {
	Nodes []Node
}
