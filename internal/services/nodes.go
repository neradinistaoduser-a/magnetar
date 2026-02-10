package services

import (
	"context"
	"log"

	gravity_api "github.com/c12s/agent_queue/pkg/api"
	"github.com/c12s/magnetar/internal/domain"
	meridian_api "github.com/c12s/meridian/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type NodeService struct {
	nodeRepo      domain.NodeRepo
	administrator *oortapi.AdministrationAsyncClient
	authorizer    AuthZService
	meridian      meridian_api.MeridianClient
	gravity       gravity_api.AgentQueueClient
}

func NewNodeService(
	nodeRepo domain.NodeRepo,
	evaluator oortapi.OortEvaluatorClient,
	administrator *oortapi.AdministrationAsyncClient,
	authorizer AuthZService,
	meridian meridian_api.MeridianClient,
	gravity gravity_api.AgentQueueClient,
) (*NodeService, error) {
	return &NodeService{
		nodeRepo:      nodeRepo,
		administrator: administrator,
		authorizer:    authorizer,
		meridian:      meridian,
		gravity:       gravity,
	}, nil
}

func (n *NodeService) GetFromNodePool(ctx context.Context, req domain.GetFromNodePoolReq) (*domain.GetFromNodePoolResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.GetFromNodePool")
	defer span.End()

	node, err := n.nodeRepo.Get(ctx, req.Id, "")
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.GetFromNodePoolResp{Node: *node}, nil
}

func (n *NodeService) GetFromOrg(ctx context.Context, req domain.GetFromOrgReq) (*domain.GetFromOrgResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.GetFromOrg")
	defer span.End()

	if !n.authorizer.Authorize(ctx, "node.get", "node", req.Id.Value) {
		err := domain.ErrForbidden
		span.RecordError(err)
		return nil, err
	}

	node, err := n.nodeRepo.Get(ctx, req.Id, req.Org)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.GetFromOrgResp{Node: *node}, nil
}

func (n *NodeService) ClaimOwnership(ctx context.Context, req domain.ClaimOwnershipReq) (*domain.ClaimOwnershipResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.ClaimOwnership")
	defer span.End()

	if !n.authorizer.Authorize(ctx, "node.put", "org", req.Org) {
		err := domain.ErrForbidden
		span.RecordError(err)
		return nil, err
	}

	cluster, err := n.nodeRepo.ListOrgOwnedNodes(ctx, req.Org)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	nodes, err := n.nodeRepo.QueryNodePool(ctx, req.Query)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	for _, node := range nodes {
		if err := n.nodeRepo.Delete(ctx, node); err != nil {
			log.Println(err)
			span.RecordError(err)
			continue
		}
		node.Org = req.Org
		if err := n.nodeRepo.Put(ctx, node); err != nil {
			log.Println(err)
			span.RecordError(err)
			continue
		}
		if err := n.administrator.SendRequest(ctx, &oortapi.CreateInheritanceRelReq{
			From: &oortapi.Resource{Id: req.Org, Kind: "org"},
			To:   &oortapi.Resource{Id: node.Id.Value, Kind: "node"},
		}, func(resp *oortapi.AdministrationAsyncResp) {
			if resp.Error != "" {
				log.Println(resp.Error)
			}
		}); err != nil {
			log.Println(err)
			span.RecordError(err)
		}
	}

	listNodesResp, err := n.ListOrgOwnedNodes(ctx, domain.ListOrgOwnedNodesReq{Org: req.Org})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	resources := make(map[string]float64)
	for _, node := range listNodesResp.Nodes {
		for resource, quota := range node.Resources {
			resources[resource] += quota
		}
	}

	ctx = setOutgoingContext(ctx)
	_, err = n.meridian.GetNamespace(ctx, &meridian_api.GetNamespaceReq{OrgId: req.Org, Name: "default"})
	if err != nil {
		status, _ := status.FromError(err)
		if status.Code() == codes.NotFound {
			_, err = n.meridian.AddNamespace(ctx, &meridian_api.AddNamespaceReq{
				OrgId:                     req.Org,
				Name:                      "default",
				Labels:                    make(map[string]string),
				Quotas:                    resources,
				SeccompDefinitionStrategy: "redefine",
				Profile: &meridian_api.SeccompProfile{
					Version:       "v1.0.0",
					DefaultAction: "ALLOW",
					Syscalls:      []*meridian_api.SyscallRule{},
				},
			})
			if err != nil {
				log.Println(err)
				span.RecordError(err)
			}
		}
	} else {
		_, err = n.meridian.SetNamespaceResources(ctx, &meridian_api.SetNamespaceResourcesReq{
			OrgId:  req.Org,
			Name:   "default",
			Quotas: resources,
		})
		if err != nil {
			log.Println(err)
			span.RecordError(err)
		}
	}

	if len(nodes) == 0 {
		return &domain.ClaimOwnershipResp{Nodes: nodes}, nil
	}

	joinAddress := nodes[0].BindAddress
	if len(cluster) > 0 {
		joinAddress = cluster[0].BindAddress
	}

	for _, node := range nodes {
		ctx = setOutgoingContext(ctx)
		if _, err := n.gravity.JoinCluster(ctx, &gravity_api.JoinClusterRequest{
			NodeId:      node.Id.Value,
			JoinAddress: joinAddress,
			ClusterId:   req.Org,
		}); err != nil {
			log.Println(err)
			span.RecordError(err)
		}
	}

	return &domain.ClaimOwnershipResp{Nodes: nodes}, nil
}

func (n *NodeService) ListNodePool(ctx context.Context, req domain.ListNodePoolReq) (*domain.ListNodePoolResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.ListNodePool")
	defer span.End()

	nodes, err := n.nodeRepo.ListNodePool(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.ListNodePoolResp{Nodes: nodes}, nil
}

func (n *NodeService) ListOrgOwnedNodes(ctx context.Context, req domain.ListOrgOwnedNodesReq) (*domain.ListOrgOwnedNodesResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.ListOrgOwnedNodes")
	defer span.End()

	if !n.authorizer.Authorize(ctx, "node.get", "org", req.Org) {
		err := domain.ErrForbidden
		span.RecordError(err)
		return nil, err
	}

	nodes, err := n.nodeRepo.ListOrgOwnedNodes(ctx, req.Org)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.ListOrgOwnedNodesResp{Nodes: nodes}, nil
}

func (n *NodeService) ListAllNodes(ctx context.Context) ([]domain.Node, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.ListAllNodes")
	defer span.End()

	nodes, err := n.nodeRepo.ListAllNodes(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return nodes, nil
}

func (n *NodeService) QueryNodePool(ctx context.Context, req domain.QueryNodePoolReq) (*domain.QueryNodePoolResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.QueryNodePool")
	defer span.End()

	nodes, err := n.nodeRepo.QueryNodePool(ctx, req.Query)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.QueryNodePoolResp{Nodes: nodes}, nil
}

func (n *NodeService) QueryOrgOwnedNodes(ctx context.Context, req domain.QueryOrgOwnedNodesReq) (*domain.QueryOrgOwnedNodesResp, error) {
	tracer := otel.Tracer("magnetar.NodeService")
	ctx, span := tracer.Start(ctx, "NodeService.QueryOrgOwnedNodes")
	defer span.End()

	if !n.authorizer.Authorize(ctx, "node.get", "org", req.Org) {
		err := domain.ErrForbidden
		span.RecordError(err)
		return nil, err
	}

	nodes, err := n.nodeRepo.QueryOrgOwnedNodes(ctx, req.Query, req.Org)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &domain.QueryOrgOwnedNodesResp{Nodes: nodes}, nil
}

func setOutgoingContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}
