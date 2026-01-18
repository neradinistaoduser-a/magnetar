package services

import (
	"context"
	"log"

	"github.com/c12s/magnetar/internal/domain"
	oortapi "github.com/c12s/oort/pkg/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type LabelService struct {
	nodeRepo   domain.NodeRepo
	authorizer AuthZService
}

func NewLabelService(nodeRepo domain.NodeRepo, evaluator oortapi.OortEvaluatorClient, authorizer AuthZService) (*LabelService, error) {
	return &LabelService{
		nodeRepo:   nodeRepo,
		authorizer: authorizer,
	}, nil
}

func (l *LabelService) PutLabel(ctx context.Context, req domain.PutLabelReq) (*domain.PutLabelResp, error) {
	tracer := otel.Tracer("magnetar.LabelService")
	ctx, span := tracer.Start(ctx, "PutLabel")
	defer span.End()

	span.SetAttributes(
		attribute.String("nodeId", req.NodeId.Value),
		attribute.String("label", req.Label.Key()),
	)

	if !l.authorizer.Authorize(ctx, "node.label.put", "node", req.NodeId.Value) {
		span.AddEvent("authorization failed")
		return nil, domain.ErrForbidden
	}
	span.AddEvent("authorization granted")

	node, err := l.nodeRepo.Get(ctx, req.NodeId, req.Org)
	if err != nil {
		span.RecordError(err)
		log.Println("Get node error:", err)
		return nil, err
	}

	node, err = l.nodeRepo.PutLabel(ctx, *node, req.Label)
	if err != nil {
		span.RecordError(err)
		log.Println("PutLabel error:", err)
		return nil, err
	}

	span.AddEvent("label updated")
	return &domain.PutLabelResp{
		Node: *node,
	}, nil
}

func (l *LabelService) DeleteLabel(ctx context.Context, req domain.DeleteLabelReq) (*domain.DeleteLabelResp, error) {
	tracer := otel.Tracer("magnetar.LabelService")
	ctx, span := tracer.Start(ctx, "DeleteLabel")
	defer span.End()

	span.SetAttributes(
		attribute.String("nodeId", req.NodeId.Value),
		attribute.String("labelKey", req.LabelKey),
	)

	if !l.authorizer.Authorize(ctx, "node.label.delete", "node", req.NodeId.Value) {
		span.AddEvent("authorization failed")
		return nil, domain.ErrForbidden
	}
	span.AddEvent("authorization granted")

	node, err := l.nodeRepo.Get(ctx, req.NodeId, req.Org)
	if err != nil {
		span.RecordError(err)
		log.Println("Get node error:", err)
		return nil, err
	}

	node, err = l.nodeRepo.DeleteLabel(ctx, *node, req.LabelKey)
	if err != nil {
		span.RecordError(err)
		log.Println("DeleteLabel error:", err)
		return nil, err
	}

	span.AddEvent("label deleted")
	return &domain.DeleteLabelResp{
		Node: *node,
	}, nil
}
