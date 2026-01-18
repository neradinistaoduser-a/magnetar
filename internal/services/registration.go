package services

import (
	"context"

	"github.com/c12s/magnetar/internal/domain"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type RegistrationService struct {
	nodeRepo domain.NodeRepo
}

func NewRegistrationService(nodeRepo domain.NodeRepo) (*RegistrationService, error) {
	return &RegistrationService{
		nodeRepo: nodeRepo,
	}, nil
}

func (r *RegistrationService) Register(ctx context.Context, req domain.RegistrationReq) (*domain.RegistrationResp, error) {
	tracer := otel.Tracer("magnetar.RegistrationService")
	ctx, span := tracer.Start(ctx, "Register")
	defer span.End()

	node := domain.Node{
		Id: domain.NodeId{
			Value: generateNodeId(),
		},
		Labels:      req.Labels,
		Resources:   req.Resources,
		BindAddress: req.BindAddress,
	}

	err := r.nodeRepo.Put(ctx, node)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &domain.RegistrationResp{
		NodeId: node.Id.Value,
	}, nil
}

func generateNodeId() string {
	return uuid.NewString()
}
