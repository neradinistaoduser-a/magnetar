package servers

import (
	"context"
	"log"

	"github.com/c12s/magnetar/internal/mappers/proto"
	"github.com/c12s/magnetar/internal/services"
	"github.com/c12s/magnetar/pkg/api"
	"github.com/c12s/magnetar/pkg/messaging"
	"go.opentelemetry.io/otel"
)

type RegistrationAsyncServer struct {
	subscriber messaging.Subscriber
	publisher  messaging.Publisher
	service    *services.RegistrationService
}

func NewRegistrationAsyncServer(subscriber messaging.Subscriber, publisher messaging.Publisher, service *services.RegistrationService) (*RegistrationAsyncServer, error) {
	return &RegistrationAsyncServer{
		subscriber: subscriber,
		publisher:  publisher,
		service:    service,
	}, nil
}

func (n *RegistrationAsyncServer) Serve() error {
	return n.subscriber.Subscribe(n.register)
}

func (n *RegistrationAsyncServer) register(msg []byte, replySubject string) {
	ctx := context.Background()
	tracer := otel.Tracer("magnetar.RegistrationAsyncServer")
	ctx, span := tracer.Start(ctx, "RegisterMessage")
	defer span.End()

	reqProto := &api.RegistrationReq{}
	if err := reqProto.Unmarshal(msg); err != nil {
		span.RecordError(err)
		log.Println(err)
		return
	}

	req, err := proto.RegistrationReqToDomain(reqProto)
	if err != nil {
		span.RecordError(err)
		log.Println(err)
		return
	}

	resp, err := n.service.Register(ctx, *req)
	if err != nil {
		span.RecordError(err)
		log.Println(err)
		return
	}

	respProto, err := proto.RegistrationRespFromDomain(*resp)
	if err != nil {
		span.RecordError(err)
		log.Println(err)
		return
	}

	respMarshalled, err := respProto.Marshal()
	if err != nil {
		span.RecordError(err)
		log.Println(err)
		return
	}

	if err := n.publisher.Publish(respMarshalled, replySubject); err != nil {
		span.RecordError(err)
		log.Println(err)
	}
}

func (n *RegistrationAsyncServer) GracefulStop() {
	if err := n.subscriber.Unsubscribe(); err != nil {
		log.Println(err)
	}
}
