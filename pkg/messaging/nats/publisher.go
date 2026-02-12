package nats

import (
	"context"
	"errors"

	"github.com/c12s/magnetar/pkg/messaging"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type publisher struct {
	conn *nats.Conn
}

func NewPublisher(conn *nats.Conn) (messaging.Publisher, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, errors.New("connection error")
	}
	return &publisher{
		conn: conn,
	}, nil
}

func (p publisher) Publish(ctx context.Context, msg []byte, subject string) error {

	tracer := otel.Tracer("nats.publisher")
	ctx, span := tracer.Start(ctx, "NATS Publish "+subject)
	defer span.End()

	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    msg,
		Header:  nats.Header{},
	}

	otel.GetTextMapPropagator().Inject(ctx,
		propagation.HeaderCarrier(natsMsg.Header),
	)

	return p.conn.PublishMsg(natsMsg)
}

func (p publisher) Request(ctx context.Context, msg []byte, subject, replySubject string) error {

	tracer := otel.Tracer("nats.publisher")
	ctx, span := tracer.Start(ctx, "NATS Request "+subject)
	defer span.End()

	natsMsg := &nats.Msg{
		Subject: subject,
		Reply:   replySubject,
		Data:    msg,
		Header:  nats.Header{},
	}

	otel.GetTextMapPropagator().Inject(ctx,
		propagation.HeaderCarrier(natsMsg.Header),
	)

	return p.conn.PublishMsg(natsMsg)
}

func (p publisher) GenerateReplySubject() string {
	return nats.NewInbox()
}
