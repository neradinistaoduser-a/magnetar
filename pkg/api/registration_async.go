package api

import (
	context "context"
	"fmt"
	"log"

	"github.com/c12s/magnetar/pkg/messaging"
	"github.com/c12s/magnetar/pkg/messaging/nats"
	"github.com/golang/protobuf/proto"
	natsgo "github.com/nats-io/nats.go"
)

type RegistrationAsyncClient struct {
	publisher         messaging.Publisher
	subscriberFactory func(subject string) messaging.Subscriber
}

func NewRegistrationAsyncClient(natsAddress string) (*RegistrationAsyncClient, error) {
	conn, err := natsgo.Connect(fmt.Sprintf("nats://%s", natsAddress))
	if err != nil {
		return nil, err
	}
	publisher, err := nats.NewPublisher(conn)
	if err != nil {
		return nil, err
	}
	subscriberFactory := func(subject string) messaging.Subscriber {
		subscriber, _ := nats.NewSubscriber(conn, subject, "")
		return subscriber
	}
	return &RegistrationAsyncClient{
		publisher:         publisher,
		subscriberFactory: subscriberFactory,
	}, nil
}

func (n *RegistrationAsyncClient) Register(ctx context.Context, req *RegistrationReq, callback RegistrationCallback) error {
	reqMarshalled, err := req.Marshal()
	if err != nil {
		return err
	}

	replySubject := n.publisher.GenerateReplySubject()
	subscriber := n.subscriberFactory(replySubject)
	err = subscriber.Subscribe(func(subCtx context.Context, msg []byte, _ string) {
		resp := &RegistrationResp{}
		err := resp.Unmarshal(msg)
		if err != nil {
			log.Println(err)
			return
		}
		callback(resp)
	})
	if err != nil {
		return err
	}

	// send request
	err = n.publisher.Request(ctx, reqMarshalled, RegistrationSubject, replySubject)
	if err != nil {
		_ = subscriber.Unsubscribe()
		return err
	}
	return nil
}

type RegistrationCallback func(resp *RegistrationResp)

type RegistrationReqBuilder struct {
	req *RegistrationReq
}

func NewRegistrationReqBuilder() RegistrationReqBuilder {
	return RegistrationReqBuilder{
		req: &RegistrationReq{
			Labels:    make([]*Label, 0),
			Resources: map[string]float64{},
		},
	}
}

func (r RegistrationReqBuilder) AddBoolLabel(key string, value bool) RegistrationReqBuilder {
	valueMarshalled, err := proto.Marshal(&BoolValue{Value: value})
	if err != nil {
		return r
	}
	return r.addLabel(key, Value_Bool, valueMarshalled)
}

func (r RegistrationReqBuilder) AddFloat64Label(key string, value float64) RegistrationReqBuilder {
	valueMarshalled, err := proto.Marshal(&Float64Value{Value: value})
	if err != nil {
		return r
	}
	return r.addLabel(key, Value_Float64, valueMarshalled)
}

func (r RegistrationReqBuilder) AddStringLabel(key string, value string) RegistrationReqBuilder {
	valueMarshalled, err := proto.Marshal(&StringValue{Value: value})
	if err != nil {
		return r
	}
	return r.addLabel(key, Value_String, valueMarshalled)
}

func (r RegistrationReqBuilder) addLabel(key string, valueType Value_ValueTYpe, valueMarshalled []byte) RegistrationReqBuilder {
	label := &Label{
		Key: key,
		Value: &Value{
			Type:       valueType,
			Marshalled: valueMarshalled,
		},
	}
	r.req.Labels = append(r.req.Labels, label)
	return r
}

func (r RegistrationReqBuilder) Request() *RegistrationReq {
	return r.req
}

func (r RegistrationReqBuilder) Clear() RegistrationReqBuilder {
	return NewRegistrationReqBuilder()
}
