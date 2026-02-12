package messaging

import "context"

type Subscriber interface {
	Subscribe(handler func(ctx context.Context, msg []byte, replySubject string)) error
	Unsubscribe() error
}

type Publisher interface {
	Publish(ctx context.Context, msg []byte, subject string) error
	Request(ctx context.Context, msg []byte, subject, replySubject string) error
	GenerateReplySubject() string
}
