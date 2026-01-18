package startup

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"

	gravity_api "github.com/c12s/agent_queue/pkg/api"
	"github.com/c12s/magnetar/internal/configs"
	"github.com/c12s/magnetar/internal/domain"
	"github.com/c12s/magnetar/internal/marshallers/proto"
	"github.com/c12s/magnetar/internal/repos"
	"github.com/c12s/magnetar/internal/servers"
	"github.com/c12s/magnetar/internal/services"
	"github.com/c12s/magnetar/pkg/api"
	"github.com/c12s/magnetar/pkg/messaging"
	"github.com/c12s/magnetar/pkg/messaging/nats"
	meridian_api "github.com/c12s/meridian/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	natsgo "github.com/nats-io/nats.go"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type app struct {
	config                    *configs.Config
	grpcServer                *grpc.Server
	magnetarServer            api.MagnetarServer
	registrationServer        *servers.RegistrationAsyncServer
	nodeService               *services.NodeService
	labelService              *services.LabelService
	authzService              services.AuthZService
	registrationService       *services.RegistrationService
	evaluatorClient           oortapi.OortEvaluatorClient
	administratorClient       *oortapi.AdministrationAsyncClient
	meridian                  meridian_api.MeridianClient
	gravity                   gravity_api.AgentQueueClient
	publisher                 messaging.Publisher
	registrationSubscriber    messaging.Subscriber
	nodeRepo                  domain.NodeRepo
	nodeMarshaller            domain.NodeMarshaller
	labelMarshaller           domain.LabelMarshaller
	shutdownProcesses         []func()
	gracefulShutdownProcesses []func(wg *sync.WaitGroup)
}

func NewAppWithConfig(config *configs.Config) (*app, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	return &app{
		config:                    config,
		shutdownProcesses:         make([]func(), 0),
		gracefulShutdownProcesses: make([]func(wg *sync.WaitGroup), 0),
	}, nil
}

func (a *app) Start() error {
	shutdownTracing := initTracing(
		"magnetar",
		a.config.JaegerGRPCEndpoint(),
	)
	a.shutdownProcesses = append(a.shutdownProcesses, shutdownTracing)
	a.init()

	err := a.startRegistrationServer()
	if err != nil {
		return err
	}
	return a.startGrpcServer()
}

func (a *app) GracefulStop(ctx context.Context) {
	// call all shutdown processes after a timeout or graceful shutdown processes completion
	defer a.shutdown()

	// wait for all graceful shutdown processes to complete
	wg := &sync.WaitGroup{}
	wg.Add(len(a.gracefulShutdownProcesses))

	for _, gracefulShutdownProcess := range a.gracefulShutdownProcesses {
		go gracefulShutdownProcess(wg)
	}

	// notify when graceful shutdown processes are done
	gracefulShutdownDone := make(chan struct{})
	go func() {
		wg.Wait()
		gracefulShutdownDone <- struct{}{}
	}()

	// wait for graceful shutdown processes to complete or for ctx timeout
	select {
	case <-ctx.Done():
		log.Println("ctx timeout ... shutting down")
	case <-gracefulShutdownDone:
		log.Println("app gracefully stopped")
	}
}

func (a *app) init() {
	natsConn, err := NewNatsConn(a.config.NatsAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.shutdownProcesses = append(a.shutdownProcesses, func() {
		log.Println("closing nats conn")
		natsConn.Close()
	})

	etcdClient, err := newEtcdClient(a.config.EtcdAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.shutdownProcesses = append(a.shutdownProcesses, func() {
		log.Println("closing etcd client conn")
		err := etcdClient.Close()
		if err != nil {
			log.Println(err)
		}
	})

	a.initNatsPublisher(natsConn)
	a.initRegistrationNatsSubscriber(natsConn)

	a.initNodeProtoMarshaller()
	a.initLabelProtoMarshaller()
	a.initNodeEtcdRepo(etcdClient)

	a.initAdministratorClient()
	a.initEvaluatorClient()
	a.initMeridian()
	a.initGravity()

	a.initAuthZService()
	a.initNodeService()
	a.initLabelService()
	a.initRegistrationService()

	a.initRegistrationServer()
	a.initMagnetarServer()
	a.initGrpcServer()
}

func (a *app) initGrpcServer() {
	if a.magnetarServer == nil {
		log.Fatalln("magnetar server is nil")
	}

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(servers.GetAuthInterceptor()),
	)

	api.RegisterMagnetarServer(s, a.magnetarServer)
	reflection.Register(s)
	a.grpcServer = s
}

func (a *app) initMagnetarServer() {
	if a.nodeService == nil {
		log.Fatalln("node service is nil")
	}
	if a.labelService == nil {
		log.Fatalln("label service is nil")
	}
	magnetarServer, err := servers.NewMagnetarGrpcServer(*a.nodeService, *a.labelService)
	if err != nil {
		log.Fatalln(err)
	}
	a.magnetarServer = magnetarServer
}

func (a *app) initRegistrationServer() {
	if a.registrationService == nil {
		log.Fatalln("registration service is nil")
	}
	if a.publisher == nil {
		log.Fatalln("publisher is nil")
	}
	if a.registrationSubscriber == nil {
		log.Fatalln("registration req subscriber is nil")
	}

	tracer := otel.Tracer("magnetar.AppInit")
	_, span := tracer.Start(context.Background(), "InitRegistrationServer")
	defer span.End()

	server, err := servers.NewRegistrationAsyncServer(a.registrationSubscriber, a.publisher, a.registrationService)
	if err != nil {
		span.RecordError(err)
		log.Fatalln(err)
	}

	a.registrationServer = server
	log.Println("Registration async server initialized")
}

func (a *app) initRegistrationService() {
	if a.nodeRepo == nil {
		log.Fatalln("node repo is nil")
	}
	registrationService, err := services.NewRegistrationService(a.nodeRepo)
	if err != nil {
		log.Fatalln(err)
	}
	a.registrationService = registrationService
}

func (a *app) initNodeService() {
	if a.nodeRepo == nil {
		log.Fatalln("node repo is nil")
	}
	if a.meridian == nil {
		log.Fatalln("meridian is nil")
	}
	if a.gravity == nil {
		log.Fatalln("gravity is nil")
	}

	nodeService, err := services.NewNodeService(a.nodeRepo, a.evaluatorClient, a.administratorClient, a.authzService, a.meridian, a.gravity)
	if err != nil {
		log.Fatalln(err)
	}
	a.nodeService = nodeService
}

func (a *app) initLabelService() {
	if a.nodeRepo == nil {
		log.Fatalln("node repo is nil")
	}
	labelService, err := services.NewLabelService(a.nodeRepo, a.evaluatorClient, a.authzService)
	if err != nil {
		log.Fatalln(err)
	}
	a.labelService = labelService
}

func (a *app) initAuthZService() {
	a.authzService = services.NewAuthZService(a.config.TokenKey())
}

func (a *app) initMeridian() {
	conn, err := grpc.NewClient(
		a.config.MeridianAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalln(err)
	}
	a.meridian = meridian_api.NewMeridianClient(conn)
}

func (a *app) initGravity() {
	conn, err := grpc.NewClient(
		a.config.GravityAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalln(err)
	}
	a.gravity = gravity_api.NewAgentQueueClient(conn)
}

func (a *app) initEvaluatorClient() {
	client, err := newOortEvaluatorClient(a.config.OortAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.evaluatorClient = client
}

func (a *app) initAdministratorClient() {
	client, err := newOortAdministratorClient(a.config.NatsAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.administratorClient = client
}

func (a *app) initNatsPublisher(conn *natsgo.Conn) {
	publisher, err := nats.NewPublisher(conn)
	if err != nil {
		log.Fatalln(err)
	}
	a.publisher = publisher
}

func (a *app) initRegistrationNatsSubscriber(conn *natsgo.Conn) {
	registrationSubscriber, err := nats.NewSubscriber(conn, api.RegistrationSubject, "magnetar")
	if err != nil {
		log.Fatalln(err)
	}
	a.registrationSubscriber = registrationSubscriber
}

func (a *app) initNodeEtcdRepo(client *etcd.Client) {
	nodeRepo, err := repos.NewNodeEtcdRepo(client, a.nodeMarshaller, a.labelMarshaller)
	if err != nil {
		log.Fatalln(err)
	}
	a.nodeRepo = nodeRepo
}

func (a *app) initLabelProtoMarshaller() {
	a.labelMarshaller = proto.NewProtoLabelMarshaller()
}

func (a *app) initNodeProtoMarshaller() {
	a.nodeMarshaller = proto.NewProtoNodeMarshaller()
}

func (a *app) startRegistrationServer() error {
	err := a.registrationServer.Serve()
	if err != nil {
		return err
	}
	a.gracefulShutdownProcesses = append(a.gracefulShutdownProcesses, func(wg *sync.WaitGroup) {
		a.registrationServer.GracefulStop()
		log.Println("registration server gracefully stopped")
		wg.Done()
	})
	return nil
}

func (a *app) startGrpcServer() error {
	lis, err := net.Listen("tcp", a.config.ServerAddress())
	if err != nil {
		return err
	}
	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := a.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	a.gracefulShutdownProcesses = append(a.gracefulShutdownProcesses, func(wg *sync.WaitGroup) {
		a.grpcServer.GracefulStop()
		log.Println("magnetar server gracefully stopped")
		wg.Done()
	})
	return nil
}

func (a *app) shutdown() {
	for _, shutdownProcess := range a.shutdownProcesses {
		shutdownProcess()
	}
}
