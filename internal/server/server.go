package server

import (
	"context"
	"distributed/internal/log"

	log_v1 "distributed/api/log.v1"

	"google.golang.org/grpc"
)

var _ log_v1.LogServer = (*grpcServer)(nil)

type CommitLog interface {
	Append(record log_v1.Record) (uint64, error)
	Read(uint64) (*log_v1.Record, error)
}

type GetServerer interface {
	GetServers() ([]*log_v1.Server, error)
}

type Config struct {
	CommitLog   log.Log
	GetServerer GetServerer
}

type grpcServer struct {
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	log_v1.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *log_v1.ProduceRequest) (
	*log_v1.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *log_v1.ConsumeRequest) (
	*log_v1.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	stream log_v1.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *log_v1.ConsumeRequest,
	stream log_v1.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case log_v1.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func (s *grpcServer) GetServers(ctx context.Context, req *log_v1.GetServersRequest) (
	*log_v1.GetServersResponse, error) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}
	return &log_v1.GetServersResponse{Servers: servers}, nil
}
