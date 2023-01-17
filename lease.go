package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/raft"
	"github.com/superfly/litefs"
)

type RaftLeaser struct {
	r         *raft.Raft
	localInfo litefs.PrimaryInfo
	pp        PrimaryProvider
}

type PrimaryProvider interface {
	PrimaryInfo() litefs.PrimaryInfo
}

func New(r *raft.Raft, localInfo litefs.PrimaryInfo, primaryProvider PrimaryProvider) *RaftLeaser {
	chObservation := make(chan raft.Observation)
	observer := raft.NewObserver(chObservation, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	go func() {
		for {
			<-chObservation
			b, _ := json.Marshal(litefs.PrimaryInfo{})
			future := r.Apply(b, time.Second)
			if err := future.Error(); err != nil {
				log.Println("cannot reset lease:", err.Error())
			}
		}
	}()
	return &RaftLeaser{
		r:         r,
		localInfo: localInfo,
		pp:        primaryProvider,
	}
}

func (l *RaftLeaser) Close() error {
	future := l.r.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("cannot shutdown raft: %w", err)
	}
	return nil
}

func (l *RaftLeaser) AdvertiseURL() string {
	return l.localInfo.AdvertiseURL
}

func (l *RaftLeaser) Acquire(ctx context.Context) (litefs.Lease, error) {
	b, err := json.Marshal(l.localInfo)
	if err != nil {
		return nil, fmt.Errorf("encode json to acquire lease: %w", err)
	}
	future := l.r.Apply(b, time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}

	return newLease(l.r, time.Now()), nil
}

func (l *RaftLeaser) PrimaryInfo(ctx context.Context) (litefs.PrimaryInfo, error) {
	if l.pp.PrimaryInfo().AdvertiseURL == "" {
		return litefs.PrimaryInfo{}, litefs.ErrNoPrimary
	}
	return l.pp.PrimaryInfo(), nil
}

type lease struct {
	r         *raft.Raft
	renewedAt time.Time
}

func newLease(r *raft.Raft, renewedAt time.Time) *lease {
	return &lease{
		r:         r,
		renewedAt: renewedAt,
	}
}

func (l *lease) RenewedAt() time.Time {
	return l.renewedAt
}

func (l *lease) TTL() time.Duration {
	return 10 * time.Second
}

func (l *lease) Renew(ctx context.Context) error {
	if err := l.r.VerifyLeader().Error(); err != nil {
		return litefs.ErrLeaseExpired
	}
	l.renewedAt = time.Now()
	return nil
}

func (l *lease) Close() error {
	b, _ := json.Marshal(litefs.PrimaryInfo{})
	future := l.r.Apply(b, time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	if future.Response() != nil {
		return future.Response().(error)
	}
	futureTransfer := l.r.LeadershipTransfer()
	return futureTransfer.Error()
}
