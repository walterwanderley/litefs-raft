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

var _ litefs.Leaser = &RaftLeaser{}
var _ litefs.Lease = &lease{}

type RaftLeaser struct {
	r         *raft.Raft
	localInfo PrimaryRedirectInfo
	fsm       *FSM
	ttl       time.Duration
}

func New(r *raft.Raft, localInfo PrimaryRedirectInfo, fsm *FSM, ttl time.Duration) *RaftLeaser {
	chObservation := make(chan raft.Observation)
	observer := raft.NewObserver(chObservation, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(observer)
	go func() {
		for {
			<-chObservation
			b, _ := json.Marshal(PrimaryRedirectInfo{})
			future := r.Apply(b, time.Second)
			if err := future.Error(); err != nil {
				log.Println("cannot reset lease:", err.Error())
			}
		}
	}()
	return &RaftLeaser{
		r:         r,
		localInfo: localInfo,
		fsm:       fsm,
		ttl:       ttl,
	}
}

func (l *RaftLeaser) Type() string {
	return "raft"
}

func (l *RaftLeaser) RedirectURL() string {
	return l.fsm.RedirectURL()
}

func (l *RaftLeaser) Close() error {
	future := l.r.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("cannot shutdown raft: %w", err)
	}
	return nil
}

func (l *RaftLeaser) AdvertiseURL() string {
	return l.localInfo.PrimaryInfo.AdvertiseURL
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

	return newLease(l.r, time.Now(), l.ttl), nil
}

func (l *RaftLeaser) AcquireExisting(ctx context.Context, leaseID string) (litefs.Lease, error) {
	return nil, fmt.Errorf("raft lease handoff not supported yet")
}

func (l *RaftLeaser) PrimaryInfo(ctx context.Context) (litefs.PrimaryInfo, error) {
	if l.fsm.PrimaryInfo().AdvertiseURL == "" {
		return litefs.PrimaryInfo{}, litefs.ErrNoPrimary
	}
	return l.fsm.PrimaryInfo(), nil
}

func (l *RaftLeaser) ClusterID(ctx context.Context) (string, error) {
	return "", nil
}

func (l *RaftLeaser) SetClusterID(ctx context.Context, clusterID string) error {
	return nil
}

type lease struct {
	r         *raft.Raft
	renewedAt time.Time
	ttl       time.Duration
}

func newLease(r *raft.Raft, renewedAt time.Time, ttl time.Duration) *lease {
	return &lease{
		r:         r,
		renewedAt: renewedAt,
		ttl:       ttl,
	}
}

func (l *lease) ID() string {
	return ""
}

func (l *lease) RenewedAt() time.Time {
	return l.renewedAt
}

func (l *lease) TTL() time.Duration {
	return l.ttl
}

func (l *lease) Renew(ctx context.Context) error {
	if err := l.r.VerifyLeader().Error(); err != nil {
		return litefs.ErrLeaseExpired
	}
	l.renewedAt = time.Now()
	return nil
}

func (l *lease) Handoff(nodeID uint64) error {
	return fmt.Errorf("raft lease does not support handoff yet")
}

func (l *lease) HandoffCh() <-chan uint64 {
	return nil
}

func (l *lease) Close() error {
	b, _ := json.Marshal(PrimaryRedirectInfo{})
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
