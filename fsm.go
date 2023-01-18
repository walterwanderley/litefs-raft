package raft

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/superfly/litefs"
)

type PrimaryRedirectInfo struct {
	PrimaryInfo litefs.PrimaryInfo
	RedirectURL string
}

type FSM struct {
	data PrimaryRedirectInfo
}

func NewFSM() *FSM {
	return &FSM{}
}

func (fsm *FSM) Apply(log *raft.Log) interface{} {
	switch log.Type {
	case raft.LogCommand:
		if err := json.Unmarshal(log.Data, &fsm.data); err != nil {
			return fmt.Errorf("cannot read the payload: %w", err)
		}
	default:
		return fmt.Errorf("unknown log type: %#v", log.Type)
	}
	return nil
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return fsm, nil
}

func (fsm *FSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	return json.NewDecoder(snapshot).Decode(&fsm.data)
}

func (fsm *FSM) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(fsm.data)
	if err != nil {
		return err
	}
	return sink.Close()
}

func (fsm *FSM) Release() {
}

func (fsm *FSM) PrimaryInfo() litefs.PrimaryInfo {
	return fsm.data.PrimaryInfo
}

func (fsm *FSM) RedirectURL() string {
	return fsm.data.RedirectURL
}
