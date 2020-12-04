package anti

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"go.uber.org/zap"
	"sync"
)

// AntiRuleManager is responsible for the lifecycle of all anti-affinity Rules.
type AntiRuleManager struct {
	sync.RWMutex
	antiRules []*AntiRule

	//antiRuleID -> storeID -> amount of leader region in storeID
	antiScore map[uint64]map[uint64]uint64

	//regionID -> storeID of region leader
	leaderLocation map[uint64]uint64
}

// NewAntiRuleManager creates a AntiRuleManager instance.
func NewAntiRuleManager() *AntiRuleManager {
	return &AntiRuleManager{antiScore: map[uint64]map[uint64]uint64{}, leaderLocation: map[uint64]uint64{}}
}

func (m *AntiRuleManager) GetRegionLeaderLocation(regionID uint64) (storeID uint64, found bool) {
	m.RLock()
	defer m.RUnlock()
	if location, ok := m.leaderLocation[regionID]; ok {
		storeID = location
		found = true
	}
	return
}

func (m *AntiRuleManager) SetRegionLeaderLocation(regionID, storeID uint64) {
	m.Lock()
	defer m.Unlock()
	m.leaderLocation[regionID] = storeID
}

func (m *AntiRuleManager) GetAntiScoreByRuleID(ruleID uint64) map[uint64]uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.antiScore[ruleID]
}

func (m *AntiRuleManager) IncrAntiScore(ruleID, storeID uint64) {
	m.Lock()
	defer m.Unlock()
	if m.antiScore[ruleID] == nil {
		m.antiScore[ruleID] = map[uint64]uint64{}
	}
	m.antiScore[ruleID][storeID]++
}

func (m *AntiRuleManager) DecrAntiScore(ruleID, storeID uint64) error {
	m.Lock()
	defer m.Unlock()
	if store, ok := m.antiScore[ruleID]; ok {
		if _, ok := store[storeID]; ok {
			m.antiScore[ruleID][storeID]--
			return nil
		}
	}
	return errors.Errorf("decr failed, unable to get ruleID(%d) or storeID(%d) in antiScore map", ruleID, storeID)
}

// GetAntiRules returns the all the anti rules.
func (m *AntiRuleManager) GetAntiRules() []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.getAntiRules()
}

// SetAntiRule inserts an anti Rule.
func (m *AntiRuleManager) SetAntiRule(antiRule *AntiRule) error {
	if err := m.setAntiRule(antiRule); err != nil {
		return err
	}
	return nil
}

// getAntiRules returns all the AntiRule.
func (m *AntiRuleManager) getAntiRules() []*AntiRule {
	m.RLock()
	defer m.RUnlock()
	return m.antiRules
}

// setAntiRule inserts or updates a AntiRule.
func (m *AntiRuleManager) setAntiRule(antiRule *AntiRule) error {
	m.Lock()
	defer m.Unlock()

	var err error
	antiRule.StartKey, err = hex.DecodeString(antiRule.StartKeyHex)
	if err != nil {
		return errs.ErrHexDecodingString.FastGenByArgs(antiRule.StartKeyHex)
	}
	antiRule.EndKey, err = hex.DecodeString(antiRule.EndKeyHex)
	if err != nil {
		return errs.ErrHexDecodingString.FastGenByArgs(antiRule.EndKeyHex)
	}

	m.antiRules = append(m.antiRules, antiRule)
	//todo remove later
	log.Warn("antiRule updated", zap.String("antiRule", fmt.Sprint(antiRule)))
	return nil
}
