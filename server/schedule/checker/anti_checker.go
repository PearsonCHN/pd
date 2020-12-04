package checker

import (
	"bytes"
	"fmt"
	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/anti"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
)

type AntiRuleChecker struct {
	cluster         opt.Cluster
	antiRuleManager *anti.AntiRuleManager
	name            string
}

// NewAntiChecker creates a checker instance.
func NewAntiChecker(cluster opt.Cluster, antiRuleManager *anti.AntiRuleManager) *AntiRuleChecker {
	return &AntiRuleChecker{
		cluster:         cluster,
		antiRuleManager: antiRuleManager,
		name:            "anti-affinity-checker",
	}
}

//Check checks the anti rules that fits given region
func (ac *AntiRuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	//log.Warn(fmt.Sprintf("FOUND-LEADER! region ID:%d, Leader ID:%d", region.GetID(), region.GetLeader().GetId()))
	startKey, endKey := region.GetStartKey(), region.GetEndKey()
	var ruleFit *anti.AntiRule
	rules := ac.antiRuleManager.GetAntiRules()
	for _, rule := range rules {
		log.Warn(fmt.Sprintf("rule.StartKey:%v, startKey:%v, rule.EndKey:%v, endKey:%v ||regionID:%d", rule.StartKey, startKey, rule.EndKey, endKey, region.GetID()))
		if startKey != nil && endKey != nil && bytes.Compare(rule.StartKey, startKey) <= 0 && bytes.Compare(rule.EndKey, endKey) >= 0 {
			ruleFit = rule
			log.Warn(fmt.Sprintf("rule fit!! ruleID:%d, regionID:%d", rule.ID, region.GetID()))
			log.Warn(fmt.Sprintf("rule.StartKey:%v, rule.EndKey:%v, region.StartKey:%v, region.EndKey:%v",
				rule.StartKey, rule.EndKey, region.GetStartKey(), region.GetEndKey()))
			/*
				TODO: we only handle the first rule that fits the region's key range(only for testing convenience),
				      other rules should be handle, will support in later days
			*/
			break
		}
	}
	if ruleFit == nil {
		return nil
	}
	//store->leaders num
	storeScore := ac.antiRuleManager.GetAntiScoreByRuleID(ruleFit.ID)
	if len(storeScore) < 1 {
		return nil
	}
	var minScore, minStoreID, maxScore, maxStoreID uint64
	//pick up a record as the initial record
	for ID, score := range storeScore {
		minScore = score
		maxScore = score
		minStoreID = ID
		maxStoreID = ID
		break
	}
	for ID, score := range storeScore {
		if score > maxScore {
			//leader数量最多的store
			maxScore = score
			maxStoreID = ID
		}
		if score < minScore {
			//leader数量最小的store
			minScore = score
			minStoreID = ID
		}
	}
	//avoid transfer leader repeatedly
	if maxScore-minScore <= 1 {
		return nil
	}

	if region.GetLeader().StoreId != maxStoreID {
		return nil
	}

	// current region leader's storeID's score is the largest one,
	// move this region leader to the store with the smallest score

	regionLeaderStoreID := region.GetLeader().StoreId

	//transfer leader to the store with min score
	op, err := operator.CreateTransferLeaderOperator("anti rule", ac.cluster, region, region.GetLeader().StoreId, minStoreID, operator.OpLeader)

	//update antiScoreMap(decr source store, incr target store)
	if err != nil {
		log.Error(fmt.Sprintf("create anti rule transferLeader op failed: %s", err.Error()))
		return nil
	}
	if err := ac.antiRuleManager.DecrAntiScore(ruleFit.ID, regionLeaderStoreID); err != nil {
		log.Error(err.Error())
	}
	ac.antiRuleManager.IncrAntiScore(ruleFit.ID, minStoreID)
	ac.antiRuleManager.SetRegionLeaderLocation(region.GetID(), minStoreID)
	//todo remove later
	log.Warn(fmt.Sprintf("transfer leader from store %d to store %d, regionID:%d", regionLeaderStoreID, minStoreID, region.GetID()))
	return op
}
