package router

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
)

type weightScaler func(value, weight float64) float64

var numberFinder = regexp.MustCompile(`\d+(\.\d+)?`)

// NewWeightManager creates a new WeightManager with settings
func NewWeightManager(s []*StrategyWeight, defaultWeight float64, scaler weightScaler) *WeightManager {
	return &WeightManager{
		settings:      s,
		cache:         make(map[string]weightEntry),
		scaler:        scaler,
		defaultWeight: defaultWeight,
	}
}

// WeightManager manages weights for specific settings
type WeightManager struct {
	settings      []*StrategyWeight
	cache         map[string]weightEntry
	scaler        weightScaler
	defaultWeight float64
	mu            sync.Mutex
}

// weightEntry is a resolved weight and a human readable reason of where it comes from
type weightEntry struct {
	value  float64
	reason string
}

// Get get the weight of specified tag
func (s *WeightManager) Get(tag string) float64 {
	return s.get(tag).value
}

// Describe explains which setting the weight of specified tag comes from
func (s *WeightManager) Describe(tag string) string {
	return s.get(tag).reason
}

// Apply applies weight to the value
func (s *WeightManager) Apply(tag string, value float64) float64 {
	return s.scaler(value, s.Get(tag))
}

func (s *WeightManager) get(tag string) weightEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[tag]
	if ok {
		return entry
	}
	entry = s.findValue(tag)
	s.cache[tag] = entry
	return entry
}

func (s *WeightManager) findValue(tag string) weightEntry {
	for i, w := range s.settings {
		matched := s.getMatch(tag, w.Match, w.Regexp)
		if matched == "" {
			continue
		}
		rule := serial.Concat("cost rule #", i+1, " ", describeWeightMatch(w))
		if w.Value > 0 {
			return weightEntry{float64(w.Value), rule}
		}
		// auto weight from matched
		numStr := numberFinder.FindString(matched)
		if numStr == "" {
			return weightEntry{s.defaultWeight, serial.Concat(rule, ": no value and no number in ", strconv.Quote(matched), ", default")}
		}
		weight, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			errors.LogError(context.Background(), "unexpected error from ParseFloat: ", err)
			return weightEntry{s.defaultWeight, serial.Concat(rule, ": cannot parse ", strconv.Quote(numStr), ", default")}
		}
		return weightEntry{weight, serial.Concat(rule, ": no value, number taken from ", strconv.Quote(matched))}
	}
	return weightEntry{s.defaultWeight, "no cost rule matched, default"}
}

func (s *WeightManager) getMatch(tag, find string, isRegexp bool) string {
	if !isRegexp {
		idx := strings.Index(tag, find)
		if idx < 0 {
			return ""
		}
		return find
	}
	r, err := regexp.Compile(find)
	if err != nil {
		errors.LogError(context.Background(), "invalid regexp: ", find, "err: ", err)
		return ""
	}
	return r.FindString(tag)
}

func describeWeightMatch(w *StrategyWeight) string {
	if w.Regexp {
		return "regexp " + strconv.Quote(w.Match)
	}
	return "substring " + strconv.Quote(w.Match)
}
