package router

import (
	"context"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
)

// LeastLoadStrategy represents a least load balancing strategy
type LeastLoadStrategy struct {
	tag      string
	settings *StrategyLeastLoadConfig
	costs    *WeightManager

	observer extension.Observatory

	ctx context.Context

	// the decision report is logged only when it differs from the last one
	reportMu   sync.Mutex
	lastReport string
}

func (l *LeastLoadStrategy) GetPrincipleTarget(strings []string) []string {
	var ret []string
	nodes := l.pickOutbounds(strings)
	for _, v := range nodes {
		ret = append(ret, v.Tag)
	}
	return ret
}

// NewLeastLoadStrategy creates a new LeastLoadStrategy with settings
func NewLeastLoadStrategy(settings *StrategyLeastLoadConfig) *LeastLoadStrategy {
	return &LeastLoadStrategy{
		settings: settings,
		costs: NewWeightManager(
			settings.Costs, 1,
			func(value, cost float64) float64 {
				return value * math.Pow(cost, 0.5)
			},
		),
	}
}

// node is a minimal copy of HealthCheckResult
// we don't use HealthCheckResult directly because
// it may change by health checker during routing
type node struct {
	Tag              string
	CountAll         int
	CountFail        int
	RTTAverage       time.Duration
	RTTDeviation     time.Duration
	RTTDeviationCost time.Duration
}

// trace collects human readable lines explaining a decision, a nil trace collects nothing
type trace []string

func (t *trace) add(msg ...interface{}) {
	if t != nil {
		*t = append(*t, serial.Concat(msg...))
	}
}

func (s *LeastLoadStrategy) InjectContext(ctx context.Context) {
	s.ctx = ctx
	common.Must(core.RequireFeatures(s.ctx, func(observatory extension.Observatory) error {
		s.observer = observatory
		return nil
	}))
	errors.LogDebug(s.ctx, s.describeSettings())
}

func (s *LeastLoadStrategy) PickOutbound(candidates []string) string {
	selects := s.pickOutbounds(candidates)
	count := len(selects)
	if count == 0 {
		// goes to fallbackTag
		return ""
	}
	return selects[dice.Roll(count)].Tag
}

func (s *LeastLoadStrategy) pickOutbounds(candidates []string) []*node {
	if log.GetSeverity() < log.Severity_Debug {
		return s.selectLeastLoad(s.getNodes(candidates, nil), nil)
	}
	var filterTrace, selectTrace trace
	qualified := s.getNodes(candidates, &filterTrace)
	selects := s.selectLeastLoad(qualified, &selectTrace)
	s.logReport(candidates, filterTrace, qualified, selectTrace, selects)
	return selects
}

// selectLeastLoad selects nodes according to Baselines and Expected Count.
//
// The strategy always improves network response speed, not matter which mode below is configured.
// But they can still have different priorities.
//
// 1. Bandwidth priority: no Baseline + Expected Count > 0.: selects `Expected Count` of nodes.
// (one if Expected Count <= 0)
//
// 2. Bandwidth priority advanced: Baselines + Expected Count > 0.
// Select `Expected Count` amount of nodes, and also those near them according to baselines.
// In other words, it selects according to different Baselines, until one of them matches
// the Expected Count, if no Baseline matches, Expected Count applied.
//
// 3. Speed priority: Baselines + `Expected Count <= 0`.
// go through all baselines until find selects, if not, select none. Used in combination
// with 'balancer.fallbackTag', it means: selects qualified nodes or use the fallback.
func (s *LeastLoadStrategy) selectLeastLoad(nodes []*node, t *trace) []*node {
	if len(nodes) == 0 {
		t.add("no outbound passed the filter, nothing to select")
		return nil
	}
	expected := int(s.settings.Expected)
	availableCount := len(nodes)
	if expected > availableCount {
		t.add("expected ", expected, " > ", availableCount, " qualified, so all qualified are selected (baselines are not used)")
		return nodes
	}

	if expected <= 0 {
		expected = 1
	}
	if len(s.settings.Baselines) == 0 {
		t.add("no baselines, take the best ", expected, " by score")
		return nodes[:expected]
	}

	count := 0
	// go through all base line until find expected selects
	for _, b := range s.settings.Baselines {
		baseline := time.Duration(b)
		for i := count; i < availableCount; i++ {
			if nodes[i].RTTDeviationCost >= baseline {
				break
			}
			count = i + 1
		}
		// don't continue if find expected selects
		if count >= expected {
			t.add("baseline ", baseline, ": ", count, " with score < ", baseline, ", at least ", expected, " -> applied, take these ", count)
			break
		}
		t.add("baseline ", baseline, ": ", count, " with score < ", baseline, ", fewer than ", expected, " -> try next baseline")
	}
	if s.settings.Expected > 0 && count < expected {
		t.add("no baseline admits ", expected, ", take the best ", expected, " by score anyway")
		count = expected
	}
	if count == 0 {
		t.add("no outbound under any baseline and expected is not set, nothing selected")
	}
	return nodes[:count]
}

func (s *LeastLoadStrategy) getNodes(candidates []string, t *trace) []*node {
	if s.observer == nil {
		errors.LogError(s.ctx, "balancer [", s.tag, "]: observer is nil")
		return make([]*node, 0)
	}
	observeResult, err := s.observer.GetObservation(s.ctx)
	if err != nil {
		errors.LogInfoInner(s.ctx, err, "balancer [", s.tag, "]: cannot get observation")
		return make([]*node, 0)
	}

	results := observeResult.(*observatory.ObservationResult)

	statuses := results.Status
	if t != nil {
		// stable order, so that an unchanged decision gives an unchanged report
		statuses = slices.Clone(statuses)
		slices.SortFunc(statuses, func(a, b *observatory.OutboundStatus) int {
			return strings.Compare(a.OutboundTag, b.OutboundTag)
		})
	}

	var ret []*node

	for _, v := range statuses {
		if s.shouldSelectNode(v, candidates, t) {
			record := &node{
				Tag:              v.OutboundTag,
				CountAll:         1,
				CountFail:        1,
				RTTAverage:       time.Duration(v.Delay) * time.Millisecond,
				RTTDeviation:     time.Duration(v.Delay) * time.Millisecond,
				RTTDeviationCost: time.Duration(s.costs.Apply(v.OutboundTag, float64(time.Duration(v.Delay)*time.Millisecond))),
			}

			if v.HealthPing != nil {
				record.RTTAverage = time.Duration(v.HealthPing.Average)
				record.RTTDeviation = time.Duration(v.HealthPing.Deviation)
				record.RTTDeviationCost = time.Duration(s.costs.Apply(v.OutboundTag, float64(v.HealthPing.Deviation)))
				record.CountAll = int(v.HealthPing.All)
				record.CountFail = int(v.HealthPing.Fail)
			}

			ret = append(ret, record)
		}
	}

	if t != nil {
		for _, c := range candidates {
			if !slices.ContainsFunc(statuses, func(v *observatory.OutboundStatus) bool { return v.OutboundTag == c }) {
				t.add(c, "\tSKIP\tnot measured by the observatory (not in its subjectSelector?), leastLoad never picks it")
			}
		}
	}

	leastloadSort(ret)
	return ret
}

func (s *LeastLoadStrategy) shouldSelectNode(v *observatory.OutboundStatus, candidates []string, t *trace) bool {
	if !slices.Contains(candidates, v.OutboundTag) {
		return false
	}
	var all, fail int64
	if v.HealthPing != nil {
		all, fail = v.HealthPing.All, v.HealthPing.Fail
	}
	if !v.Alive {
		if t != nil {
			t.add(v.OutboundTag, "\tFAIL\tdead, no successful health ping (failed ", fail, "/", all, ")")
		}
		return false
	}
	maxRTT := time.Duration(s.settings.MaxRTT)
	rtt := time.Duration(v.Delay) * time.Millisecond
	if maxRTT != 0 && v.Delay >= maxRTT.Milliseconds() {
		if t != nil {
			t.add(v.OutboundTag, "\tFAIL\trtt ", fmtDuration(rtt), " >= maxRTT ", maxRTT)
		}
		return false
	}
	tolerance := float64(s.settings.Tolerance)
	if v.HealthPing != nil && v.HealthPing.All > 0 && tolerance > 0 && float64(v.HealthPing.Fail)/float64(v.HealthPing.All) > tolerance {
		if t != nil {
			t.add(v.OutboundTag, "\tFAIL\tfailed ", fail, "/", all, " = ", fmtPercent(float64(fail)/float64(all)),
				" > tolerance ", fmtPercent(tolerance), " (allows ", allowedFails(tolerance, all), " of ", all, ")")
		}
		return false
	}
	if t != nil {
		rttCheck := "no maxRTT"
		if maxRTT != 0 {
			rttCheck = serial.Concat("< maxRTT ", maxRTT)
		}
		failCheck := "no tolerance"
		if tolerance > 0 && all > 0 {
			failCheck = serial.Concat("= ", fmtPercent(float64(fail)/float64(all)), " <= tolerance ", fmtPercent(tolerance),
				" (allows ", allowedFails(tolerance, all), " of ", all, ")")
		}
		t.add(v.OutboundTag, "\tPASS\trtt ", fmtDuration(rtt), " ", rttCheck, ", failed ", fail, "/", all, " ", failCheck)
	}
	return true
}

func leastloadSort(nodes []*node) {
	sort.Slice(nodes, func(i, j int) bool {
		left := nodes[i]
		right := nodes[j]
		if left.RTTDeviationCost != right.RTTDeviationCost {
			return left.RTTDeviationCost < right.RTTDeviationCost
		}
		if left.RTTAverage != right.RTTAverage {
			return left.RTTAverage < right.RTTAverage
		}
		if left.CountFail != right.CountFail {
			return left.CountFail < right.CountFail
		}
		if left.CountAll != right.CountAll {
			return left.CountAll > right.CountAll
		}
		return left.Tag < right.Tag
	})
}

// logReport logs how the outbounds were selected, if the decision differs from the last logged one
func (s *LeastLoadStrategy) logReport(candidates []string, filterTrace []string, qualified []*node, selectTrace []string, selects []*node) {
	var b strings.Builder
	b.WriteString(serial.Concat("balancer [", s.tag, "] leastLoad decision, candidates: ", strings.Join(candidates, ", ")))

	b.WriteString(serial.Concat("\n  1. filter: ", s.describeFilter()))
	writeTable(&b, filterTrace)

	b.WriteString("\n  2. rank by score = rtt deviation x sqrt(cost weight), lower is better")
	rows := make([]string, 0, len(qualified))
	for i, n := range qualified {
		weight := s.costs.Get(n.Tag)
		mark := ""
		if i < len(selects) {
			mark = "  <- selected"
		}
		rows = append(rows, serial.Concat("#", i+1, " ", n.Tag, "\tscore ", fmtDuration(n.RTTDeviationCost),
			"\t= deviation ", fmtDuration(n.RTTDeviation), " x ", fmtNumber(math.Sqrt(weight)),
			"\tweight ", fmtNumber(weight), " (", s.costs.Describe(n.Tag), ")",
			"\tavg rtt ", fmtDuration(n.RTTAverage), mark))
	}
	if len(rows) == 0 {
		rows = append(rows, "(no outbound left)")
	}
	writeTable(&b, rows)

	b.WriteString(serial.Concat("\n  3. select: expected ", s.settings.Expected, ", baselines ", fmtBaselines(s.settings.Baselines)))
	writeTable(&b, selectTrace)

	if len(selects) == 0 {
		b.WriteString("\n  => nothing selected, the balancer uses its fallbackTag")
	} else {
		tags := make([]string, 0, len(selects))
		for _, n := range selects {
			tags = append(tags, n.Tag)
		}
		b.WriteString(serial.Concat("\n  => selected: ", strings.Join(tags, ", ")))
		if len(selects) > 1 {
			b.WriteString(" (each connection picks one of them at random)")
		}
	}

	report := b.String()
	s.reportMu.Lock()
	changed := report != s.lastReport
	s.lastReport = report
	s.reportMu.Unlock()
	if changed {
		errors.LogDebug(s.ctx, report)
	}
}

func (s *LeastLoadStrategy) describeFilter() string {
	checks := []string{"alive"}
	if s.settings.MaxRTT > 0 {
		checks = append(checks, serial.Concat("rtt < maxRTT ", time.Duration(s.settings.MaxRTT)))
	}
	if s.settings.Tolerance > 0 {
		checks = append(checks, serial.Concat("failed pings <= tolerance ", fmtPercent(float64(s.settings.Tolerance))))
	}
	return strings.Join(checks, ", ")
}

// describeSettings explains how the configured settings are going to be applied
func (s *LeastLoadStrategy) describeSettings() string {
	var b strings.Builder
	b.WriteString(serial.Concat("balancer [", s.tag, "] leastLoad settings:"))

	expected := s.settings.Expected
	switch {
	case len(s.settings.Baselines) == 0 && expected > 0:
		b.WriteString(serial.Concat("\n  expected ", expected, ": select the ", expected, " best outbound(s) by score"))
	case len(s.settings.Baselines) == 0:
		b.WriteString("\n  expected not set: select the single best outbound by score")
	case expected > 0:
		b.WriteString(serial.Concat("\n  expected ", expected, ": select all outbounds under the first baseline that admits at least ", expected,
			", if none does, the ", expected, " best by score"))
	default:
		b.WriteString("\n  expected not set: select all outbounds under the first baseline that admits any, if none does, nothing (fallbackTag is used)")
	}

	if s.settings.MaxRTT > 0 {
		b.WriteString(serial.Concat("\n  maxRTT ", time.Duration(s.settings.MaxRTT), ": drop outbounds whose average rtt is >= ", time.Duration(s.settings.MaxRTT)))
	} else {
		b.WriteString("\n  maxRTT not set: no rtt limit")
	}

	if s.settings.Tolerance > 0 {
		b.WriteString(serial.Concat("\n  tolerance ", fmtNumber(float64(s.settings.Tolerance)), ": drop outbounds whose failed health pings exceed ",
			fmtPercent(float64(s.settings.Tolerance)), " of the recent samples"))
	} else {
		b.WriteString("\n  tolerance not set: failed health pings drop an outbound only when all of them failed")
	}

	if len(s.settings.Baselines) > 0 {
		b.WriteString(serial.Concat("\n  baselines ", fmtBaselines(s.settings.Baselines), ": score limits tried in order"))
	} else {
		b.WriteString("\n  baselines not set")
	}

	if len(s.settings.Costs) == 0 {
		b.WriteString("\n  costs not set: every weight is 1, score = rtt deviation")
	}
	for i, w := range s.settings.Costs {
		b.WriteString(serial.Concat("\n  cost rule #", i+1, " ", describeWeightMatch(w), ": "))
		if w.Value > 0 {
			b.WriteString(serial.Concat("weight ", fmtNumber(float64(w.Value)), ", score = rtt deviation x ", fmtNumber(math.Sqrt(float64(w.Value)))))
		} else {
			b.WriteString("no value, weight is the number found in the matched text, else 1")
		}
	}
	b.WriteString("\n  rules are tried in order, the first match decides; outbounds matching none get weight 1")
	return b.String()
}

// writeTable writes tab separated rows as indented lines with aligned columns
func writeTable(b *strings.Builder, rows []string) {
	var widths []int
	cells := make([][]string, 0, len(rows))
	for _, row := range rows {
		cols := strings.Split(row, "\t")
		for i, col := range cols {
			if i == len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], len(col))
		}
		cells = append(cells, cols)
	}
	for _, cols := range cells {
		b.WriteString("\n     ")
		for i, col := range cols {
			if i == len(cols)-1 {
				b.WriteString(col)
				break
			}
			b.WriteString(col)
			b.WriteString(strings.Repeat(" ", widths[i]-len(col)+2))
		}
	}
}

// allowedFails returns how many failed pings out of all the tolerance accepts
func allowedFails(tolerance float64, all int64) int64 {
	return int64(math.Floor(tolerance*float64(all) + 1e-9))
}

func fmtDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(100 * time.Microsecond).String()
	default:
		return d.Round(time.Microsecond).String()
	}
}

func fmtNumber(f float64) string {
	if f >= 1e4 && f < 1e15 {
		return strconv.FormatFloat(f, 'f', 0, 64)
	}
	return strconv.FormatFloat(f, 'g', 4, 64)
}

func fmtPercent(f float64) string {
	return strconv.FormatFloat(f*100, 'g', 3, 64) + "%"
}

func fmtBaselines(baselines []int64) string {
	ds := make([]string, 0, len(baselines))
	for _, b := range baselines {
		ds = append(ds, time.Duration(b).String())
	}
	return "[" + strings.Join(ds, ", ") + "]"
}
