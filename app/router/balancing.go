package router

import (
	"context"
	sync "sync"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
)

type BalancingStrategy interface {
	PickOutbound([]string) string
}

type BalancingPrincipleTarget interface {
	GetPrincipleTarget([]string) []string
}

type RoundRobinStrategy struct {
	FallbackTag string

	ctx         context.Context
	observatory extension.Observatory
	mu          sync.Mutex
	index       int
}

func (s *RoundRobinStrategy) InjectContext(ctx context.Context) {
	s.ctx = ctx
	if len(s.FallbackTag) > 0 {
		common.Must(core.RequireFeatures(s.ctx, func(observatory extension.Observatory) error {
			s.observatory = observatory
			return nil
		}))
	}
}

func (s *RoundRobinStrategy) GetPrincipleTarget(strings []string) []string {
	return strings
}

func (s *RoundRobinStrategy) PickOutbound(tags []string) string {
	if s.observatory != nil {
		observeReport, err := s.observatory.GetObservation(s.ctx)
		if err == nil {
			aliveTags := make([]string, 0)
			if result, ok := observeReport.(*observatory.ObservationResult); ok {
				status := result.Status
				statusMap := make(map[string]*observatory.OutboundStatus)
				for _, outboundStatus := range status {
					statusMap[outboundStatus.OutboundTag] = outboundStatus
				}
				for _, candidate := range tags {
					if outboundStatus, found := statusMap[candidate]; found {
						if outboundStatus.Alive {
							aliveTags = append(aliveTags, candidate)
						}
					} else {
						// unfound candidate is considered alive
						aliveTags = append(aliveTags, candidate)
					}
				}
				tags = aliveTags
			}
		}
	}

	n := len(tags)
	if n == 0 {
		// goes to fallbackTag
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tag := tags[s.index%n]
	s.index = (s.index + 1) % n
	return tag
}

type Balancer struct {
	tag          string
	strategyName string
	selectors    []string
	strategy     BalancingStrategy
	ohm          outbound.Manager
	fallbackTag  string

	override override
}

// PickOutbound picks the tag of a outbound
func (b *Balancer) PickOutbound() (string, error) {
	candidates, err := b.SelectOutbounds()
	if err != nil {
		if b.fallbackTag != "" {
			errors.LogInfo(context.Background(), "balancer [", b.tag, "]: fallback to [", b.fallbackTag, "], due to error: ", err)
			return b.fallbackTag, nil
		}
		return "", err
	}
	var tag, source string
	if o := b.override.Get(); o != "" {
		tag, source = o, "override set via API, strategy skipped"
	} else {
		tag, source = b.strategy.PickOutbound(candidates), b.strategyName
	}
	if tag == "" {
		if b.fallbackTag != "" {
			errors.LogInfo(context.Background(), "balancer [", b.tag, "]: ", b.strategyName, " selected no outbound, fallback to [", b.fallbackTag, "]")
			return b.fallbackTag, nil
		}
		// will use default handler
		return "", errors.New("balancer [", b.tag, "]: ", b.strategyName, " selected no outbound and no fallbackTag is set")
	}
	errors.LogDebug(context.Background(), "balancer [", b.tag, "]: picked [", tag, "] (", source, ")")
	return tag, nil
}

func (b *Balancer) InjectContext(ctx context.Context) {
	errors.LogDebug(ctx, "balancer [", b.tag, "]: strategy ", b.strategyName, ", selector ", b.selectors,
		" (outbound tags starting with any of them), fallbackTag [", b.fallbackTag, "]")
	if contextReceiver, ok := b.strategy.(extension.ContextReceiver); ok {
		contextReceiver.InjectContext(ctx)
	}
}

// SelectOutbounds select outbounds with selectors of the Balancer
func (b *Balancer) SelectOutbounds() ([]string, error) {
	hs, ok := b.ohm.(outbound.HandlerSelector)
	if !ok {
		return nil, errors.New("outbound.Manager is not a HandlerSelector")
	}
	tags := hs.Select(b.selectors)
	return tags, nil
}

// GetPrincipleTarget implements routing.BalancerPrincipleTarget
func (r *Router) GetPrincipleTarget(tag string) ([]string, error) {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		if s, ok := b.strategy.(BalancingPrincipleTarget); ok {
			candidates, err := b.SelectOutbounds()
			if err != nil {
				return nil, errors.New("unable to select outbounds").Base(err)
			}
			return s.GetPrincipleTarget(candidates), nil
		}
		return nil, errors.New("unsupported GetPrincipleTarget")
	}
	return nil, errors.New("cannot find tag")
}

// SetOverrideTarget implements routing.BalancerOverrider
func (r *Router) SetOverrideTarget(tag, target string) error {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		b.override.Put(target)
		return nil
	}
	return errors.New("cannot find tag")
}

// GetOverrideTarget implements routing.BalancerOverrider
func (r *Router) GetOverrideTarget(tag string) (string, error) {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		return b.override.Get(), nil
	}
	return "", errors.New("cannot find tag")
}
