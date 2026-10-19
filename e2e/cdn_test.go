package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dynatrace "github.com/Dynatrace/openfeature-provider-go"
	"github.com/Dynatrace/openfeature-provider-go/internal"
	"github.com/cucumber/godog"
	"github.com/open-feature/go-sdk/openfeature"
)

const (
	// well-formed server SDK key every READY arrangement initializes with
	baseKey = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	// shared flag configuration fixtures from the harness submodule
	flagsDir = harnessDir + "/flags"
	// the provider's poll cadence
	pollInterval = internal.DefaultPollInterval
	// bound on one CDN attempt (KB section 2.1)
	requestTimeout = internal.RequestTimeout
	// longer than the poll cadence, so a refresh is observed
	pollTimeout = pollInterval + time.Second
	// covers the time taken by the retries for 5xx codes
	retryTimeout = 2 * time.Second
	// window over which a "continues to" assertion must keep holding
	stableWindow = 2 * time.Second
	// time a state change may take to arrive, since state rides OpenFeature events
	stateTimeout = 2 * time.Second
	// slack added when sleeping to a tick boundary, so the tick has fired by the time we look
	tickMargin = 500 * time.Millisecond
	// cap on waiting for expected requests; a fetch is at most two bounded attempts
	countTimeout = 3 * requestTimeout
	// how long an exact request count must keep holding, so a stray extra request still fails
	countSettle = time.Second
	// tolerance on a measured interval; wide enough for CI jitter, narrow enough to catch a wrongly anchored cadence
	cadenceTolerance = 1500 * time.Millisecond
	// requests closer than this belong to the same poll tick (a retry follows one request timeout later)
	sameTickGap = pollInterval / 2
	// cap on waiting for the first poll tick: one interval plus a whole fetch
	firstTickTimeout = pollInterval + countTimeout
	// outlives a whole fetch (two attempts, each abandoned at the timeout), so it spans a tick
	slowResponseDelay = 4 * requestTimeout
	// takes real time without timing out, so fetch start and fetch end are far enough apart to tell apart
	slowButWithinTimeoutDelay = requestTimeout - time.Second
	// no periodic metrics flush during a scenario
	metricsFlushInterval = time.Hour
	// a cadence needs at least two ticks to measure a gap
	minTicksToCompare = 2
	// how often waitFor re-checks its condition
	waitPollStep = 100 * time.Millisecond
	// how often assertHoldsFor re-checks its condition
	holdCheckStep = 50 * time.Millisecond
	// never matches an expected count, so a failed control call fails the assertion
	requestCountUnavailable = -1
)

var (
	mock *mockServer
	// gives every scenario a unique OpenFeature domain so providers never leak across scenarios
	scenarioSeq atomic.Int64
)

func TestCDNE2E(t *testing.T) {
	suite := godog.TestSuite{
		Name: "cdn",
		TestSuiteInitializer: func(sc *godog.TestSuiteContext) {
			sc.AfterSuite(func() {
				if mock != nil {
					_ = mock.stop(context.Background())
				}
			})
		},
		ScenarioInitializer: initializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{filepath.Join(harnessDir, "gherkin")},
			Strict:   true,
			TestingT: t,
			// single-flight needs an SSE-triggered re-fetch; SSE is not implemented yet
			Tags: "~@single-flight && ~@sse",
		},
	}
	if suite.Run() != 0 {
		t.Fatal("cdn e2e suite failed")
	}
}

// scenario holds one scenario's state, shared across its steps; each one binds a fresh provider to its own OpenFeature domain.
type scenario struct {
	domain  string
	client  *openfeature.Client
	sdkKey  string
	options []dynatrace.ProviderOption

	initError error
	// baseline for "further request" counts, reset by every clock-advancing step
	countBaseline int
	// when initialization returned; the polling timer and every tick boundary are anchored here
	pollingStartedAtMs int64
	// CDN responses currently set on the mock, so a delay can be layered onto them
	cdnResponses []cdnResponse
	// Retry-After the CDN was set up with, so a scenario can wait that window out
	retryAfter time.Duration
}

func initializeScenario(sc *godog.ScenarioContext) {
	s := &scenario{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.domain = fmt.Sprintf("e2e-test-%d", scenarioSeq.Add(1))
		s.client = openfeature.NewClient(s.domain)
		if mock != nil {
			return ctx, mock.reset()
		}
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		openfeature.Shutdown()
		return ctx, nil
	})

	// GIVEN
	sc.Step(`^a mock server is running$`, s.aMockServerIsRunning)
	sc.Step(`^the SDK key "([^"]*)"$`, s.theSDKKey)
	sc.Step(`^the CDN serves the "([^"]*)" flag configuration$`, s.theCDNServesTheFlagConfiguration)
	sc.Step(`^an initialized, READY provider serving the "([^"]*)" flag configuration(?: with "([^"]*)" header "([^"]*)")?$`, s.anInitializedReadyProviderServingTheFlagConfiguration)
	sc.Step(`^the CDN responds slower than the request timeout$`, s.theCDNRespondsSlowerThanTheRequestTimeout)
	sc.Step(`^the CDN responds slowly but within the request timeout$`, s.theCDNRespondsSlowlyButWithinTheRequestTimeout)

	// WHEN
	sc.Step(`^the provider is initialized$`, s.theProviderIsInitialized)
	sc.Step(`^the CDN responds with status (\d+)$`, s.theCDNRespondsWithStatus)
	sc.Step(`^the CDN responds with status (\d+) and body ("[^"]*"|null)$`, s.theCDNRespondsWithStatusAndBody)
	sc.Step(`^the CDN responds with status (\d+) and the "([^"]*)" flag configuration with "([^"]*)" header "([^"]*)"$`, s.theCDNRespondsWithStatusAndTheFlagConfigurationWithHeader)
	sc.Step(`^the CDN responds (\d+) then (\d+) with the "([^"]*)" configuration$`, s.theCDNRespondsThenWithTheConfiguration)
	sc.Step(`^the CDN responds with status (\d+) and Retry-After (\d+) seconds$`, s.theCDNRespondsWithStatusAndRetryAfterSeconds)
	sc.Step(`^polling triggers a configuration refetch$`, s.pollingTriggersAConfigurationRefetch)
	sc.Step(`^(\d+) poll intervals? elapses?$`, s.pollIntervalsElapse)
	sc.Step(`^the next poll tick arrives$`, s.theNextPollTickArrives)
	sc.Step(`^the Retry-After window expires$`, s.theRetryAfterWindowExpires)
	sc.Step(`^an SSE re-fetch is triggered shortly before the next poll tick$`, s.anSSERefetchIsTriggeredShortlyBeforeTheNextPollTick)

	// THEN
	sc.Step(`^initialization succeeds$`, s.initializationSucceeds)
	sc.Step(`^initialization fails$`, s.initializationFails)
	sc.Step(`^the provider state is "([^"]*)"$`, s.theProviderStateIs)
	sc.Step(`^the initial CDN request has no "([^"]*)" header$`, s.theInitialCDNRequestHasNoHeader)
	sc.Step(`^the (\d+)\. CDN request has the "([^"]*)" header "([^"]*)"$`, s.theCDNRequestHasTheHeader)
	sc.Step(`^the CDN has received (\d+) further requests?$`, s.theCDNHasReceivedFurtherRequests)
	sc.Step(`^consecutive poll ticks are one poll interval apart$`, s.consecutivePollTicksAreOnePollIntervalApart)
	sc.Step(`^the first poll tick is one poll interval after initialization completed$`, s.theFirstPollTickIsOnePollIntervalAfterInitializationCompleted)
	sc.Step(`^flag "([^"]*)" evaluates to (\w+)$`, s.flagEvaluatesTo)
	sc.Step(`^flag "([^"]*)" eventually evaluates to (\w+)$`, s.flagEventuallyEvaluatesTo)
	sc.Step(`^flag "([^"]*)" continues to evaluate to (\w+)$`, s.flagContinuesToEvaluateTo)
	sc.Step(`^evaluating flag "([^"]*)" returns a Flag-Not-Found error$`, s.evaluatingFlagReturnsAFlagNotFoundError)
}

// GIVEN

func (s *scenario) aMockServerIsRunning() error {
	if mock == nil {
		m, err := startMockServer(context.Background())
		if err != nil {
			return err
		}
		mock = m
		if err := mock.reset(); err != nil {
			return err
		}
	}
	s.options = []dynatrace.ProviderOption{
		dynatrace.WithConfigOrigin(mock.origin),
		dynatrace.WithMetricsOrigin(mock.origin),
		dynatrace.WithMetricsFlushInterval(metricsFlushInterval),
	}
	return nil
}

func (s *scenario) theSDKKey(key string) error {
	s.sdkKey = key
	return nil
}

func (s *scenario) theCDNServesTheFlagConfiguration(name string) error {
	body, err := loadFlagConfig(name)
	if err != nil {
		return err
	}
	return s.setCDNResponses(buildCDNResponse(http.StatusOK, body))
}

// header is empty when the step has no "with ... header ..." suffix
func (s *scenario) anInitializedReadyProviderServingTheFlagConfiguration(name, header, value string) error {
	body, err := loadFlagConfig(name)
	if err != nil {
		return err
	}
	response := buildCDNResponse(http.StatusOK, body)
	if header != "" {
		response = response.withHeader(header, value)
	}
	s.sdkKey = baseKey
	if err := s.setCDNResponses(response); err != nil {
		return err
	}
	if err := s.registerProviderWithSDK(); err != nil {
		return err
	}
	if got := s.client.State(); got != openfeature.ReadyState {
		return fmt.Errorf("expected READY after arrangement, got %s", got)
	}
	// end of arrangement: counts and tick boundaries run from here
	s.pollingStartedAtMs = nowMs()
	return s.resetCountBaseline()
}

func (s *scenario) theCDNRespondsSlowerThanTheRequestTimeout() error {
	return s.delayCDNResponses(slowResponseDelay)
}

func (s *scenario) theCDNRespondsSlowlyButWithinTheRequestTimeout() error {
	return s.delayCDNResponses(slowButWithinTimeoutDelay)
}

// WHEN

func (s *scenario) theProviderIsInitialized() error {
	s.initError = s.registerProviderWithSDK()
	// anchor, not baseline: a scenario with no arrangement counts the requests initialization made
	s.pollingStartedAtMs = nowMs()
	return nil
}

func (s *scenario) theCDNRespondsWithStatus(code int) error {
	return s.setCDNResponses(buildCDNResponse(code))
}

// unquoted null is an absent body, as opposed to the empty-string body ""
func (s *scenario) theCDNRespondsWithStatusAndBody(code int, body string) error {
	if body == "null" {
		return s.setCDNResponses(buildCDNResponse(code))
	}
	return s.setCDNResponses(buildCDNResponse(code, strings.Trim(body, `"`)))
}

func (s *scenario) theCDNRespondsWithStatusAndTheFlagConfigurationWithHeader(code int, name, header, value string) error {
	body, err := loadFlagConfig(name)
	if err != nil {
		return err
	}
	return s.setCDNResponses(buildCDNResponse(code, body).withHeader(header, value))
}

func (s *scenario) theCDNRespondsThenWithTheConfiguration(first, second int, name string) error {
	body, err := loadFlagConfig(name)
	if err != nil {
		return err
	}
	return s.setCDNResponses(buildCDNResponse(first), buildCDNResponse(second, body))
}

func (s *scenario) theCDNRespondsWithStatusAndRetryAfterSeconds(code, seconds int) error {
	s.retryAfter = time.Duration(seconds) * time.Second
	return s.setCDNResponses(buildCDNResponse(code).withHeader("Retry-After", strconv.Itoa(seconds)))
}

func (s *scenario) pollingTriggersAConfigurationRefetch() error {
	baseline, err := mock.countCDNRequests()
	if err != nil {
		return err
	}
	return waitFor(pollTimeout, func() bool {
		n, err := mock.countCDNRequests()
		return err == nil && n > baseline
	})
}

// baseline is taken before sleeping, so the next assertion counts what these intervals produced
func (s *scenario) pollIntervalsElapse(intervals int) error {
	if err := s.resetCountBaseline(); err != nil {
		return err
	}
	sleepUntil(s.computeNextTickAtMs() + int64(intervals-1)*pollInterval.Milliseconds() + tickMargin.Milliseconds())
	return nil
}

// waits for the next cadence-anchored tick, not a duration; the Retry-After window expiring does not itself fetch
func (s *scenario) theNextPollTickArrives() error {
	if err := s.resetCountBaseline(); err != nil {
		return err
	}
	sleepUntil(s.computeNextTickAtMs() + tickMargin.Milliseconds())
	return nil
}

func (s *scenario) theRetryAfterWindowExpires() error {
	if err := s.resetCountBaseline(); err != nil {
		return err
	}
	time.Sleep(s.retryAfter + tickMargin)
	return nil
}

// Pending: emit a refetchConfig event via POST /__control__/sse/emit shortly before the next poll tick,
// once the provider has an SSE client
func (s *scenario) anSSERefetchIsTriggeredShortlyBeforeTheNextPollTick() error {
	return godog.ErrPending
}

// THEN

func (s *scenario) initializationSucceeds() error {
	if s.initError != nil {
		return fmt.Errorf("expected init to succeed but got: %w", s.initError)
	}
	return nil
}

func (s *scenario) initializationFails() error {
	if s.initError == nil {
		return errors.New("expected initialization to fail")
	}
	return nil
}

// state reaches the client over an OpenFeature event, so allow it a moment to arrive
func (s *scenario) theProviderStateIs(want string) error {
	if err := waitFor(stateTimeout, func() bool { return string(s.client.State()) == want }); err != nil {
		return fmt.Errorf("expected provider state %s, got %s", want, s.client.State())
	}
	return nil
}

func (s *scenario) theInitialCDNRequestHasNoHeader(header string) error {
	req, err := getNthCDNRequest(0)
	if err != nil {
		return err
	}
	if _, ok := req.getHeader(header); ok {
		return fmt.Errorf("initial request unexpectedly carried header: %s", header)
	}
	return nil
}

func (s *scenario) theCDNRequestHasTheHeader(index int, header, value string) error {
	req, err := getNthCDNRequest(index - 1)
	if err != nil {
		return err
	}
	got, ok := req.getHeader(header)
	if !ok {
		return fmt.Errorf("%d. CDN request missing expected header: %s", index, header)
	}
	if got != value {
		return fmt.Errorf("%d. CDN request %s header mismatch: expected %q, got %q", index, header, value, got)
	}
	return nil
}

// the count must settle, not merely be reached: these are upper bounds, which only hold if they keep holding
func (s *scenario) theCDNHasReceivedFurtherRequests(count int) error {
	if count > 0 {
		if err := waitFor(countTimeout, func() bool { return s.countFurtherRequests() >= count }); err != nil {
			return fmt.Errorf("expected %d further CDN request(s), saw %d", count, s.countFurtherRequests())
		}
	}
	if err := assertHoldsFor(countSettle, func() bool { return s.countFurtherRequests() == count }); err != nil {
		return fmt.Errorf("expected %d further CDN request(s), saw %d", count, s.countFurtherRequests())
	}
	return s.resetCountBaseline()
}

// the cadence is anchored to when each fetch was issued, not when it finished
func (s *scenario) consecutivePollTicksAreOnePollIntervalApart() error {
	reqs, err := mock.fetchCDNRequests()
	if err != nil {
		return err
	}
	// arrival of the first request of each tick; requests under half an interval apart are one tick's attempts
	var ticks []int64
	for i, r := range reqs {
		if i == 0 || r.ReceivedAtMs-reqs[i-1].ReceivedAtMs >= sameTickGap.Milliseconds() {
			ticks = append(ticks, r.ReceivedAtMs)
		}
	}
	if len(ticks) < minTicksToCompare {
		return fmt.Errorf("need at least %d poll ticks to compare, saw %d", minTicksToCompare, len(ticks))
	}
	for i := 1; i < len(ticks); i++ {
		if err := assertCadence(ticks[i]-ticks[i-1], fmt.Sprintf("gap between poll tick %d and %d", i, i+1)); err != nil {
			return err
		}
	}
	return nil
}

// the polling timer starts once initialization has finished, not when it began
func (s *scenario) theFirstPollTickIsOnePollIntervalAfterInitializationCompleted() error {
	anchor := s.pollingStartedAtMs
	var first int64
	found := func() bool {
		reqs, err := mock.fetchCDNRequests()
		if err != nil {
			return false
		}
		for _, r := range reqs {
			if r.ReceivedAtMs > anchor {
				first = r.ReceivedAtMs
				return true
			}
		}
		return false
	}
	if err := waitFor(firstTickTimeout, found); err != nil {
		return errors.New("no poll tick observed after initialization completed")
	}
	return assertCadence(first-anchor, "gap between initialization completing and the first poll tick")
}

func (s *scenario) flagEvaluatesTo(flag, expected string) error {
	want, err := strconv.ParseBool(expected)
	if err != nil {
		return err
	}
	if !s.isFlagValue(flag, want) {
		return fmt.Errorf("flag %s did not evaluate to %s", flag, expected)
	}
	return nil
}

func (s *scenario) flagEventuallyEvaluatesTo(flag, expected string) error {
	want, err := strconv.ParseBool(expected)
	if err != nil {
		return err
	}
	if err := waitFor(retryTimeout, func() bool { return s.isFlagValue(flag, want) }); err != nil {
		return fmt.Errorf("flag %s did not eventually evaluate to %s", flag, expected)
	}
	return nil
}

func (s *scenario) flagContinuesToEvaluateTo(flag, expected string) error {
	if err := s.flagEvaluatesTo(flag, expected); err != nil {
		return err
	}
	want, _ := strconv.ParseBool(expected)
	if err := assertHoldsFor(stableWindow, func() bool { return s.isFlagValue(flag, want) }); err != nil {
		return fmt.Errorf("flag %s changed away from %s", flag, expected)
	}
	return nil
}

func (s *scenario) evaluatingFlagReturnsAFlagNotFoundError(flag string) error {
	details, _ := s.client.BooleanValueDetails(context.Background(), flag, false, openfeature.EvaluationContext{})
	if details.ErrorCode != openfeature.FlagNotFoundCode {
		return fmt.Errorf("expected FLAG_NOT_FOUND for unknown flag %s, got %q", flag, details.ErrorCode)
	}
	return nil
}

// setCDNResponses sets the CDN responses on the mock and remembers them, so a later step can layer a delay onto them.
func (s *scenario) setCDNResponses(responses ...cdnResponse) error {
	s.cdnResponses = responses
	return mock.setCDNResponses(responses)
}

// delayCDNResponses holds back every current CDN response by delay before the mock answers.
func (s *scenario) delayCDNResponses(delay time.Duration) error {
	if len(s.cdnResponses) == 0 {
		return errors.New("no CDN response set to delay")
	}
	for i := range s.cdnResponses {
		s.cdnResponses[i] = s.cdnResponses[i].withDelay(delay)
	}
	return mock.setCDNResponses(s.cdnResponses)
}

// registerProviderWithSDK embeds a fresh provider under this scenario's domain; a malformed key fails in NewProvider, leaving the domain unbound.
func (s *scenario) registerProviderWithSDK() error {
	provider, err := dynatrace.NewProvider(s.sdkKey, s.options...)
	if err != nil {
		return err
	}
	return openfeature.SetNamedProviderAndWait(s.domain, provider)
}

// resetCountBaseline makes later "further request" counts start from the current request count.
func (s *scenario) resetCountBaseline() error {
	n, err := mock.countCDNRequests()
	if err != nil {
		return err
	}
	s.countBaseline = n
	return nil
}

// countFurtherRequests returns the number of CDN requests received since the count baseline.
func (s *scenario) countFurtherRequests() int {
	n, err := mock.countCDNRequests()
	if err != nil {
		return requestCountUnavailable
	}
	return n - s.countBaseline
}

// computeNextTickAtMs returns the next tick boundary on the cadence anchored at pollingStartedAtMs.
func (s *scenario) computeNextTickAtMs() int64 {
	period := pollInterval.Milliseconds()
	elapsed := nowMs() - s.pollingStartedAtMs
	return s.pollingStartedAtMs + (elapsed/period+1)*period
}

// getNthCDNRequest returns the 0-indexed recorded CDN request, failing if fewer were received.
func getNthCDNRequest(index int) (recordedRequest, error) {
	reqs, err := mock.fetchCDNRequests()
	if err != nil {
		return recordedRequest{}, err
	}
	if len(reqs) <= index {
		return recordedRequest{}, fmt.Errorf("expected at least %d CDN request(s), saw %d", index+1, len(reqs))
	}
	return reqs[index], nil
}

// isFlagValue reports whether the flag evaluates to expected; it defaults to the opposite, so a missing flag never masquerades as the expected value.
func (s *scenario) isFlagValue(flag string, expected bool) bool {
	return s.client.Boolean(context.Background(), flag, !expected, openfeature.EvaluationContext{}) == expected
}

// assertCadence fails unless measuredMs is within cadenceTolerance of one poll interval.
func assertCadence(measuredMs int64, what string) error {
	expected := pollInterval.Milliseconds()
	if diff := measuredMs - expected; diff > cadenceTolerance.Milliseconds() || -diff > cadenceTolerance.Milliseconds() {
		return fmt.Errorf("%s was %dms, expected %dms +/- %dms", what, measuredMs, expected, cadenceTolerance.Milliseconds())
	}
	return nil
}

// loadFlagConfig returns a flag configuration fixture from the harness by name, e.g. "flags-v1".
func loadFlagConfig(name string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(flagsDir, name+".json"))
	if err != nil {
		return "", fmt.Errorf("read flag config fixture %s: %w", name, err)
	}
	return string(raw), nil
}

// waitFor polls cond until it holds, failing once timeout passes.
func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(waitPollStep)
	}
	if cond() {
		return nil
	}
	return fmt.Errorf("condition not met within %s", timeout)
}

// assertHoldsFor fails if cond stops holding at any point during duration.
func assertHoldsFor(duration time.Duration, cond func() bool) error {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if !cond() {
			return errors.New("condition stopped holding")
		}
		time.Sleep(holdCheckStep)
	}
	if !cond() {
		return errors.New("condition stopped holding")
	}
	return nil
}

// sleepUntil sleeps until the given epoch milliseconds, returning at once if that time has passed.
func sleepUntil(epochMs int64) {
	if remaining := epochMs - nowMs(); remaining > 0 {
		time.Sleep(time.Duration(remaining) * time.Millisecond)
	}
}

// nowMs returns the current time in epoch milliseconds, the unit the mock stamps requests with.
func nowMs() int64 {
	return time.Now().UnixMilli()
}
