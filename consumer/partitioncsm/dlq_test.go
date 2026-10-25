package partitioncsm

import (
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/deadletter"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

// DlqSuite is a hermetic suite for the retry-exhausted dead lettering
// strategy: no Kafka broker is involved, production receipts, offset
// commits and the partition lifecycle are all controlled by test doubles.
// It is registered with the package-level gocheck runner (Test in
// partitioncsm_test.go), so it runs together with PartitionCsmSuite.

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// fakeDlqCall is one controllable production attempt.
type fakeDlqCall struct {
	topic     string
	key       []byte
	value     []byte
	headers   []sarama.RecordHeader
	receiptCh chan deadletter.Receipt
}

func (call *fakeDlqCall) identity() string {
	return dlqHeader(call.headers, deadletter.HeaderIdentity)
}

// fakeDlqProducer records every production attempt and lets the test decide
// when (and whether) a receipt is delivered.
type fakeDlqProducer struct {
	mu     sync.Mutex
	calls  []*fakeDlqCall
	callCh chan *fakeDlqCall
}

func newFakeDlqProducer() *fakeDlqProducer {
	return &fakeDlqProducer{callCh: make(chan *fakeDlqCall, 64)}
}

func (p *fakeDlqProducer) AsyncProduce(topic string, key, value sarama.Encoder,
	headers []sarama.RecordHeader,
) <-chan deadletter.Receipt {
	k, _ := key.Encode()
	v, _ := value.Encode()
	call := &fakeDlqCall{
		topic:     topic,
		key:       k,
		value:     v,
		headers:   headers,
		receiptCh: make(chan deadletter.Receipt, 1),
	}
	p.mu.Lock()
	p.calls = append(p.calls, call)
	p.mu.Unlock()
	select {
	case p.callCh <- call:
	default:
	}
	return call.receiptCh
}

func (p *fakeDlqProducer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *fakeDlqProducer) snapshotCalls() []*fakeDlqCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*fakeDlqCall, len(p.calls))
	copy(out, p.calls)
	return out
}

func (p *fakeDlqProducer) callForOffset(offset int64) *fakeDlqCall {
	want := offsetIdentity(offset)
	for _, call := range p.snapshotCalls() {
		if call.identity() == want {
			return call
		}
	}
	return nil
}

func (p *fakeDlqProducer) nextCall(c *C, within time.Duration) *fakeDlqCall {
	select {
	case call := <-p.callCh:
		return call
	case <-time.After(within):
		c.Fatalf("no dead letter production within %v (got %d)", within, p.callCount())
		return nil
	}
}

// fakeOffsetMgr records every submitted offset and persists the newest one,
// emulating Kafka offset commits without any broker.
type fakeOffsetMgr struct {
	f        *fakeOffsetMgrFactory
	committed chan offsetmgr.Offset
	mu       sync.Mutex
	submits  []offsetmgr.Offset
}

func (m *fakeOffsetMgr) SubmitOffset(offset offsetmgr.Offset) {
	m.mu.Lock()
	m.submits = append(m.submits, offset)
	m.mu.Unlock()
	m.f.mu.Lock()
	m.f.current = offset
	m.f.mu.Unlock()
	select {
	case m.committed <- offset:
	default:
	}
}

func (m *fakeOffsetMgr) CommittedOffsets() <-chan offsetmgr.Offset {
	return m.committed
}

func (m *fakeOffsetMgr) Stop() {
	close(m.committed)
}

func (m *fakeOffsetMgr) snapshotSubmits() []offsetmgr.Offset {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]offsetmgr.Offset, len(m.submits))
	copy(out, m.submits)
	return out
}

type fakeOffsetMgrFactory struct {
	mu      sync.Mutex
	current offsetmgr.Offset
	oms     []*fakeOffsetMgr
}

func (f *fakeOffsetMgrFactory) Spawn(*actor.Descriptor, string, string, int32) (offsetmgr.T, error) {
	om := &fakeOffsetMgr{f: f, committed: make(chan offsetmgr.Offset, 64)}
	f.mu.Lock()
	om.committed <- f.current
	f.oms = append(f.oms, om)
	f.mu.Unlock()
	return om, nil
}

func (*fakeOffsetMgrFactory) Stop() {}

func (f *fakeOffsetMgrFactory) lastOM() *fakeOffsetMgr {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.oms[len(f.oms)-1]
}

func (f *fakeOffsetMgrFactory) committed() offsetmgr.Offset {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

// fakeFetcher is demand driven: unlike a channel pre-filled with the whole
// script it delivers a script message only after the test schedules it with
// feed. That mirrors the production back-pressure under which no further
// source records are handed to the partition consumer until the current one
// is offered, which is what makes retry deadlines expire deterministically.
type fakeFetcher struct {
	msgs chan consumer.Message
	feed chan consumer.Message
	done chan none.T
	wg   sync.WaitGroup
}

func (f *fakeFetcher) Messages() <-chan consumer.Message { return f.msgs }

func (f *fakeFetcher) Stop() {
	close(f.done)
	f.wg.Wait()
}

func (f *fakeFetcher) run() {
	defer f.wg.Done()
	for {
		select {
		case m := <-f.feed:
			select {
			case f.msgs <- m:
			case <-f.done:
				return
			}
		case <-f.done:
			return
		}
	}
}

type fakeFetcherFactory struct {
	mu        sync.Mutex
	script    []consumer.Message
	fetchers  []*fakeFetcher
	expected  int
}

func (*fakeFetcherFactory) Stop() {}

func (f *fakeFetcherFactory) Spawn(_ *actor.Descriptor, _ string, _ int32, offset int64,
) (msgfetcher.T, int64, error) {
	ff := &fakeFetcher{
		msgs: make(chan consumer.Message),
		feed: make(chan consumer.Message, 1),
		done: make(chan none.T),
	}
	ff.wg.Add(1)
	go ff.run()
	f.mu.Lock()
	f.fetchers = append(f.fetchers, ff)
	f.mu.Unlock()
	return ff, offset, nil
}

// expectSpawn records that a partition consumer (re)spawn is about to create
// a fetcher. feed blocks until the most recently expected fetcher exists,
// since fetcher creation happens on the partition consumer goroutine.
func (f *fakeFetcherFactory) expectSpawn() {
	f.mu.Lock()
	f.expected++
	f.mu.Unlock()
}

// feed hands the script message with the given offset to the most recently
// spawned fetcher (the current partition owner).
func (f *fakeFetcherFactory) feed(offset int64) {
	var msg consumer.Message
	for _, m := range f.script {
		if m.Offset == offset {
			msg = m
			break
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	var ff *fakeFetcher
	for {
		f.mu.Lock()
		if len(f.fetchers) >= f.expected {
			ff = f.fetchers[len(f.fetchers)-1]
		}
		f.mu.Unlock()
		if ff != nil {
			break
		}
		if time.Now().After(deadline) {
			panic("no fetcher registered to feed")
		}
		time.Sleep(time.Millisecond)
	}
	ff.feed <- msg
}

type fakeClaimer struct {
	released chan struct{}
}

func (f *fakeClaimer) ClaimPartition(*actor.Descriptor, string, int32, <-chan none.T) func() {
	return func() { close(f.released) }
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

type DlqSuite struct {
	ns *actor.Descriptor
}

var _ = Suite(&DlqSuite{})

func (s *DlqSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *DlqSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
	check4RetryInterval = 10 * time.Millisecond
}

const (
	dlqTestGroup = "g_dlq"
	dlqTestTopic = "src_topic"
	dlqTarget    = "dlq_topic"
)

type dlqHarness struct {
	ns     *actor.Descriptor
	cfg    *config.Proxy
	omf    *fakeOffsetMgrFactory
	fetchF *fakeFetcherFactory
	dlq    *fakeDlqProducer
}

func newDlqHarness(ns *actor.Descriptor, maxRetries int) *dlqHarness {
	cfg := &config.Proxy{}
	cfg.Consumer.MaxRetries = maxRetries
	cfg.Consumer.AckTimeout = 200 * time.Millisecond
	cfg.Consumer.RetryBackoff = 120 * time.Millisecond
	cfg.Consumer.MaxPendingMessages = 100
	cfg.Consumer.DeadLetterTopic = dlqTarget
	return &dlqHarness{
		ns:     ns,
		cfg:    cfg,
		omf:    &fakeOffsetMgrFactory{current: offsetmgr.Offset{Val: 100}},
		fetchF: &fakeFetcherFactory{script: defaultScript()},
		dlq:    newFakeDlqProducer(),
	}
}

func (h *dlqHarness) spawn() (*T, *fakeClaimer) {
	return h.spawnWith(h.dlq)
}

func (h *dlqHarness) spawnWith(producer deadletter.Producer) (*T, *fakeClaimer) {
	claim := &fakeClaimer{released: make(chan struct{})}
	h.fetchF.expectSpawn()
	pc := Spawn(h.ns, dlqTestGroup, dlqTestTopic, 0, h.cfg, claim,
		h.fetchF, h.omf, "prod", producer)
	return pc, claim
}

func (h *dlqHarness) deliver(offset int64) {
	h.fetchF.feed(offset)
}

func defaultScript() []consumer.Message {
	msgs := make([]consumer.Message, 0, 5)
	for offset := int64(100); offset < 105; offset++ {
		msgs = append(msgs, sourceMessage(offset, string([]byte{byte('A' + offset - 100)})))
	}
	return msgs
}

func sourceMessage(offset int64, value string) consumer.Message {
	return consumer.Message{ConsumerMessage: sarama.ConsumerMessage{
		Topic:     dlqTestTopic,
		Partition: 0,
		Offset:    offset,
		Key:       []byte{0x00, 0x01, byte(offset), 0xFF},
		Value:     []byte(value),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("dup"), Value: []byte("one")},
			{Key: []byte("dup"), Value: []byte("two")},
			{Key: []byte("x-trace"), Value: []byte("binary\x00\xff")},
		},
		Timestamp: time.Date(2019, 5, 1, 10, 20, 30, 0, time.UTC),
	}}
}

func offsetIdentity(offset int64) string {
	return deadletter.StableIdentity("prod", dlqTestGroup, dlqTestTopic, 0, offset)
}

func dlqHeader(headers []sarama.RecordHeader, name string) string {
	for i := range headers {
		if string(headers[i].Key) == name {
			return string(headers[i].Value)
		}
	}
	return ""
}

func offerOffset(c *C, pc *T, offset int64) consumer.Message {
	select {
	case m := <-pc.Messages():
		c.Assert(m.Offset, Equals, offset, Commentf("want %d", offset))
		m.EventsCh <- consumer.Event{T: consumer.EvOffered, Offset: offset}
		return m
	case <-time.After(2 * time.Second):
		c.Fatalf("message %d was never offered", offset)
		return consumer.Message{}
	}
}

func ackOffset(c *C, m consumer.Message) {
	m.EventsCh <- consumer.Ack(m.Offset)
}

func waitFor(c *C, timeout time.Duration, cond func() bool, what string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Fatalf("timed out waiting for: %s", what)
}

func assertNoCallFor(c *C, p *fakeDlqProducer, d time.Duration) {
	select {
	case call := <-p.callCh:
		c.Fatalf("unexpected dead letter production for identity=%s after %v", call.identity(), d)
	case <-time.After(d):
	}
}

func waitForRelease(c *C, claim *fakeClaimer, what string) {
	select {
	case <-claim.released:
	case <-time.After(2 * time.Second):
		c.Fatal(what)
	}
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

// Normal dump: an exhausted record is copied to the quarantine topic, and the
// source offset is acknowledged only after the durable receipt; a later
// successful receipt must not confirm anything twice.
func (s *DlqSuite) TestNormalDump(c *C) {
	h := newDlqHarness(s.ns, 0) // max_retries=0
	pc, claim := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	h.deliver(101)
	m101 := offerOffset(c, pc, 101)
	h.deliver(102)
	m102 := offerOffset(c, pc, 102)
	// Other records keep being acknowledged while the dump is pending.
	ackOffset(c, m101)
	ackOffset(c, m102)
	_ = m100

	call := h.dlq.nextCall(c, 2*time.Second)
	c.Assert(call.topic, Equals, dlqTarget)
	c.Assert(call.value, DeepEquals, []byte("A"))
	c.Assert(call.key, DeepEquals, []byte{0x00, 0x01, 100, 0xFF})
	c.Assert(call.identity(), Equals, offsetIdentity(100))
	c.Assert(dlqHeader(call.headers, deadletter.HeaderSourceCluster), Equals, "prod")
	c.Assert(dlqHeader(call.headers, deadletter.HeaderSourceGroup), Equals, dlqTestGroup)
	c.Assert(dlqHeader(call.headers, deadletter.HeaderSourceTopic), Equals, dlqTestTopic)
	c.Assert(dlqHeader(call.headers, deadletter.HeaderSourcePartition), Equals, "0")
	c.Assert(dlqHeader(call.headers, deadletter.HeaderSourceOffset), Equals, "100")
	c.Assert(dlqHeader(call.headers, deadletter.HeaderRetryNo), Equals, "1")

	// Before the receipt the source offset cannot cross the gap, but the
	// sparse acks around it are submitted.
	waitFor(c, time.Second, func() bool {
		o := h.omf.committed()
		return o.Val == 100 && offsettrk.SparseAcks2Str(o) == "1-3"
	}, "sparse acks around pending dump")

	call.receiptCh <- deadletter.Receipt{Partition: 2, Offset: 555}

	// After the receipt the source range closes contiguously.
	waitFor(c, time.Second, func() bool {
		return h.omf.committed() == (offsetmgr.Offset{Val: 103})
	}, "source offset 103 committed after receipt")

	// No duplicate production and no duplicate source acknowledgement.
	produced := h.dlq.callCount()
	submitted := len(h.omf.lastOM().snapshotSubmits())
	assertNoCallFor(c, h.dlq, 250*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, produced)
	time.Sleep(50 * time.Millisecond)
	c.Assert(len(h.omf.lastOM().snapshotSubmits()), Equals, submitted)
	for _, so := range h.omf.lastOM().snapshotSubmits() {
		c.Assert(so.Val <= 103, Equals, true)
	}

	pc.Stop()
	waitForRelease(c, claim, "partition claim was not released on stop")
}

// Target unavailable: failed/unknown receipts keep the source gap, copies are
// retried with backoff (not on every retry tick) while other records are
// acknowledged normally; recovery closes the gap.
func (s *DlqSuite) TestTargetUnavailableAndBackoff(c *C) {
	h := newDlqHarness(s.ns, 0)
	pc, _ := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	h.deliver(101)
	m101 := offerOffset(c, pc, 101)
	ackOffset(c, m101)
	_ = m100

	first := h.dlq.nextCall(c, 2*time.Second)
	failedAt := time.Now()
	first.receiptCh <- deadletter.Receipt{Err: errors.New("not leader for partition")}

	// The gap is retained: the committed value does not cross offset 100.
	time.Sleep(50 * time.Millisecond)
	o := h.omf.committed()
	c.Assert(o.Val, Equals, int64(100))
	c.Assert(offsettrk.SparseAcks2Str(o), Equals, "1-2")

	// Backoff suppresses the retry for ~RetryBackoff (120ms): no retry yet
	// 90ms after the failed receipt, while retry ticks keep firing.
	assertNoCallFor(c, h.dlq, 40*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 1)

	// One retry eventually goes out under the same identity, no earlier than
	// the configured backoff.
	second := h.dlq.nextCall(c, time.Second)
	c.Assert(time.Since(failedAt) >= 100*time.Millisecond, Equals, true,
		Commentf("copy retried before the backoff expired"))
	c.Assert(second.identity(), Equals, first.identity())
	second.receiptCh <- deadletter.Receipt{Partition: 0, Offset: 1}

	waitFor(c, time.Second, func() bool {
		return h.omf.committed() == (offsetmgr.Offset{Val: 102})
	}, "gap closes after recovered target")

	// No further production for the resolved record.
	assertNoCallFor(c, h.dlq, 250*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 2)

	pc.Stop()
}

// Late client acknowledgement: a client Ack arriving while a copy is in
// flight ends the source record; the later receipt must not confirm twice or
// otherwise mutate committed state.
func (s *DlqSuite) TestDlqLateClientAck(c *C) {
	h := newDlqHarness(s.ns, 0)
	pc, _ := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	call := h.dlq.nextCall(c, 2*time.Second)

	// A late explicit acknowledgement resolves the source record.
	ackOffset(c, m100)
	waitFor(c, time.Second, func() bool {
		return h.omf.committed() == (offsetmgr.Offset{Val: 101})
	}, "client ack commits offset 101")

	submitted := len(h.omf.lastOM().snapshotSubmits())

	// The in-flight copy still finishes successfully, but its receipt is
	// ignored: the record is already terminal.
	call.receiptCh <- deadletter.Receipt{Partition: 0, Offset: 9}
	assertNoCallFor(c, h.dlq, 200*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 1)
	c.Assert(len(h.omf.lastOM().snapshotSubmits()), Equals, submitted)
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 101})

	pc.Stop()
}

// Crash between the successful copy and the source offset commit (modeled as
// a copy whose receipt is never processed): the source gap must survive, and
// the next owner copies the same identity again before advancing.
func (s *DlqSuite) TestDlqCrashBeforeSourceCommit(c *C) {
	h := newDlqHarness(s.ns, 0)
	pc1, claim1 := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc1, 100)
	_ = m100
	first := h.dlq.nextCall(c, 2*time.Second)

	// The owner dies while the copy is outstanding. The in-flight grace is
	// bounded by AckTimeout.
	stopDone := make(chan struct{})
	go func() { pc1.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		c.Fatal("partition consumer did not stop with an outstanding copy")
	}
	waitForRelease(c, claim1, "ownership not released")
	// Source gap is intact despite the (later) successful broker write.
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 100})
	first.receiptCh <- deadletter.Receipt{Partition: 0, Offset: 777}

	// A new owner takes over from the sparse-ack state and re-disposes the
	// record under the same stable identity.
	pc2, claim2 := h.spawn()
	h.deliver(100)
	m100Again := offerOffset(c, pc2, 100)
	_ = m100Again
	second := h.dlq.nextCall(c, 2*time.Second)
	c.Assert(second.identity(), Equals, first.identity())
	c.Assert(second.value, DeepEquals, []byte("A"))
	second.receiptCh <- deadletter.Receipt{Partition: 0, Offset: 778}

	waitFor(c, time.Second, func() bool {
		return h.omf.committed() == (offsetmgr.Offset{Val: 101})
	}, "re-dumped record committed by new owner")

	pc2.Stop()
	waitForRelease(c, claim2, "ownership not released by new owner")
}

// Graceful partition handover: a receipt landing during the shutdown drain
// commits before ownership is released, and the new owner has nothing to
// re-copy.
func (s *DlqSuite) TestDlqPartitionHandoverGraceful(c *C) {
	h := newDlqHarness(s.ns, 0)
	pc1, claim1 := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc1, 100)
	_ = m100
	call := h.dlq.nextCall(c, 2*time.Second)

	stopDone := make(chan struct{})
	go func() { pc1.Stop(); close(stopDone) }()
	// While the partition drains, the outstanding copy gets acknowledged.
	time.Sleep(50 * time.Millisecond)
	call.receiptCh <- deadletter.Receipt{Partition: 0, Offset: 321}

	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		c.Fatal("stop blocked")
	}
	waitForRelease(c, claim1, "ownership not released")
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 101})

	// The next owner starts at 101 and never re-copies offset 100.
	produced := h.dlq.callCount()
	pc2, claim2 := h.spawn()
	assertNoCallFor(c, h.dlq, 400*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, produced)
	pc2.Stop()
	waitForRelease(c, claim2, "ownership not released by new owner")
}

// Bounded in-flight concurrency and "one produce per record at a time":
// exhausted records beyond the budget wait, and completing one frees a slot.
func (s *DlqSuite) TestDlqBoundedConcurrency(c *C) {
	oldMax := deadletter.MaxInFlight
	deadletter.MaxInFlight = 2
	defer func() { deadletter.MaxInFlight = oldMax }()

	h := newDlqHarness(s.ns, 0)
	pc, _ := h.spawn()

	for offset := int64(100); offset < 105; offset++ {
		h.deliver(offset)
		offerOffset(c, pc, offset)
	}
	waitFor(c, 2*time.Second, func() bool { return h.dlq.callCount() == 2 },
		"two copies in flight")
	// Ticks keep firing but the other three exhausted records stay queued.
	time.Sleep(250 * time.Millisecond)
	calls := h.dlq.snapshotCalls()
	c.Assert(calls, HasLen, 2)
	identities := map[string]bool{calls[0].identity(): true, calls[1].identity(): true}
	c.Assert(identities, DeepEquals, map[string]bool{
		offsetIdentity(100): true,
		offsetIdentity(101): true,
	})

	// Completing the lowest in-flight record starts the next lowest waiting
	// record.
	h.dlq.callForOffset(100).receiptCh <- deadletter.Receipt{Partition: 0, Offset: 1}
	waitFor(c, time.Second, func() bool { return h.dlq.callCount() == 3 },
		"freed slot starts the next copy")
	c.Assert(h.dlq.callForOffset(102), NotNil)

	pc.Stop()
	// Offset 100 advanced; 101..104 gaps remain sparse/uncommitted.
	waitFor(c, time.Second, func() bool {
		return h.omf.committed().Val == int64(101)
	}, "committed advances for the acknowledged dead letter")
}

// Cycle misconfiguration: dead letter topic equal to the source topic never
// produces (that would loop) and never drops the record either; the gap is
// held.
func (s *DlqSuite) TestDlqCycleConfiguration(c *C) {
	h := newDlqHarness(s.ns, 0)
	h.cfg.Consumer.DeadLetterTopic = dlqTestTopic
	pc, claim := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	h.deliver(101)
	m101 := offerOffset(c, pc, 101)
	ackOffset(c, m101)
	_ = m100

	assertNoCallFor(c, h.dlq, 400*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 0)
	o := h.omf.committed()
	c.Assert(o.Val, Equals, int64(100))
	c.Assert(offsettrk.SparseAcks2Str(o), Equals, "1-2")

	stopDone := make(chan struct{})
	go func() { pc.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		c.Fatal("stop blocked by cycle-held gap")
	}
	waitForRelease(c, claim, "ownership not released")
	c.Assert(h.dlq.callCount(), Equals, 0)
}

// A topic configured without a usable producer must not drop exhausted
// records either: the source gap is retained (and made visible in logs)
// until the producer is available.
func (s *DlqSuite) TestDlqProducerUnavailableRetainsGaps(c *C) {
	h := newDlqHarness(s.ns, 0)
	pc, claim := h.spawnWith(nil)

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	_ = m100

	// Wait well past the ack timeout: the record exhausts its retries but,
	// without a producer, it cannot be copied nor acknowledged.
	time.Sleep(350 * time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 0)
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 100})

	stopDone := make(chan struct{})
	go func() { pc.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		c.Fatal("stop blocked by retained gap")
	}
	waitForRelease(c, claim, "ownership not released")
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 100})
}

// Disabled dead lettering preserves the legacy behavior: finite retries drop
// the record and acknowledge its offset without any production.
func (s *DlqSuite) TestDlqDisabledDropsOnExhaustion(c *C) {
	h := newDlqHarness(s.ns, 0)
	h.cfg.Consumer.DeadLetterTopic = ""
	pc, claim := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	_ = m100
	waitFor(c, 2*time.Second, func() bool {
		return h.omf.committed() == (offsetmgr.Offset{Val: 101})
	}, "legacy drop commits offset 101")
	assertNoCallFor(c, h.dlq, 200*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 0)
	pc.Stop()
	waitForRelease(c, claim, "ownership not released")
}

// max_retries=-1 must keep retrying forever: nothing is dead lettered and
// nothing is acknowledged/dropped even with a topic configured.
func (s *DlqSuite) TestDlqInfiniteRetriesCompat(c *C) {
	h := newDlqHarness(s.ns, -1)
	pc, claim := h.spawn()

	h.deliver(100)
	m100 := offerOffset(c, pc, 100)
	// The same record is offered over and over again as retries expire.
	for i := 0; i < 3; i++ {
		retry := offerOffset(c, pc, 100)
		c.Assert(retry.Offset, Equals, int64(100))
	}
	_ = m100
	assertNoCallFor(c, h.dlq, 200*time.Millisecond)
	c.Assert(h.dlq.callCount(), Equals, 0)
	c.Assert(h.omf.committed(), Equals, offsetmgr.Offset{Val: 100})
	pc.Stop()
	waitForRelease(c, claim, "ownership not released")
}
