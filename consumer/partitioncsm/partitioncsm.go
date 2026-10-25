package partitioncsm

import (
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/deadletter"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
)

var (
	// TESTING ONLY!: If this channel is not `nil` then partition consumers
	// will use it to notify when they fetch the very first message.
	FirstMessageFetchedCh chan *T

	initialOffsetCh chan offsetmgr.Offset

	// Sets an interval for periodical checks for messages to retry.
	check4RetryInterval = time.Second
)

// partitionClaimer is the subset of the subscriber API used by a partition
// consumer to claim exclusive ownership of a partition in ZooKeeper.
type partitionClaimer interface {
	ClaimPartition(claimerActDesc *actor.Descriptor, topic string, partition int32, cancelCh <-chan none.T) func()
}

// T ensures exclusive consumption of messages from a topic
// partition within a particular group. It ensures that a partition is consumed
// exclusively by first claiming the partition in ZooKeeper. When a fetched
// message is pulled from the `messages()` channel, it is considered to be
// consumed and its offset is committed.
type T struct {
	actDesc     *actor.Descriptor
	cfg         *config.Proxy
	cluster     string
	group       string
	topic       string
	partition   int32
	groupMember partitionClaimer
	msgFetcherF msgfetcher.Factory
	offsetMgrF  offsetmgr.Factory
	dlqProducer deadletter.Producer
	messagesCh  chan consumer.Message
	eventsCh    chan consumer.Event
	stopCh      chan none.T
	wg          sync.WaitGroup

	offsetMgr       offsetmgr.T
	committedOffset offsetmgr.Offset
	submittedOffset offsetmgr.Offset
	offsetsOk       bool
	offsetTrk       *offsettrk.T
	offerCount      int32

	// Dead letter disposal state. dlq is non-nil only when the dead letter
	// topic is configured and the configuration is valid for this partition.
	// dlqStates is keyed by source offset and exists whenever dead lettering
	// is configured, including the misconfigured (source==target topic) case,
	// where exhausted offers are retained without any copying.
	dlq            *deadletter.T
	dlqStates      map[int64]*dlqState
	dlqInFlight    int
	dlqMaxInFlight int
	dlqCycle       bool
	dlqCycleWarned bool
	dlqNoProdWarn  bool

	// For tests only!
	firstMsgFetched bool
}

// dlqPhase tracks the disposal state of one retry-exhausted source record.
type dlqPhase int

const (
	// dlqInflight means a copy request is outstanding, waiting for a receipt.
	dlqInflight dlqPhase = iota
	// dlqBackoff means the last attempt failed or its result is unknown; the
	// record waits for the retry backoff to expire before another attempt.
	dlqBackoff
)

type dlqState struct {
	phase    dlqPhase
	attempts int
	retryAt  time.Time
}

// Spawn creates a partition consumer instance and starts its goroutines.
// `cluster` identifies the Kafka cluster in dead letter envelopes, and
// `dlqProducer` ships dead letter copies; a nil producer together with an
// empty `cfg.Consumer.DeadLetterTopic` keeps the legacy drop-on-exhaustion
// behavior.
func Spawn(parentActDesc *actor.Descriptor, group, topic string, partition int32, cfg *config.Proxy,
	groupMember partitionClaimer, msgFetcherF msgfetcher.Factory, offsetMgrF offsetmgr.Factory,
	cluster string, dlqProducer deadletter.Producer,
) *T {
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s.p%d", topic, partition))
	actDesc.AddLogField("kafka.group", group)
	actDesc.AddLogField("kafka.topic", topic)
	actDesc.AddLogField("kafka.partition", partition)
	pc := &T{
		actDesc:     actDesc,
		cfg:         cfg,
		cluster:     cluster,
		group:       group,
		topic:       topic,
		partition:   partition,
		groupMember: groupMember,
		msgFetcherF: msgFetcherF,
		offsetMgrF:  offsetMgrF,
		dlqProducer: dlqProducer,
		messagesCh:  make(chan consumer.Message, 1),
		eventsCh:    make(chan consumer.Event, 1),
		stopCh:      make(chan none.T),
	}
	actor.Spawn(pc.actDesc, &pc.wg, pc.run)
	return pc
}

// Topic returns the partition ID this partition consumer is responsible for.
func (pc *T) Partition() int32 {
	return pc.partition
}

// implements `multiplexer.In`
func (pc *T) Messages() <-chan consumer.Message {
	return pc.messagesCh
}

// implements `multiplexer.In`
func (pc *T) IsSafe2Stop() bool {
	return atomic.LoadInt32(&pc.offerCount) == 0
}

// implements `multiplexer.In`
func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}

func (pc *T) run() {
	defer close(pc.messagesCh)
	defer pc.groupMember.ClaimPartition(pc.actDesc, pc.topic, pc.partition, pc.stopCh)()

	var err error
	if pc.offsetMgr, err = pc.offsetMgrF.Spawn(pc.actDesc, pc.group, pc.topic, pc.partition); err != nil {
		panic(errors.Wrapf(err, "<%s> must never happen", pc.actDesc))
	}
	defer pc.stopOffsetMgr()
	// The dumper is created after the offset manager and stopped before it,
	// so receipts processed during the shutdown drain can still submit
	// offsets.
	defer func() {
		if pc.dlq != nil {
			pc.dlq.Stop()
		}
	}()

	// Wait for the initial offset to be retrieved or a stop signal.
	select {
	case pc.committedOffset = <-pc.offsetMgr.CommittedOffsets():
	case <-pc.stopCh:
		return
	}
	pc.actDesc.Log().Infof("Initial offset: %s", offsetRepr(pc.committedOffset))
	pc.offsetTrk = offsettrk.New(pc.actDesc, pc.committedOffset, pc.cfg.Consumer.AckTimeout)
	pc.submittedOffset = pc.committedOffset
	pc.offsetsOk = true
	pc.initDeadLetter()
	pc.notifyTestInitialized(pc.committedOffset)

	// Run a fetch loop until the partition consumer is signalled to stop.
	for pc.runFetchLoop() {
	}

	// Wait for clients to acknowledge pending offers and for in-flight dead
	// letter copies to get receipts, bounded by Consumer.AckTimeout.
	stopDeadline := time.Now().Add(pc.cfg.Consumer.AckTimeout)
	for {
		wait4Ack := pc.offsetTrk.ShouldWait4Ack()
		wait4Dlq := time.Duration(0)
		if pc.dlq != nil && pc.dlqInFlight > 0 {
			wait4Dlq = stopDeadline.Sub(time.Now())
		}
		timeout := wait4Ack
		if wait4Dlq > 0 && (timeout <= 0 || wait4Dlq < timeout) {
			timeout = wait4Dlq
		}
		if timeout <= 0 {
			break
		}
		select {
		case event := <-pc.eventsCh:
			switch event.T {
			case consumer.EvAcked:
				var offerCount int
				pc.submittedOffset, offerCount = pc.offsetTrk.OnAcked(event.Offset)
				pc.dlqForget(event.Offset)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
			case consumer.EvDeadLettered:
				if submitted, offerCount, confirmed := pc.onDlqResult(event); confirmed {
					pc.submittedOffset = submitted
					atomic.StoreInt32(&pc.offerCount, int32(offerCount))
					pc.offsetMgr.SubmitOffset(pc.submittedOffset)
				}
			}
		case <-time.After(timeout):
			continue
		}
	}
}

// initDeadLetter prepares the retry-exhausted message disposal strategy.
// When the dead letter topic is not configured (or no producer is available)
// the legacy drop-on-exhaustion behavior is preserved. A dead letter topic
// equal to the consumed source topic is a cycle: copies are never attempted
// (that would feed the same source stream) and exhausted records are retained
// as gaps until the misconfiguration is fixed.
func (pc *T) initDeadLetter() {
	target := pc.cfg.Consumer.DeadLetterTopic
	if target == "" {
		return
	}
	pc.dlqStates = make(map[int64]*dlqState)
	pc.dlqMaxInFlight = deadletter.MaxInFlight
	if pc.cfg.Consumer.MaxRetries < 0 {
		pc.actDesc.Log().Warnf("Dead letter topic %q configured but consumer.max_retries=-1, "+
			"messages are retried indefinitely and will never be dead lettered", target)
	}
	if pc.dlqProducer == nil {
		pc.actDesc.Log().Errorf("Dead letter topic %q configured but no producer is available, "+
			"exhausted messages will be retained as gaps", target)
		return
	}
	if target == pc.topic {
		pc.dlqCycle = true
		pc.actDesc.Log().Errorf("Refusing to dead letter: target topic %q equals source topic, "+
			"exhausted messages will be retained as gaps until configuration is fixed", target)
		return
	}
	pc.dlq = deadletter.Spawn(pc.actDesc, target, pc.dlqProducer, pc.eventsCh, pc.dlqMaxInFlight)
	pc.actDesc.Log().Infof("Dead lettering enabled: target=%s", target)
}

func (pc *T) runFetchLoop() bool {
	// Initialize a message fetcher to read from the initial offset.
	mf, realOffsetVal, err := pc.msgFetcherF.Spawn(pc.actDesc, pc.topic, pc.partition, pc.submittedOffset.Val)
	if err != nil {
		pc.actDesc.Log().WithError(err).Error("Failed to spawn fetcher")
		return true
	}
	defer mf.Stop()

	var offerCount int
	pc.submittedOffset, offerCount = pc.offsetTrk.Adjust(realOffsetVal)
	atomic.StoreInt32(&pc.offerCount, int32(offerCount))
	// Offers rebased away are gone from this partition consumer, drop their
	// pending dump state as well.
	pc.dlqPurgeBefore(realOffsetVal)

	// If the real offset is different from the committed one then submit it
	// and report in the logs.
	if pc.submittedOffset != pc.committedOffset {
		pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		pc.actDesc.Log().Errorf("Adjusted offset: new=%s, old=%s",
			offsetRepr(pc.submittedOffset), offsetRepr(pc.committedOffset))
	}
	var (
		nilOrMsgInCh  = mf.Messages()
		nilOrMsgOutCh chan consumer.Message
		retryTicker   = time.NewTicker(check4RetryInterval)
		msg           consumer.Message
		msgOk         bool
	)
	defer retryTicker.Stop()
	for {
		select {
		case msg, msgOk = <-nilOrMsgInCh:
			// If the fetcher terminated due to failure, then quit the fetch
			// loop signaling that it needs to be reinitialized.
			if !msgOk {
				return true
			}
			// If a fetched message has already been acked, then skip it.
			if ok, _ := pc.offsetTrk.IsAcked(msg.Offset); ok {
				msgOk = false
				continue
			}
			msg.EventsCh = pc.eventsCh
			pc.notifyTestFetched()
			nilOrMsgOutCh = pc.messagesCh
			// Stop fetching messages until this one is offered to a client.
			nilOrMsgInCh = nil

		case <-retryTicker.C:
			if msgOk {
				continue
			}
			if msg, msgOk = pc.nextRetry(); msgOk {
				nilOrMsgInCh = nil
				nilOrMsgOutCh = pc.messagesCh
			}
			// Retry dead letter copies whose backoff expired (dumps are also
			// scheduled from nextRetry; this covers stretches without
			// expired retries).
			pc.scheduleDumps()
		case nilOrMsgOutCh <- msg:
			nilOrMsgOutCh = nil

		case event := <-pc.eventsCh:
			switch event.T {
			case consumer.EvOffered:
				if !msgOk || event.Offset != msg.Offset {
					pc.actDesc.Log().Errorf("Invalid offer offset %d, want=%d", event.Offset, msg.Offset)
					continue
				}
				offerCount = pc.offsetTrk.OnOffered(msg)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				if msg, msgOk = pc.nextRetry(); msgOk {
					nilOrMsgOutCh = pc.messagesCh
					continue
				}
				if offerCount > pc.cfg.Consumer.MaxPendingMessages {
					pc.actDesc.Log().Warnf("Offer count above HWM: %d", offerCount)
					nilOrMsgInCh = nil
					continue
				}
				nilOrMsgInCh = mf.Messages()

			case consumer.EvAcked:
				pc.submittedOffset, offerCount = pc.offsetTrk.OnAcked(event.Offset)
				// A late client acknowledgement ends the record even if a dead
				// letter copy is still in flight; the late receipt will be
				// ignored by onDlqResult.
				pc.dlqForget(event.Offset)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
				if !msgOk && offerCount <= pc.cfg.Consumer.MaxPendingMessages {
					nilOrMsgInCh = mf.Messages()
				}

			case consumer.EvDeadLettered:
				submitted, newOfferCount, confirmed := pc.onDlqResult(event)
				if !confirmed {
					continue
				}
				// A durable dead letter receipt is the only moment the source
				// record is allowed to join the acknowledged range.
				pc.submittedOffset = submitted
				offerCount = newOfferCount
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
				if !msgOk && offerCount <= pc.cfg.Consumer.MaxPendingMessages {
					nilOrMsgInCh = mf.Messages()
				}
			}
		case pc.committedOffset = <-pc.offsetMgr.CommittedOffsets():
		case <-pc.stopCh:
			return false
		}
	}
}

// nextRetry checks with the offset tracker if there is a message ready to be
// retried. Messages that exhausted their configured retry attempts are
// handled per the configured terminal strategy: they are either copied to the
// dead letter topic (joining the acknowledged range only after a durable
// production receipt), marked exhausted-and-retained when dead lettering is
// misconfigured, or acknowledged and dropped when dead lettering is disabled
// (the legacy behavior). It continues until either a message with retries
// left is returned or there are no more messages to be retried.
func (pc *T) nextRetry() (consumer.Message, bool) {
	msg, retryNo, ok := pc.offsetTrk.NextRetry()
	for ok && pc.cfg.Consumer.MaxRetries >= 0 && retryNo > pc.cfg.Consumer.MaxRetries {
		// Whenever a dead letter topic is configured the exhausted record
		// must be retained until a durable copy receipt (or a client ack)
		// ends it, even when the producer is unavailable or the target
		// topic is misconfigured: dlqStates exists exactly in those cases.
		if pc.dlqStates != nil {
			pc.actDesc.Log().Warnf("Retries exhausted, dead lettering: retryNo=%d, offset=%d, key=%s",
				retryNo, msg.Offset, string(msg.Key))
			pc.offsetTrk.MarkExhausted(msg.Offset)
		} else {
			pc.actDesc.Log().Errorf("Too many retries: retryNo=%d, offset=%d, key=%s, msg=%s",
				retryNo, msg.Offset, string(msg.Key), base64.StdEncoding.EncodeToString(msg.Value))
			pc.submittedOffset, _ = pc.offsetTrk.OnAcked(msg.Offset)
			pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		}
		msg, retryNo, ok = pc.offsetTrk.NextRetry()
	}
	if ok {
		pc.actDesc.Log().Warnf("Retrying: retryNo=%d, offset=%d, key=%s",
			retryNo, msg.Offset, string(msg.Key))
	}
	pc.scheduleDumps()
	return msg, ok
}

// scheduleDumps starts due dead letter copies while the per-partition
// in-flight budget allows. A record is copied at most once concurrently;
// failed attempts are retried only after Consumer.RetryBackoff expires, so
// repeated retry ticks never result in duplicate produce requests for the
// same offset. In the cycle-misconfigured case it only logs.
func (pc *T) scheduleDumps() {
	if pc.dlqCycle {
		if !pc.dlqCycleWarned && len(pc.offsetTrk.ExhaustedOffers()) > 0 {
			pc.dlqCycleWarned = true
			pc.actDesc.Log().Errorf("Dead letter topic equals source topic %q, %d exhausted "+
				"message(s) are held as unacknowledged gaps",
				pc.topic, len(pc.offsetTrk.ExhaustedOffers()))
		}
		return
	}
	if pc.dlq == nil {
		// A topic is configured but the producer is unavailable: copies
		// cannot be shipped, so exhausted records remain gaps. Make the
		// stall visible instead of silently holding messages forever.
		if !pc.dlqNoProdWarn && pc.dlqStates != nil && len(pc.offsetTrk.ExhaustedOffers()) > 0 {
			pc.dlqNoProdWarn = true
			pc.actDesc.Log().Errorf("Dead letter copies cannot be produced, %d exhausted "+
				"message(s) are held as unacknowledged gaps",
				len(pc.offsetTrk.ExhaustedOffers()))
		}
		return
	}
	now := time.Now()
	for _, exhausted := range pc.offsetTrk.ExhaustedOffers() {
		if pc.dlqInFlight >= pc.dlqMaxInFlight {
			return
		}
		offset := exhausted.Msg.Offset
		state := pc.dlqStates[offset]
		if state != nil {
			if state.phase == dlqInflight || now.Before(state.retryAt) {
				continue
			}
		} else {
			state = &dlqState{}
		}
		state.phase = dlqInflight
		state.attempts++
		state.retryAt = time.Time{}
		pc.dlqStates[offset] = state
		pc.dlqInFlight++
		pc.dlq.Dump(deadletter.Request{
			Offset:    offset,
			Cluster:   pc.cluster,
			Group:     pc.group,
			Topic:     pc.topic,
			Partition: pc.partition,
			Key:       exhausted.Msg.Key,
			Value:     exhausted.Msg.Value,
			Headers:   exhausted.Msg.Headers,
			Timestamp: exhausted.Msg.Timestamp,
			RetryNo:   exhausted.RetryNo,
		})
	}
}

// onDlqResult applies a dead letter production receipt. It returns
// confirmed=true only when a durable receipt proved the copy stored, in which
// case the returned values are meant to become the new submitted offset and
// offer count. A non-nil receipt error (or an unknown result) keeps the
// source gap open and schedules a bounded backoff retry. Results arriving
// after the record was ended by a client acknowledgement (or dropped on
// rebalance/adjust) are ignored: a record is never acknowledged twice and a
// stopped partition is never revived.
func (pc *T) onDlqResult(event consumer.Event) (offsetmgr.Offset, int, bool) {
	state, tracked := pc.dlqStates[event.Offset]
	if tracked {
		delete(pc.dlqStates, event.Offset)
		if state.phase == dlqInflight {
			pc.dlqInFlight--
		}
	}
	if acked, _ := pc.offsetTrk.IsAcked(event.Offset); acked {
		pc.actDesc.Log().Warnf("Ignoring dead letter receipt for ended record: offset=%d, err=%v",
			event.Offset, event.Err)
		pc.scheduleDumps()
		return offsetmgr.Offset{}, 0, false
	}
	if event.Err != nil {
		// Failure or unknown result: the copy may not exist, therefore the
		// source gap must stay open and the copy has to be retried.
		attempts := 1
		if tracked {
			attempts = state.attempts + 1
		}
		pc.dlqStates[event.Offset] = &dlqState{
			phase:    dlqBackoff,
			attempts: attempts,
			retryAt:  time.Now().Add(pc.cfg.Consumer.RetryBackoff),
		}
		pc.actDesc.Log().WithError(event.Err).Errorf(
			"Dead letter copy failed, source gap retained for retry: offset=%d, attempts=%d",
			event.Offset, attempts)
		pc.scheduleDumps()
		return offsetmgr.Offset{}, 0, false
	}
	submitted, offerCount := pc.offsetTrk.OnAcked(event.Offset)
	pc.actDesc.Log().Infof("Dead lettered after retries: offset=%d, submitted=%s",
		event.Offset, offsetRepr(submitted))
	pc.scheduleDumps()
	return submitted, offerCount, true
}

// dlqForget drops disposal state for a record that reached its terminal state
// without needing a dead letter receipt (a client acknowledgement or an
// offset tracker removal). An in-flight copy, if any, still completes
// eventually, but its receipt will be ignored.
func (pc *T) dlqForget(offset int64) {
	if pc.dlqStates == nil {
		return
	}
	if state, ok := pc.dlqStates[offset]; ok {
		if state.phase == dlqInflight {
			pc.dlqInFlight--
		}
		delete(pc.dlqStates, offset)
	}
}

// dlqPurgeBefore drops disposal state for source offsets rebased away by a
// fetcher adjust, keeping the in-flight counter consistent.
func (pc *T) dlqPurgeBefore(offset int64) {
	for staleOffset, state := range pc.dlqStates {
		if staleOffset < offset {
			if state.phase == dlqInflight {
				pc.dlqInFlight--
			}
			delete(pc.dlqStates, staleOffset)
		}
	}
}

func (pc *T) stopOffsetMgr() {
	pc.offsetMgr.Stop()
	if !pc.offsetsOk {
		return
	}
	// Drain committed offsets.
	for pc.committedOffset = range pc.offsetMgr.CommittedOffsets() {
	}
	if pc.committedOffset != pc.submittedOffset {
		pc.actDesc.Log().Errorf("Failed to commit offset: %s", offsetRepr(pc.submittedOffset))
	}
	pc.actDesc.Log().Infof("Last committed offset: %s", offsetRepr(pc.committedOffset))
}

// notifyTestInitialized sends initial offset to initialOffsetCh channel.
func (pc *T) notifyTestInitialized(initialOffset offsetmgr.Offset) {
	if initialOffsetCh != nil {
		initialOffsetCh <- initialOffset
	}
}

// notifyTestFetched sends a signal to FirstMessageFetchedCh channel the
// first time it is called.
func (pc *T) notifyTestFetched() {
	if !pc.firstMsgFetched && FirstMessageFetchedCh != nil {
		pc.firstMsgFetched = true
		FirstMessageFetchedCh <- pc
	}
}

func offsetRepr(offset offsetmgr.Offset) string {
	return fmt.Sprintf("%d(%s)", offset.Val, offsettrk.SparseAcks2Str(offset))
}
