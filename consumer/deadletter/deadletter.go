// Package deadletter implements copying of retry-exhausted source records
// into a configured quarantine ("dead letter") topic of the same Kafka
// cluster.
//
// Reliability model. A source record is acknowledged by its partition
// consumer only after the broker durably acknowledges its dead letter copy.
// The copy and the source offset commit are two independent operations,
// therefore exactly-once is not promised: if the process dies after the copy
// was acknowledged but before the source offset was committed, the record is
// copied again. Every copy carries a stable identity derived from the source
// location (cluster/group/topic/partition/offset) so that downstream
// consumers can correlate duplicates. A failed production, or one whose
// result is unknown, keeps the source gap open and is retried with backoff.
package deadletter

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/none"
)

const (
	// HeaderPrefix is the common prefix of every envelope metadata header;
	// it allows consumers to tell metadata apart from the verbatim original
	// headers of the source record.
	HeaderPrefix = "x-kafka-pixy-dlq-"

	headerPrefix = HeaderPrefix

	// HeaderVersion identifies the envelope format.
	HeaderVersion = headerPrefix + "version"
	// HeaderIdentity carries the stable identity of the source record.
	HeaderIdentity = headerPrefix + "identity"
	// HeaderSourceCluster is the name of the cluster the record came from.
	HeaderSourceCluster = headerPrefix + "source-cluster"
	// HeaderSourceGroup is the consumer group the record was consumed by.
	HeaderSourceGroup = headerPrefix + "source-group"
	// HeaderSourceTopic is the topic the record came from.
	HeaderSourceTopic = headerPrefix + "source-topic"
	// HeaderSourcePartition is the partition the record came from.
	HeaderSourcePartition = headerPrefix + "source-partition"
	// HeaderSourceOffset is the offset of the record in the source partition.
	HeaderSourceOffset = headerPrefix + "source-offset"
	// HeaderSourceTimestamp is the source record timestamp in Unix nanoseconds.
	HeaderSourceTimestamp = headerPrefix + "source-timestamp"
	// HeaderExhaustedAt is the time (Unix nanoseconds) the copy was built.
	HeaderExhaustedAt = headerPrefix + "exhausted-at"
	// HeaderRetryNo is the total number of offer attempts made before the
	// record was declared retry-exhausted.
	HeaderRetryNo = headerPrefix + "retry-no"

	envelopeVersion = "1"
	identityPrefix  = "kafka-pixy-dlq:v1:"
)

var (
	// MaxInFlight bounds the number of dead letter copies that a single
	// partition consumer may wait on a production receipt for concurrently.
	// It is a variable so that tests can tune it.
	MaxInFlight = 8
)

// Receipt reports the outcome of a dead letter production. A non-nil Err
// means either a definitive failure or an unknown result (e.g. a lost
// receipt); the caller must assume the copy may not exist and retry.
type Receipt struct {
	Partition int32
	Offset    int64
	Err       error
}

// Producer abstracts a producer channel that reports one durable receipt for
// every submitted message.
type Producer interface {
	AsyncProduce(topic string, key, value sarama.Encoder, headers []sarama.RecordHeader) <-chan Receipt
}

// Request asks to copy one retry-exhausted source record into the dead
// letter topic.
type Request struct {
	// Offset identifies the source record and is used to correlate the
	// production receipt with the source offer.
	Offset int64

	// Source location coordinates.
	Cluster   string
	Group     string
	Topic     string
	Partition int32

	// Source record payload. Slices are copied before the request is sent.
	Key     []byte
	Value   []byte
	Headers []*sarama.RecordHeader

	// Timestamp is the source record timestamp.
	Timestamp time.Time
	// RetryNo is the number of offer attempts reached by exhaustion.
	RetryNo int
}

// StableIdentity deterministically derives the dead letter identity of a
// source record from its location. The same source record re-copied after a
// crash between the copy and the source offset commit gets the same identity,
// which allows downstream consumers to de-duplicate copies.
func StableIdentity(cluster, group, topic string, partition int32, offset int64) string {
	return fmt.Sprintf("%s%s/%s/%s/%d/%d", identityPrefix,
		cluster, group, topic, partition, offset)
}

// T is a bounded-concurrency dispatcher that ships dead letter copies to the
// target topic and reports production receipts back to the owning partition
// consumer. It is driven by a single owner goroutine, therefore its methods
// do not need internal synchronization beyond the delivery goroutines.
type T struct {
	actDesc     *actor.Descriptor
	targetTopic string
	prod        Producer
	resultsCh   chan<- consumer.Event
	requestsCh  chan Request
	stopCh      chan none.T
	wg          sync.WaitGroup
}

// Spawn creates a dead letter dispatcher for one source partition and starts
// its goroutine. Receipts are posted to resultsCh as EvDeadLettered events.
// maxInFlight bounds the number of copies waiting for a receipt concurrently;
// the owner undertakes never to have more than that many requests outstanding.
func Spawn(parentActDesc *actor.Descriptor, targetTopic string, prod Producer,
	resultsCh chan<- consumer.Event, maxInFlight int,
) *T {
	d := &T{
		actDesc:     parentActDesc.NewChild("dlq"),
		targetTopic: targetTopic,
		prod:        prod,
		resultsCh:   resultsCh,
		// The buffer allows the owner to enqueue up to maxInFlight requests
		// without blocking its event loop.
		requestsCh: make(chan Request, maxInFlight),
		stopCh:     make(chan none.T),
	}
	actor.Spawn(d.actDesc, &d.wg, d.run)
	return d
}

// Dump enqueues a copy request. The caller is responsible for keeping the
// number of outstanding requests (those without a receipt yet) within
// MaxInFlight so that this call never blocks the partition event loop.
func (d *T) Dump(rq Request) {
	d.requestsCh <- rq
}

// TargetTopic returns the configured dead letter topic name.
func (d *T) TargetTopic() string {
	return d.targetTopic
}

// Stop stops accepting new requests and releases delivery goroutines waiting
// on receipts. It does not wait for in-flight broker operations: an
// abandoned copy either lands in the topic (in which case the source gap is
// still open and the record gets copied again under the same identity) or it
// does not (the gap is retained as well).
func (d *T) Stop() {
	close(d.stopCh)
	close(d.requestsCh)
	d.wg.Wait()
}

func (d *T) run() {
	for rq := range d.requestsCh {
		rq := rq
		// Concurrency is bounded by the owner, which never enqueues more than
		// MaxInFlight outstanding requests.
		go d.deliver(rq)
	}
}

func (d *T) deliver(rq Request) {
	prodMsg := buildProducerMessage(rq, time.Now())
	prodMsg.Topic = d.targetTopic
	receiptCh := d.prod.AsyncProduce(d.targetTopic, prodMsg.Key, prodMsg.Value, prodMsg.Headers)

	var receipt Receipt
	select {
	case receipt = <-receiptCh:
	case <-d.stopCh:
		// The partition is gone and nobody will read the result; the source
		// gap was not acknowledged, so the record will be disposed of again
		// by the next owner. The per-request receipt channel is required to be
		// buffered and deliver exactly once, so this goroutine does not leak.
		return
	}

	event := consumer.Event{T: consumer.EvDeadLettered, Offset: rq.Offset, Err: receipt.Err}
	select {
	case d.resultsCh <- event:
	case <-d.stopCh:
	}
}

// buildProducerMessage constructs the Kafka message written to the dead
// letter topic. The original key/value/headers are copied verbatim (nil key
// or value stays a null field, duplicate headers are preserved) and metadata
// headers are appended. The message timestamp is assigned by the broker, the
// source timestamp is kept in a header.
func buildProducerMessage(rq Request, now time.Time) *sarama.ProducerMessage {
	identity := StableIdentity(rq.Cluster, rq.Group, rq.Topic, rq.Partition, rq.Offset)

	headers := make([]sarama.RecordHeader, 0, len(rq.Headers)+10)
	for _, h := range rq.Headers {
		if h == nil {
			continue
		}
		// Headers are copied verbatim, including duplicates of the same key;
		// they remain a list, never a map.
		headers = append(headers, sarama.RecordHeader{
			Key:   copyBytes(h.Key),
			Value: copyBytes(h.Value),
		})
	}
	headers = appendHeader(headers, HeaderVersion, envelopeVersion)
	headers = appendHeader(headers, HeaderIdentity, identity)
	headers = appendHeader(headers, HeaderSourceCluster, rq.Cluster)
	headers = appendHeader(headers, HeaderSourceGroup, rq.Group)
	headers = appendHeader(headers, HeaderSourceTopic, rq.Topic)
	headers = appendHeader(headers, HeaderSourcePartition, strconv.Itoa(int(rq.Partition)))
	headers = appendHeader(headers, HeaderSourceOffset, strconv.FormatInt(rq.Offset, 10))
	headers = appendHeader(headers, HeaderSourceTimestamp, strconv.FormatInt(rq.Timestamp.UnixNano(), 10))
	headers = appendHeader(headers, HeaderExhaustedAt, strconv.FormatInt(now.UnixNano(), 10))
	headers = appendHeader(headers, HeaderRetryNo, strconv.Itoa(rq.RetryNo))

	prodMsg := &sarama.ProducerMessage{
		Headers: headers,
	}
	if rq.Key != nil {
		prodMsg.Key = sarama.ByteEncoder(copyBytes(rq.Key))
	}
	if rq.Value != nil {
		prodMsg.Value = sarama.ByteEncoder(copyBytes(rq.Value))
	}
	return prodMsg
}

func appendHeader(headers []sarama.RecordHeader, key, value string) []sarama.RecordHeader {
	return append(headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
