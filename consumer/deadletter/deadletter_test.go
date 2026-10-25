package deadletter

import (
	"errors"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type DeadLetterSuite struct {
	ns *actor.Descriptor
}

var _ = Suite(&DeadLetterSuite{})

func (s *DeadLetterSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *DeadLetterSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

// The identity is a pure function of the source location: the same source
// record re-copied after a crash gets the very same identity.
func (s *DeadLetterSuite) TestStableIdentity(c *C) {
	id1 := StableIdentity("prod", "g1", "t1", 3, 42)
	id2 := StableIdentity("prod", "g1", "t1", 3, 42)
	c.Assert(id2, Equals, id1)
	c.Assert(id1, Matches, "kafka-pixy-dlq:v1:prod/g1/t1/3/42")
	// Every coordinate participates in the identity.
	c.Assert(StableIdentity("stage", "g1", "t1", 3, 42), Not(Equals), id1)
	c.Assert(StableIdentity("prod", "g2", "t1", 3, 42), Not(Equals), id1)
	c.Assert(StableIdentity("prod", "g1", "t2", 3, 42), Not(Equals), id1)
	c.Assert(StableIdentity("prod", "g1", "t1", 4, 42), Not(Equals), id1)
	c.Assert(StableIdentity("prod", "g1", "t1", 3, 43), Not(Equals), id1)
}

// The envelope preserves binary key/value byte-for-byte, keeps duplicate
// headers verbatim and appends the metadata headers.
func (s *DeadLetterSuite) TestBuildProducerMessageFidelity(c *C) {
	sourceTs := time.Date(2019, 5, 1, 10, 20, 30, 0, time.UTC)
	key := []byte{0x00, 0x01, 0x02, 0xFF, 'k'}
	value := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x42}
	headers := []*sarama.RecordHeader{
		{Key: []byte("dup"), Value: []byte("first")},
		{Key: []byte("dup"), Value: []byte("second")},
		{Key: []byte("bin"), Value: []byte{0x00, 0xFF}},
	}
	rq := Request{
		Offset:    42,
		Cluster:   "prod",
		Group:     "g1",
		Topic:     "t1",
		Partition: 3,
		Key:       key,
		Value:     value,
		Headers:   headers,
		Timestamp: sourceTs,
		RetryNo:   4,
	}

	now := time.Date(2019, 5, 1, 11, 0, 0, 0, time.UTC)
	msg := buildProducerMessage(rq, now)

	// Binary payload is copied verbatim.
	gotKey, err := msg.Key.Encode()
	c.Assert(err, IsNil)
	c.Assert(gotKey, DeepEquals, key)
	gotValue, err := msg.Value.Encode()
	c.Assert(err, IsNil)
	c.Assert(gotValue, DeepEquals, value)

	// Three original headers (including the duplicate key) followed by the
	// metadata headers.
	headerMap := func() map[string][]string {
		m := make(map[string][]string)
		for _, h := range msg.Headers {
			m[string(h.Key)] = append(m[string(h.Key)], string(h.Value))
		}
		return m
	}()
	c.Assert(headerMap["dup"], DeepEquals, []string{"first", "second"})
	c.Assert(headerMap[HeaderVersion], DeepEquals, []string{"1"})
	c.Assert(headerMap[HeaderIdentity], DeepEquals,
		[]string{"kafka-pixy-dlq:v1:prod/g1/t1/3/42"})
	c.Assert(headerMap[HeaderSourceCluster], DeepEquals, []string{"prod"})
	c.Assert(headerMap[HeaderSourceGroup], DeepEquals, []string{"g1"})
	c.Assert(headerMap[HeaderSourceTopic], DeepEquals, []string{"t1"})
	c.Assert(headerMap[HeaderSourcePartition], DeepEquals, []string{"3"})
	c.Assert(headerMap[HeaderSourceOffset], DeepEquals, []string{"42"})
	c.Assert(headerMap[HeaderSourceTimestamp], DeepEquals,
		[]string{"1556706030000000000"})
	c.Assert(headerMap[HeaderExhaustedAt], DeepEquals,
		[]string{"1556708400000000000"})
	c.Assert(headerMap[HeaderRetryNo], DeepEquals, []string{"4"})
	// Original binary header is preserved too.
	var binHeader *sarama.RecordHeader
	for i := range msg.Headers {
		if string(msg.Headers[i].Key) == "bin" {
			binHeader = &msg.Headers[i]
		}
	}
	c.Assert(binHeader, NotNil)
	c.Assert(binHeader.Value, DeepEquals, []byte{0x00, 0xFF})

	// Envelope bytes are copies: mutating the request slices must not affect
	// the built message.
	key[0] = 0x77
	value[0] = 0x77
	headers[0].Value[0] = 'X'
	gotKey, _ = msg.Key.Encode()
	gotValue, _ = msg.Value.Encode()
	c.Assert(gotKey[0], Equals, byte(0x00))
	c.Assert(gotValue[0], Equals, byte(0xDE))
	c.Assert(string(msg.Headers[0].Value), Equals, "first")
}

// A null key/value of the source record stays a null (nil encoder) field.
func (s *DeadLetterSuite) TestBuildProducerMessageNullPayload(c *C) {
	msg := buildProducerMessage(Request{
		Cluster: "prod", Group: "g", Topic: "t", Partition: 0, Offset: 1,
	}, time.Now())
	c.Assert(msg.Key, IsNil)
	c.Assert(msg.Value, IsNil)
}

// controllableProducer hands out per-request channels the test resolves.
type controllableProducer struct {
	requests   chan producedCall
	neverReply bool
}

type producedCall struct {
	topic     string
	key       sarama.Encoder
	value     sarama.Encoder
	headers   []sarama.RecordHeader
	receiptCh chan Receipt
}

func (p *controllableProducer) AsyncProduce(topic string, key, value sarama.Encoder,
	headers []sarama.RecordHeader,
) <-chan Receipt {
	call := producedCall{
		topic:     topic,
		key:       key,
		value:     value,
		headers:   headers,
		receiptCh: make(chan Receipt, 1),
	}
	p.requests <- call
	return call.receiptCh
}

// A successful broker receipt produces an EvDeadLettered event with nil err.
func (s *DeadLetterSuite) TestDispatcherSuccess(c *C) {
	prod := &controllableProducer{requests: make(chan producedCall, 1)}
	resultsCh := make(chan consumer.Event, 1)
	d := Spawn(s.ns, "dlq_topic", prod, resultsCh)

	d.Dump(Request{Offset: 7, Cluster: "prod", Group: "g", Topic: "t",
		Partition: 2, Value: []byte("v")})
	call := <-prod.requests
	c.Assert(call.topic, Equals, "dlq_topic")
	call.receiptCh <- Receipt{Partition: 5, Offset: 100}

	event := <-resultsCh
	c.Assert(event.T, Equals, consumer.EvDeadLettered)
	c.Assert(event.Offset, Equals, int64(7))
	c.Assert(event.Err, IsNil)

	d.Stop()
}

// A definitive failure (or an unknown result) is delivered as an error event
// so the caller can keep the source gap and retry.
func (s *DeadLetterSuite) TestDispatcherError(c *C) {
	prod := &controllableProducer{requests: make(chan producedCall, 1)}
	resultsCh := make(chan consumer.Event, 1)
	d := Spawn(s.ns, "dlq_topic", prod, resultsCh)

	d.Dump(Request{Offset: 8})
	call := <-prod.requests
	call.receiptCh <- Receipt{Err: errors.New("leader unavailable")}

	event := <-resultsCh
	c.Assert(event.T, Equals, consumer.EvDeadLettered)
	c.Assert(event.Offset, Equals, int64(8))
	c.Assert(event.Err, ErrorMatches, "leader unavailable")

	d.Stop()
}

// A lost receipt (nothing ever comes back) must not block shutdown: Stop
// releases the waiting delivery and no event is delivered.
func (s *DeadLetterSuite) TestDispatcherLostReceiptOnStop(c *C) {
	prod := &controllableProducer{requests: make(chan producedCall, MaxInFlight)}
	resultsCh := make(chan consumer.Event, 1)
	d := Spawn(s.ns, "dlq_topic", prod, resultsCh)

	d.Dump(Request{Offset: 9})
	<-prod.requests // request reached the producer, receipt never arrives

	done := make(chan struct{})
	go func() { d.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.Fatal("Stop blocked on a lost receipt")
	}
	select {
	case event := <-resultsCh:
		c.Fatalf("unexpected event after stop: %v", event)
	default:
	}
}
