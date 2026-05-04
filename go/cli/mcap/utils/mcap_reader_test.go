package utils

import (
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeMCAPInfos(t *testing.T) {
	infoA := &mcap.Info{
		Header: &mcap.Header{Profile: "ros2", Library: "rosbag2_storage_mcap"},
		Schemas: map[uint16]*mcap.Schema{
			1: {ID: 1, Name: "std_msgs/msg/String", Encoding: "ros2msg"},
		},
		Channels: map[uint16]*mcap.Channel{
			1: {ID: 1, SchemaID: 1, Topic: "/chatter", MessageEncoding: "cdr"},
		},
		Statistics: &mcap.Statistics{
			MessageCount:         10,
			ChannelCount:         1,
			SchemaCount:          1,
			ChunkCount:           2,
			MessageStartTime:     100,
			MessageEndTime:       200,
			ChannelMessageCounts: map[uint16]uint64{1: 10},
		},
	}
	infoB := &mcap.Info{
		Header: &mcap.Header{Profile: "ros2", Library: "rosbag2_storage_mcap"},
		Schemas: map[uint16]*mcap.Schema{
			// same schema in input B has different ID
			7: {ID: 7, Name: "std_msgs/msg/String", Encoding: "ros2msg"},
			8: {ID: 8, Name: "std_msgs/msg/Int32", Encoding: "ros2msg"},
		},
		Channels: map[uint16]*mcap.Channel{
			// Same channel as A under different ID
			3: {ID: 3, SchemaID: 7, Topic: "/chatter", MessageEncoding: "cdr"},
			4: {ID: 4, SchemaID: 8, Topic: "/numbers", MessageEncoding: "cdr"},
		},
		Statistics: &mcap.Statistics{
			MessageCount:         15,
			ChannelCount:         2,
			SchemaCount:          2,
			ChunkCount:           3,
			MessageStartTime:     150,
			MessageEndTime:       300,
			ChannelMessageCounts: map[uint16]uint64{3: 5, 4: 10},
		},
	}

	merged := mergeMCAPInfos([]*mcap.Info{infoA, infoB})
	require.NotNil(t, merged.Statistics)
	assert.Equal(t, uint64(25), merged.Statistics.MessageCount)
	assert.Equal(t, uint32(5), merged.Statistics.ChunkCount)
	assert.Equal(t, uint64(100), merged.Statistics.MessageStartTime)
	assert.Equal(t, uint64(300), merged.Statistics.MessageEndTime)
	// /chatter coalesced; /numbers added separately
	assert.Equal(t, 2, len(merged.Channels))
	assert.Equal(t, 2, len(merged.Schemas))
	assert.Equal(t, "ros2", merged.Header.Profile)

	// /chatter should accumulate counts from both inputs
	var chatterCount, numbersCount uint64
	for id, ch := range merged.Channels {
		switch ch.Topic {
		case "/chatter":
			chatterCount = merged.Statistics.ChannelMessageCounts[id]
		case "/numbers":
			numbersCount = merged.Statistics.ChannelMessageCounts[id]
		}
	}
	assert.Equal(t, uint64(15), chatterCount)
	assert.Equal(t, uint64(10), numbersCount)
}

func TestMergeMCAPInfos_DifferentProfiles(t *testing.T) {
	a := &mcap.Info{
		Header:     &mcap.Header{Profile: "ros2"},
		Channels:   map[uint16]*mcap.Channel{},
		Schemas:    map[uint16]*mcap.Schema{},
		Statistics: &mcap.Statistics{ChannelMessageCounts: map[uint16]uint64{}},
	}
	b := &mcap.Info{
		Header:     &mcap.Header{Profile: "ros1"},
		Channels:   map[uint16]*mcap.Channel{},
		Schemas:    map[uint16]*mcap.Schema{},
		Statistics: &mcap.Statistics{ChannelMessageCounts: map[uint16]uint64{}},
	}
	merged := mergeMCAPInfos([]*mcap.Info{a, b})
	assert.Equal(t, "", merged.Header.Profile)
}

func TestEncodeDecodeOffset(t *testing.T) {
	cases := []struct {
		fileIdx int
		offset  uint64
	}{
		{0, 1234},
		{1, 0},
		{42, 0xDEADBEEF},
		{maxIndexedFiles - 1, 0xFFFFFFFFFFFF},
	}
	for _, c := range cases {
		encoded := encodeOffset(c.fileIdx, c.offset)
		idx, off := decodeOffset(encoded)
		assert.Equal(t, c.fileIdx, idx)
		assert.Equal(t, c.offset, off)
	}
}

func TestChainedIterator_EmptyAndChained(t *testing.T) {
	openers := []func() (mcap.MessageIterator, error){
		func() (mcap.MessageIterator, error) { return &fixedIterator{}, nil },
		func() (mcap.MessageIterator, error) {
			return &fixedIterator{
				msgs: []chainedTestMsg{
					{topic: "/a", logTime: 1},
					{topic: "/b", logTime: 2},
				},
			}, nil
		},
		func() (mcap.MessageIterator, error) {
			return &fixedIterator{
				msgs: []chainedTestMsg{{topic: "/c", logTime: 3}},
			}, nil
		},
	}
	it := &chainedIterator{openers: openers}

	var topics []string
	for {
		_, ch, _, err := it.NextInto(nil)
		if err != nil {
			break
		}
		topics = append(topics, ch.Topic)
	}
	assert.Equal(t, []string{"/a", "/b", "/c"}, topics)
}

type chainedTestMsg struct {
	topic   string
	logTime uint64
}

type fixedIterator struct {
	msgs []chainedTestMsg
	i    int
}

func (f *fixedIterator) Next(buf []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	return f.NextInto(nil)
}

func (f *fixedIterator) NextInto(_ *mcap.Message) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	if f.i >= len(f.msgs) {
		return nil, nil, nil, io.EOF
	}
	m := f.msgs[f.i]
	f.i++
	ch := &mcap.Channel{Topic: m.topic}
	msg := &mcap.Message{LogTime: m.logTime}
	return nil, ch, msg, nil
}
