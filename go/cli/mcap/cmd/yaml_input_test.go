package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeShardMCAP writes a small chunked+indexed mcap with messages for
// the given log times on a single topic. Used to build synthetic
// rosbag2-style splits.
func writeShardMCAP(t *testing.T, path, topic string, logTimes []uint64) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	w, err := mcap.NewWriter(f, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: mcap.CompressionNone,
		IncludeCRC:  true,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteHeader(&mcap.Header{Profile: "ros2"}))
	require.NoError(t, w.WriteSchema(&mcap.Schema{
		ID: 1, Name: "std_msgs/msg/UInt64", Encoding: "ros2msg", Data: []byte("uint64 data"),
	}))
	require.NoError(t, w.WriteChannel(&mcap.Channel{
		ID: 1, SchemaID: 1, Topic: topic, MessageEncoding: "cdr",
	}))
	for i, lt := range logTimes {
		require.NoError(t, w.WriteMessage(&mcap.Message{
			ChannelID: 1, Sequence: uint32(i), LogTime: lt, Data: []byte{byte(i)},
		}))
	}
	require.NoError(t, w.Close())
}

func writeRos2BagYAML(t *testing.T, path string, files []string) {
	t.Helper()
	body := "rosbag2_bagfile_information:\n  version: 5\n  storage_identifier: mcap\n  relative_file_paths:\n"
	for _, f := range files {
		body += "    - " + f + "\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

func TestSort_YAMLInput(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.mcap")
	b := filepath.Join(dir, "b.mcap")
	// b's first message has a smaller timestamp than a's last, so a global
	// sort across the two shards reorders things.
	writeShardMCAP(t, a, "/topic", []uint64{10, 30, 50})
	writeShardMCAP(t, b, "/topic", []uint64{20, 40, 60})
	yaml := filepath.Join(dir, "metadata.yaml")
	writeRos2BagYAML(t, yaml, []string{"a.mcap", "b.mcap"})

	out := filepath.Join(dir, "sorted.mcap")
	f, err := os.Create(out)
	require.NoError(t, err)
	defer f.Close()

	// Honour the sort flags' defaults.
	sortChunked = true
	sortChunkSize = 4 * 1024 * 1024
	sortCompression = "zstd"
	sortIncludeCRC = true

	ctx := context.Background()
	newReader := utils.NewMCAPReader(ctx, yaml)
	err = utils.WithReader(ctx, yaml, func(_ bool, rs io.ReadSeeker) error {
		reader, err := newReader(rs)
		if err != nil {
			return err
		}
		defer reader.Close()
		return sortReader(f, reader)
	})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Verify the output is monotonic.
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	r, err := mcap.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer r.Close()
	it, err := r.Messages()
	require.NoError(t, err)
	var times []uint64
	for {
		_, _, msg, err := it.NextInto(nil)
		if err != nil {
			break
		}
		times = append(times, msg.LogTime)
	}
	assert.Equal(t, []uint64{10, 20, 30, 40, 50, 60}, times)
}

func TestDu_YAMLInputAggregates(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.mcap")
	b := filepath.Join(dir, "b.mcap")
	writeShardMCAP(t, a, "/topic", []uint64{10, 20, 30})
	writeShardMCAP(t, b, "/topic", []uint64{40, 50})

	ctx := context.Background()
	statsA, err := computeDuFor(ctx, a, false)
	require.NoError(t, err)
	statsB, err := computeDuFor(ctx, b, false)
	require.NoError(t, err)

	// Manual sum across the two files.
	expected := newDuStats()
	expected.merge(statsA)
	expected.merge(statsB)

	yaml := filepath.Join(dir, "metadata.yaml")
	writeRos2BagYAML(t, yaml, []string{"a.mcap", "b.mcap"})

	files, err := utils.ResolveSourceFiles(ctx, yaml)
	require.NoError(t, err)
	require.Len(t, files, 2)

	got := newDuStats()
	for _, f := range files {
		s, err := computeDuFor(ctx, f, false)
		require.NoError(t, err)
		got.merge(s)
	}

	assert.Equal(t, expected.recordKindTotal, got.recordKindTotal)
	assert.Equal(t, expected.totalMessageSize, got.totalMessageSize)
	for k, v := range expected.recordKindSize {
		assert.Equal(t, v, got.recordKindSize[k], "kind=%s", k)
	}
	for k, v := range expected.topicMessageSize {
		assert.Equal(t, v, got.topicMessageSize[k], "topic=%s", k)
	}
}
