package utils

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestMCAP writes a small mcap with a single topic and a sequence of
// messages whose log times and message data are derived from logTimes.
func writeTestMCAP(t *testing.T, path string, topic string, logTimes []uint64) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	w, err := mcap.NewWriter(f, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   1024,
		Compression: mcap.CompressionNone,
	})
	require.NoError(t, err)
	require.NoError(t, w.WriteHeader(&mcap.Header{Profile: "ros2"}))
	require.NoError(t, w.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "std_msgs/msg/UInt64",
		Encoding: "ros2msg",
		Data:     []byte("uint64 data"),
	}))
	require.NoError(t, w.WriteChannel(&mcap.Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           topic,
		MessageEncoding: "cdr",
	}))
	require.NoError(t, w.WriteMetadata(&mcap.Metadata{
		Name:     "shard-info",
		Metadata: map[string]string{"path": path},
	}))
	for i, lt := range logTimes {
		require.NoError(t, w.WriteMessage(&mcap.Message{
			ChannelID: 1,
			Sequence:  uint32(i),
			LogTime:   lt,
			Data:      []byte{byte(i)},
		}))
	}
	require.NoError(t, w.Close())
}

func writeMetadataYAML(t *testing.T, path string, files []string) {
	t.Helper()
	body := "rosbag2_bagfile_information:\n  version: 5\n  storage_identifier: mcap\n  relative_file_paths:\n"
	for _, f := range files {
		body += "    - " + f + "\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

// withTestMCAPReader runs fn against an MCAPReader appropriate to
// location, mirroring the pattern commands use: NewMCAPReader picks
// the constructor based on the path and WithReader provides the
// underlying ReadSeeker.
func withTestMCAPReader(t *testing.T, location string, fn func(MCAPReader)) {
	t.Helper()
	ctx := context.Background()
	newReader := NewMCAPReader(ctx, location)
	require.NoError(t, WithReader(ctx, location, func(_ bool, rs io.ReadSeeker) error {
		reader, err := newReader(rs)
		if err != nil {
			return err
		}
		defer reader.Close()
		fn(reader)
		return nil
	}))
}

func TestNewMCAPReader_SingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "single.mcap")
	writeTestMCAP(t, path, "/topic", []uint64{10, 20, 30})

	withTestMCAPReader(t, path, func(reader MCAPReader) {
		info, err := reader.Info()
		require.NoError(t, err)
		require.NotNil(t, info.Statistics)
		assert.Equal(t, uint64(3), info.Statistics.MessageCount)
		assert.Equal(t, "ros2", reader.Header().Profile)

		it, err := reader.Messages()
		require.NoError(t, err)
		count := 0
		for {
			_, _, _, err := it.NextInto(nil)
			if err != nil {
				require.True(t, errors.Is(err, io.EOF))
				break
			}
			count++
		}
		assert.Equal(t, 3, count)
	})
}

func TestNewMCAPReader_MultiFileViaYAML(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.mcap")
	b := filepath.Join(dir, "b.mcap")
	writeTestMCAP(t, a, "/topic", []uint64{10, 20, 30})
	writeTestMCAP(t, b, "/topic", []uint64{40, 50})
	yaml := filepath.Join(dir, "metadata.yaml")
	writeMetadataYAML(t, yaml, []string{"a.mcap", "b.mcap"})

	withTestMCAPReader(t, yaml, func(reader MCAPReader) {
		// Aggregated info: 5 messages, channel coalesced.
		info, err := reader.Info()
		require.NoError(t, err)
		require.NotNil(t, info.Statistics)
		assert.Equal(t, uint64(5), info.Statistics.MessageCount)
		assert.Equal(t, uint64(10), info.Statistics.MessageStartTime)
		assert.Equal(t, uint64(50), info.Statistics.MessageEndTime)
		assert.Len(t, info.Channels, 1, "channel should be coalesced across files")
		assert.Len(t, info.Schemas, 1, "schema should be coalesced across files")
		// Both files contributed metadata records under the same name.
		assert.Equal(t, uint32(2), info.Statistics.MetadataCount)
		assert.Len(t, info.MetadataIndexes, 2)

		// Chained iteration yields all messages in log-time order.
		it, err := reader.Messages()
		require.NoError(t, err)
		logTimes := []uint64{}
		for {
			_, _, msg, err := it.NextInto(nil)
			if err != nil {
				require.True(t, errors.Is(err, io.EOF))
				break
			}
			logTimes = append(logTimes, msg.LogTime)
		}
		assert.Equal(t, []uint64{10, 20, 30, 40, 50}, logTimes)
	})
}

func TestNewMCAPReader_LogTimeOrderMergesAcrossShards(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.mcap")
	b := filepath.Join(dir, "b.mcap")
	// Deliberately overlap the two shards' log times so file order and
	// log-time order disagree.
	writeTestMCAP(t, a, "/topic", []uint64{10, 30, 50})
	writeTestMCAP(t, b, "/topic", []uint64{20, 40, 60})
	yaml := filepath.Join(dir, "metadata.yaml")
	writeMetadataYAML(t, yaml, []string{"a.mcap", "b.mcap"})

	withTestMCAPReader(t, yaml, func(reader MCAPReader) {
		// FileOrder: chained, no global sort.
		it, err := reader.Messages(mcap.UsingIndex(true), mcap.InOrder(mcap.FileOrder))
		require.NoError(t, err)
		var fileOrder []uint64
		for {
			_, _, msg, err := it.NextInto(nil)
			if err != nil {
				break
			}
			fileOrder = append(fileOrder, msg.LogTime)
		}
		assert.Equal(t, []uint64{10, 30, 50, 20, 40, 60}, fileOrder)

		// LogTimeOrder: heap-merged across shards.
		it, err = reader.Messages(mcap.UsingIndex(true), mcap.InOrder(mcap.LogTimeOrder))
		require.NoError(t, err)
		var logTimeOrder []uint64
		for {
			_, _, msg, err := it.NextInto(nil)
			if err != nil {
				break
			}
			logTimeOrder = append(logTimeOrder, msg.LogTime)
		}
		assert.Equal(t, []uint64{10, 20, 30, 40, 50, 60}, logTimeOrder)
	})
}

func TestNewMCAPReader_GetMetadataDispatchesByFile(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.mcap")
	b := filepath.Join(dir, "b.mcap")
	writeTestMCAP(t, a, "/topic", []uint64{1})
	writeTestMCAP(t, b, "/topic", []uint64{2})
	yaml := filepath.Join(dir, "metadata.yaml")
	writeMetadataYAML(t, yaml, []string{"a.mcap", "b.mcap"})

	withTestMCAPReader(t, yaml, func(reader MCAPReader) {
		info, err := reader.Info()
		require.NoError(t, err)
		require.Len(t, info.MetadataIndexes, 2)

		// The encoded "path" value in each metadata record points back to its
		// originating file. Reading via the merged index must dispatch to the
		// correct underlying reader.
		paths := []string{}
		for _, idx := range info.MetadataIndexes {
			md, err := reader.GetMetadata(idx.Offset)
			require.NoError(t, err)
			paths = append(paths, md.Metadata["path"])
		}
		assert.Contains(t, paths, a)
		assert.Contains(t, paths, b)
	})
}
