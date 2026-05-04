package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/foxglove/mcap/go/mcap"
)

// MCAPReader is the surface used by callers that consume mcap content.
// Both *mcap.Reader (single file) and *multiMCAPReader (multi file)
// satisfy this interface, so helpers shared between the two dispatch
// branches can take it without caring which kind of input they have.
type MCAPReader interface {
	Header() *mcap.Header
	Info() (*mcap.Info, error)
	Messages(opts ...mcap.ReadOpt) (mcap.MessageIterator, error)
	GetAttachmentReader(offset uint64) (*mcap.AttachmentReader, error)
	GetMetadata(offset uint64) (*mcap.Metadata, error)
	Close()
}

// NewMCAPReader returns a constructor for an MCAPReader appropriate to
// location. For a rosbag2 metadata.yaml the constructor parses the
// already-opened ReadSeeker as yaml and opens its referenced shards;
// for any other path it wraps rs as a single *mcap.Reader.
//
// Typical usage from a command:
//
//	newReader := utils.NewMCAPReader(ctx, filename)
//	err := utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
//	    reader, err := newReader(rs)
//	    if err != nil {
//	        return err
//	    }
//	    defer reader.Close()
//	    // use reader (utils.MCAPReader)
//	})
func NewMCAPReader(ctx context.Context, location string) func(rs io.ReadSeeker) (MCAPReader, error) {
	if isRos2BagMetadataPath(location) {
		return func(rs io.ReadSeeker) (MCAPReader, error) {
			return newMultiMCAPReader(ctx, location, rs)
		}
	}
	return func(rs io.ReadSeeker) (MCAPReader, error) {
		return mcap.NewReader(rs)
	}
}

// newMultiMCAPReader constructs a multiMCAPReader from a ReadSeeker
// that points at rosbag2 metadata.yaml content. The yaml is parsed
// from rs (typically the same ReadSeeker that utils.WithReader handed
// to the caller's callback) and each referenced shard is opened. The
// returned reader owns the shard handles; callers must invoke Close.
//
// location is used to resolve relative shard paths and for error
// messages.
func newMultiMCAPReader(ctx context.Context, location string, rs io.ReadSeeker) (*multiMCAPReader, error) {
	meta, err := parseRos2BagMetadataReader(rs, location)
	if err != nil {
		return nil, err
	}
	shards, err := resolveRos2BagShards(location, meta)
	if err != nil {
		return nil, err
	}
	return openMultiMCAPFromPaths(ctx, shards)
}

// multiMCAPReader presents N physical mcap files as one virtual mcap
// source. Per-file *mcap.Reader instances are opened eagerly and
// closed together via Close. Attachment/metadata offsets exposed via
// Info() encode the originating file index in the high bits so that
// GetAttachmentReader and GetMetadata can dispatch back to the right
// file.
type multiMCAPReader struct {
	files    []string
	readers  []*mcap.Reader
	closeFns []func() error
	header   *mcap.Header
	info     *mcap.Info
}

const (
	// maxIndexedFiles is the maximum number of physical mcap files that
	// can be combined into a single multiMCAPReader. The high 16 bits
	// of an offset are used to encode the originating file index, which
	// caps the per-file logical size at 2**48 bytes (256 TiB) — well
	// beyond practical mcap sizes.
	maxIndexedFiles = 1 << 16
	offsetFileShift = 48
	offsetMask      = (uint64(1) << offsetFileShift) - 1
)

func encodeOffset(fileIdx int, offset uint64) uint64 {
	if offset > offsetMask {
		// Should not happen in practice; guard against silent corruption.
		offset &= offsetMask
	}
	return (uint64(fileIdx) << offsetFileShift) | offset
}

func decodeOffset(encoded uint64) (fileIdx int, offset uint64) {
	return int(encoded >> offsetFileShift), encoded & offsetMask
}

func openMultiMCAPFromPaths(ctx context.Context, files []string) (*multiMCAPReader, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no mcap files referenced")
	}
	if len(files) > maxIndexedFiles {
		return nil, fmt.Errorf("too many mcap files (%d > %d)", len(files), maxIndexedFiles)
	}
	m := &multiMCAPReader{
		files:    files,
		readers:  make([]*mcap.Reader, 0, len(files)),
		closeFns: make([]func() error, 0, len(files)),
	}
	for _, f := range files {
		closeFn, rs, err := GetReader(ctx, f)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("failed to open %s: %w", f, err)
		}
		r, err := mcap.NewReader(rs)
		if err != nil {
			_ = closeFn()
			m.Close()
			return nil, fmt.Errorf("failed to read %s: %w", f, err)
		}
		m.readers = append(m.readers, r)
		m.closeFns = append(m.closeFns, closeFn)
	}
	m.header = m.readers[0].Header()
	return m, nil
}

func (m *multiMCAPReader) Header() *mcap.Header {
	return m.header
}

func (m *multiMCAPReader) Info() (*mcap.Info, error) {
	if m.info != nil {
		return m.info, nil
	}
	infos := make([]*mcap.Info, 0, len(m.readers))
	for i, r := range m.readers {
		info, err := r.Info()
		if err != nil {
			return nil, fmt.Errorf("failed to read info from %s: %w", m.files[i], err)
		}
		infos = append(infos, info)
	}
	m.info = mergeMCAPInfos(infos)
	// Rewrite attachment/metadata index offsets to encode the file index
	// so that GetAttachmentReader / GetMetadata can dispatch correctly.
	attachIdx := 0
	metaIdx := 0
	for fileIdx, info := range infos {
		for _, ai := range info.AttachmentIndexes {
			out := *ai
			out.Offset = encodeOffset(fileIdx, ai.Offset)
			m.info.AttachmentIndexes[attachIdx] = &out
			attachIdx++
		}
		for _, mi := range info.MetadataIndexes {
			out := *mi
			out.Offset = encodeOffset(fileIdx, mi.Offset)
			m.info.MetadataIndexes[metaIdx] = &out
			metaIdx++
		}
	}
	return m.info, nil
}

func (m *multiMCAPReader) Messages(opts ...mcap.ReadOpt) (mcap.MessageIterator, error) {
	// Resolve the requested ordering once so we can pick the right
	// cross-file strategy. Per-file iterators still receive the same
	// opts, so within a file the requested ordering is honored.
	parsed := mcap.ReadOptions{Order: mcap.FileOrder}
	for _, opt := range opts {
		if err := opt(&parsed); err != nil {
			return nil, err
		}
	}
	openers := make([]func() (mcap.MessageIterator, error), len(m.readers))
	for i, r := range m.readers {
		r := r
		openers[i] = func() (mcap.MessageIterator, error) {
			return r.Messages(opts...)
		}
	}
	switch parsed.Order {
	case mcap.LogTimeOrder, mcap.ReverseLogTimeOrder:
		// Heap-merge across files so the global stream is correctly
		// ordered even when shards overlap in time.
		return newMergedIterator(openers, parsed.Order)
	default:
		// FileOrder (or any caller-defined default): chaining preserves
		// the natural per-file order at lower overhead.
		return &chainedIterator{openers: openers}, nil
	}
}

func (m *multiMCAPReader) GetAttachmentReader(offset uint64) (*mcap.AttachmentReader, error) {
	idx, real := decodeOffset(offset)
	if idx < 0 || idx >= len(m.readers) {
		return nil, fmt.Errorf("attachment offset references unknown file index %d", idx)
	}
	return m.readers[idx].GetAttachmentReader(real)
}

func (m *multiMCAPReader) GetMetadata(offset uint64) (*mcap.Metadata, error) {
	idx, real := decodeOffset(offset)
	if idx < 0 || idx >= len(m.readers) {
		return nil, fmt.Errorf("metadata offset references unknown file index %d", idx)
	}
	return m.readers[idx].GetMetadata(real)
}

func (m *multiMCAPReader) Close() {
	for _, r := range m.readers {
		if r != nil {
			r.Close()
		}
	}
	for _, c := range m.closeFns {
		if c != nil {
			_ = c()
		}
	}
}

// mergedIterator returns messages from a set of iterators, ordered by
// log time across all of them. It is used when callers request
// LogTimeOrder / ReverseLogTimeOrder over a multi-file source where
// shards may overlap in time.
//
// We don't reuse utils.PriorityQueue here because that type is
// hard-coded to ascending order and only carries *mcap.Message; this
// iterator must support both directions and pass schema/channel
// pointers alongside each message. With typical N (rosbag2 splits in
// the tens), the linear pickIndex is competitive with a heap.
type mergedIterator struct {
	iters []mcap.MessageIterator
	// Buffered head for each iterator. heads[i] is the next pending
	// (schema, channel, message) tuple for iters[i], or nil if iters[i]
	// has been exhausted.
	heads   []*mergedHead
	reverse bool
}

type mergedHead struct {
	schema  *mcap.Schema
	channel *mcap.Channel
	message *mcap.Message
}

func newMergedIterator(
	openers []func() (mcap.MessageIterator, error),
	order mcap.ReadOrder,
) (mcap.MessageIterator, error) {
	m := &mergedIterator{
		iters:   make([]mcap.MessageIterator, 0, len(openers)),
		heads:   make([]*mergedHead, 0, len(openers)),
		reverse: order == mcap.ReverseLogTimeOrder,
	}
	for _, open := range openers {
		it, err := open()
		if err != nil {
			return nil, err
		}
		m.iters = append(m.iters, it)
		m.heads = append(m.heads, nil)
	}
	// Prime each iterator with its first message so heads[i] reflects
	// each file's earliest available message.
	for i := range m.iters {
		if err := m.refill(i); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *mergedIterator) refill(i int) error {
	schema, channel, msg, err := m.iters[i].NextInto(nil)
	if err != nil {
		if errors.Is(err, io.EOF) {
			m.heads[i] = nil
			return nil
		}
		return err
	}
	m.heads[i] = &mergedHead{schema: schema, channel: channel, message: msg}
	return nil
}

// pickIndex returns the index of the head that should be emitted next,
// or -1 when all iterators are exhausted.
func (m *mergedIterator) pickIndex() int {
	picked := -1
	for i, h := range m.heads {
		if h == nil {
			continue
		}
		if picked == -1 {
			picked = i
			continue
		}
		if m.reverse {
			if h.message.LogTime > m.heads[picked].message.LogTime {
				picked = i
			}
		} else {
			if h.message.LogTime < m.heads[picked].message.LogTime {
				picked = i
			}
		}
	}
	return picked
}

func (m *mergedIterator) Next(_ []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	return m.NextInto(nil)
}

func (m *mergedIterator) NextInto(out *mcap.Message) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	idx := m.pickIndex()
	if idx == -1 {
		return nil, nil, nil, io.EOF
	}
	head := m.heads[idx]
	// Hand the buffered message back. If the caller supplied an out
	// buffer, copy into it for backwards compatibility with NextInto's
	// contract; otherwise return the head pointer directly.
	var msg *mcap.Message
	if out != nil {
		out.ChannelID = head.message.ChannelID
		out.Sequence = head.message.Sequence
		out.LogTime = head.message.LogTime
		out.PublishTime = head.message.PublishTime
		out.Data = append(out.Data[:0], head.message.Data...)
		msg = out
	} else {
		msg = head.message
	}
	schema, channel := head.schema, head.channel
	if err := m.refill(idx); err != nil {
		return nil, nil, nil, err
	}
	return schema, channel, msg, nil
}

// chainedIterator returns messages from a sequence of iterators, one
// after the other. Iterators are opened lazily and closed implicitly
// when exhausted.
type chainedIterator struct {
	openers []func() (mcap.MessageIterator, error)
	idx     int
	current mcap.MessageIterator
}

func (c *chainedIterator) advance() error {
	for c.current == nil {
		if c.idx >= len(c.openers) {
			return io.EOF
		}
		it, err := c.openers[c.idx]()
		c.idx++
		if err != nil {
			return err
		}
		c.current = it
	}
	return nil
}

func (c *chainedIterator) Next(buf []byte) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	for {
		if err := c.advance(); err != nil {
			return nil, nil, nil, err
		}
		schema, channel, msg, err := c.current.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.current = nil
				continue
			}
			return nil, nil, nil, err
		}
		return schema, channel, msg, nil
	}
}

func (c *chainedIterator) NextInto(msg *mcap.Message) (*mcap.Schema, *mcap.Channel, *mcap.Message, error) {
	for {
		if err := c.advance(); err != nil {
			return nil, nil, nil, err
		}
		schema, channel, m, err := c.current.NextInto(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.current = nil
				continue
			}
			return nil, nil, nil, err
		}
		return schema, channel, m, nil
	}
}

// channelKey identifies a channel across multiple files for deduplication.
type channelKey struct {
	topic           string
	messageEncoding string
	schemaName      string
	schemaEncoding  string
}

// mergeMCAPInfos aggregates a sequence of mcap.Info values (one per
// source file) into a single Info. Channels and schemas are
// deduplicated by (topic, encoding, schema). Statistics are summed and
// chunk/attachment/metadata indexes are concatenated. The resulting
// Info is suitable for reporting (e.g. printInfo) but its index offsets
// are not directly usable for I/O — see multiMCAPReader.Info() which
// rewrites attachment and metadata offsets to encode the originating
// file index.
func mergeMCAPInfos(infos []*mcap.Info) *mcap.Info {
	out := &mcap.Info{
		Header:   &mcap.Header{},
		Channels: map[uint16]*mcap.Channel{},
		Schemas:  map[uint16]*mcap.Schema{},
		Statistics: &mcap.Statistics{
			ChannelMessageCounts: map[uint16]uint64{},
		},
	}

	channelIDs := map[channelKey]uint16{}
	var nextChannelID uint16 = 1
	var nextSchemaID uint16 = 1
	schemaIDByName := map[string]uint16{}
	profiles := make([]string, 0, len(infos))
	library := ""
	librarySet := false

	out.Statistics.MessageStartTime = math.MaxUint64

	for _, info := range infos {
		if info == nil {
			continue
		}
		if info.Header != nil {
			profiles = append(profiles, info.Header.Profile)
			if !librarySet {
				library = info.Header.Library
				librarySet = true
			}
		}

		schemaRemap := map[uint16]uint16{0: 0}
		for _, schema := range info.Schemas {
			key := schema.Name + "\x00" + schema.Encoding
			id, ok := schemaIDByName[key]
			if !ok {
				id = nextSchemaID
				nextSchemaID++
				schemaIDByName[key] = id
				out.Schemas[id] = &mcap.Schema{
					ID:       id,
					Name:     schema.Name,
					Encoding: schema.Encoding,
					Data:     schema.Data,
				}
			}
			schemaRemap[schema.ID] = id
		}

		channelRemap := map[uint16]uint16{}
		for _, channel := range info.Channels {
			schemaName := ""
			schemaEncoding := ""
			if s, ok := info.Schemas[channel.SchemaID]; ok && s != nil {
				schemaName = s.Name
				schemaEncoding = s.Encoding
			}
			key := channelKey{
				topic:           channel.Topic,
				messageEncoding: channel.MessageEncoding,
				schemaName:      schemaName,
				schemaEncoding:  schemaEncoding,
			}
			id, ok := channelIDs[key]
			if !ok {
				id = nextChannelID
				nextChannelID++
				channelIDs[key] = id
				out.Channels[id] = &mcap.Channel{
					ID:              id,
					SchemaID:        schemaRemap[channel.SchemaID],
					Topic:           channel.Topic,
					MessageEncoding: channel.MessageEncoding,
					Metadata:        channel.Metadata,
				}
			}
			channelRemap[channel.ID] = id
		}

		if info.Statistics != nil {
			out.Statistics.MessageCount += info.Statistics.MessageCount
			out.Statistics.AttachmentCount += info.Statistics.AttachmentCount
			out.Statistics.MetadataCount += info.Statistics.MetadataCount
			out.Statistics.ChunkCount += info.Statistics.ChunkCount

			if info.Statistics.MessageCount > 0 {
				if info.Statistics.MessageStartTime < out.Statistics.MessageStartTime {
					out.Statistics.MessageStartTime = info.Statistics.MessageStartTime
				}
				if info.Statistics.MessageEndTime > out.Statistics.MessageEndTime {
					out.Statistics.MessageEndTime = info.Statistics.MessageEndTime
				}
			}

			for srcID, count := range info.Statistics.ChannelMessageCounts {
				if outID, ok := channelRemap[srcID]; ok {
					out.Statistics.ChannelMessageCounts[outID] += count
				}
			}
		}

		out.ChunkIndexes = append(out.ChunkIndexes, info.ChunkIndexes...)
		out.AttachmentIndexes = append(out.AttachmentIndexes, info.AttachmentIndexes...)
		out.MetadataIndexes = append(out.MetadataIndexes, info.MetadataIndexes...)
	}

	if out.Statistics.MessageStartTime == math.MaxUint64 {
		out.Statistics.MessageStartTime = 0
	}
	out.Statistics.ChannelCount = uint32(len(out.Channels))
	out.Statistics.SchemaCount = uint16(len(out.Schemas))
	out.Header.Profile = MatchingProfile(profiles)
	out.Header.Library = library
	return out
}

// MatchingProfile returns the common Profile string when every input
// agrees, and "" when they don't (or when the slice is empty). Used to
// derive a single Header.Profile from a set of mcap inputs.
func MatchingProfile(profiles []string) string {
	if len(profiles) == 0 {
		return ""
	}
	first := profiles[0]
	for _, p := range profiles {
		if p != first {
			return ""
		}
	}
	return first
}
