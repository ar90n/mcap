package utils

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Ros2BagMetadata mirrors the subset of fields we need from a rosbag2
// metadata.yaml document. Only fields used to enumerate the underlying
// storage files are decoded; unknown fields are ignored.
type Ros2BagMetadata struct {
	Version           int               `yaml:"version"`
	StorageIdentifier string            `yaml:"storage_identifier"`
	RelativeFilePaths []string          `yaml:"relative_file_paths"`
	Files             []ros2BagFileInfo `yaml:"files"`
}

type ros2BagFileInfo struct {
	Path string `yaml:"path"`
}

type ros2BagMetadataDoc struct {
	Info Ros2BagMetadata `yaml:"rosbag2_bagfile_information"`
}

// isRos2BagMetadataPath returns true if the given path looks like a
// rosbag2 metadata.yaml file by extension. Remote URIs like
// "gs://bucket/path/metadata.yaml" are also accepted.
func isRos2BagMetadataPath(p string) bool {
	if p == "" {
		return false
	}
	ext := strings.ToLower(filepath.Ext(p))
	return ext == ".yaml" || ext == ".yml"
}

// parseRos2BagMetadataReader parses a rosbag2 metadata.yaml from an
// already-open Reader. location is used in error messages only. The
// reader is consumed in full.
func parseRos2BagMetadataReader(r io.Reader, location string) (*Ros2BagMetadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", location, err)
	}
	var doc ros2BagMetadataDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse %s as rosbag2 metadata: %w", location, err)
	}
	if doc.Info.Version == 0 && len(doc.Info.RelativeFilePaths) == 0 && len(doc.Info.Files) == 0 {
		return nil, fmt.Errorf("file %s is not a recognized rosbag2 metadata.yaml", location)
	}
	return &doc.Info, nil
}

// joinRos2BagPath resolves rel against the directory of base. base may
// be a local filesystem path or a remote URI (e.g. "gs://bucket/dir/metadata.yaml").
// Absolute relative paths and relative paths with their own scheme are
// returned unchanged.
func joinRos2BagPath(base, rel string) string {
	if relScheme, _, _ := GetScheme(rel); relScheme != "" {
		return rel
	}
	scheme, bucket, p := GetScheme(base)
	if scheme == "" {
		if filepath.IsAbs(rel) {
			return rel
		}
		return filepath.Join(filepath.Dir(base), rel)
	}
	// Remote URI: use slash-based joining.
	dir := path.Dir(p)
	if dir == "." || dir == "/" {
		dir = ""
	}
	if dir == "" {
		return fmt.Sprintf("%s://%s/%s", scheme, bucket, rel)
	}
	return fmt.Sprintf("%s://%s/%s", scheme, bucket, path.Join(dir, rel))
}

// ResolveSourceFiles expands a CLI input location to the list of
// physical mcap files it represents. A rosbag2 metadata.yaml is
// parsed and expanded to its referenced shards (in recorded order);
// any other path is returned as a singleton. Shard paths are resolved
// relative to the directory containing the metadata file. location
// may be a local path or a remote URI supported by GetReader.
func ResolveSourceFiles(ctx context.Context, location string) ([]string, error) {
	if !isRos2BagMetadataPath(location) {
		return []string{location}, nil
	}
	var meta *Ros2BagMetadata
	err := WithReader(ctx, location, func(_ bool, rs io.ReadSeeker) error {
		var err error
		meta, err = parseRos2BagMetadataReader(rs, location)
		return err
	})
	if err != nil {
		return nil, err
	}
	return resolveRos2BagShards(location, meta)
}

// resolveRos2BagShards turns a parsed metadata document into the list
// of shard paths/URIs, resolved relative to the metadata file's
// directory. Used by ResolveSourceFiles and by newMultiMCAPReader,
// both of which have already parsed the yaml content.
func resolveRos2BagShards(location string, meta *Ros2BagMetadata) ([]string, error) {
	if meta.StorageIdentifier != "" && meta.StorageIdentifier != "mcap" {
		return nil, fmt.Errorf(
			"rosbag2 metadata at %s uses storage_identifier %q; only \"mcap\" is supported",
			location, meta.StorageIdentifier,
		)
	}

	rels := meta.RelativeFilePaths
	if len(rels) == 0 {
		for _, f := range meta.Files {
			if f.Path != "" {
				rels = append(rels, f.Path)
			}
		}
	}
	if len(rels) == 0 {
		return nil, fmt.Errorf("rosbag2 metadata at %s lists no storage files", location)
	}

	resolved := make([]string, 0, len(rels))
	for _, rel := range rels {
		resolved = append(resolved, joinRos2BagPath(location, rel))
	}
	return resolved, nil
}

// ExpandRos2BagInputs takes a list of CLI inputs and expands any rosbag2
// metadata.yaml entries into the list of underlying mcap files they
// reference. Non-yaml entries are passed through unchanged. Order is
// preserved. Inputs may be local paths or remote URIs supported by
// GetReader.
func ExpandRos2BagInputs(ctx context.Context, inputs []string) ([]string, error) {
	expanded := make([]string, 0, len(inputs))
	for _, in := range inputs {
		files, err := ResolveSourceFiles(ctx, in)
		if err != nil {
			return nil, err
		}
		expanded = append(expanded, files...)
	}
	return expanded, nil
}
