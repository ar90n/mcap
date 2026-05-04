package utils

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))
}

func TestIsRos2BagMetadataPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"metadata.yaml", true},
		{"metadata.yml", true},
		{"PATH/TO/metadata.YAML", true},
		{"gs://bucket/dir/metadata.yaml", true},
		{"file.mcap", false},
		{"", false},
		{"file", false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, isRos2BagMetadataPath(c.path), c.path)
	}
}

func TestResolveRos2BagFiles_RelativeFilePaths(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	writeFile(t, yamlPath, `rosbag2_bagfile_information:
  version: 5
  storage_identifier: mcap
  relative_file_paths:
    - my_bag_0.mcap
    - my_bag_1.mcap
`)

	files, err := ResolveSourceFiles(context.Background(), yamlPath)
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, filepath.Join(dir, "my_bag_0.mcap"), files[0])
	assert.Equal(t, filepath.Join(dir, "my_bag_1.mcap"), files[1])
}

func TestResolveRos2BagFiles_FilesFallback(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	writeFile(t, yamlPath, `rosbag2_bagfile_information:
  version: 6
  storage_identifier: mcap
  files:
    - path: bag_0.mcap
      message_count: 10
    - path: bag_1.mcap
      message_count: 5
`)

	files, err := ResolveSourceFiles(context.Background(), yamlPath)
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, filepath.Join(dir, "bag_0.mcap"), files[0])
	assert.Equal(t, filepath.Join(dir, "bag_1.mcap"), files[1])
}

func TestResolveRos2BagFiles_AbsolutePath(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	abs := filepath.Join(dir, "elsewhere.mcap")
	writeFile(t, yamlPath, `rosbag2_bagfile_information:
  version: 5
  storage_identifier: mcap
  relative_file_paths:
    - `+abs+`
`)

	files, err := ResolveSourceFiles(context.Background(), yamlPath)
	require.NoError(t, err)
	require.Equal(t, []string{abs}, files)
}

func TestResolveRos2BagFiles_UnsupportedStorage(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	writeFile(t, yamlPath, `rosbag2_bagfile_information:
  version: 5
  storage_identifier: sqlite3
  relative_file_paths:
    - my_bag_0.db3
`)

	_, err := ResolveSourceFiles(context.Background(), yamlPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite3")
}

func TestResolveRos2BagFiles_NotMetadata(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	writeFile(t, yamlPath, `unrelated:
  field: value
`)

	_, err := ResolveSourceFiles(context.Background(), yamlPath)
	require.Error(t, err)
}

func TestExpandRos2BagInputs_Mixed(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "metadata.yaml")
	writeFile(t, yamlPath, `rosbag2_bagfile_information:
  version: 5
  storage_identifier: mcap
  relative_file_paths:
    - a.mcap
    - b.mcap
`)

	expanded, err := ExpandRos2BagInputs(context.Background(), []string{"first.mcap", yamlPath, "last.mcap"})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"first.mcap",
		filepath.Join(dir, "a.mcap"),
		filepath.Join(dir, "b.mcap"),
		"last.mcap",
	}, expanded)
}

func TestJoinRos2BagPath(t *testing.T) {
	cases := []struct {
		base string
		rel  string
		want string
	}{
		{"gs://bkt/dir/metadata.yaml", "a.mcap", "gs://bkt/dir/a.mcap"},
		{"gs://bkt/metadata.yaml", "a.mcap", "gs://bkt/a.mcap"},
		{"gs://bkt/dir/metadata.yaml", "sub/a.mcap", "gs://bkt/dir/sub/a.mcap"},
		{"s3://bkt/dir/metadata.yaml", "a.mcap", "s3://bkt/dir/a.mcap"},
		{"/local/dir/metadata.yaml", "a.mcap", filepath.Join("/local/dir", "a.mcap")},
		{"gs://bkt/dir/metadata.yaml", "gs://other/x.mcap", "gs://other/x.mcap"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, joinRos2BagPath(c.base, c.rel), "base=%s rel=%s", c.base, c.rel)
	}
}
