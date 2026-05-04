package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/spf13/cobra"
)

var (
	addAttachmentLogTime      string
	addAttachmentName         string
	addAttachmentCreationTime string
	addAttachmentFilename     string
	addAttachmentMediaType    string
)

var (
	getAttachmentName   string
	getAttachmentOffset uint64
	getAttachmentOutput string
)

func getAttachment(w io.Writer, reader utils.MCAPReader, idx *mcap.AttachmentIndex) error {
	ar, err := reader.GetAttachmentReader(idx.Offset)
	if err != nil {
		return fmt.Errorf("failed to get attachment reader at offset %d: %w", idx.Offset, err)
	}
	if _, err := io.Copy(w, ar.Data()); err != nil {
		return fmt.Errorf("failed to copy attachment to output: %w", err)
	}
	return nil
}

var getAttachmentCmd = &cobra.Command{
	Use:   "attachment",
	Short: "Get an attachment by name or offset",
	Run: func(_ *cobra.Command, args []string) {
		ctx := context.Background()
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]

		var output io.Writer
		var err error
		if getAttachmentOutput == "" {
			if !utils.StdoutRedirected() {
				die(PleaseRedirect)
			}
			output = os.Stdout
		} else {
			output, err = os.Create(getAttachmentOutput)
			if err != nil {
				die("failed to create output file: %s", err)
			}
		}

		extract := func(reader utils.MCAPReader) error {
			info, err := reader.Info()
			if err != nil {
				return fmt.Errorf("failed to get mcap info: %w", err)
			}
			attachments := make(map[string][]*mcap.AttachmentIndex)
			for _, attachmentIdx := range info.AttachmentIndexes {
				attachments[attachmentIdx.Name] = append(
					attachments[attachmentIdx.Name],
					attachmentIdx,
				)
			}

			switch {
			case len(attachments[getAttachmentName]) == 0:
				die("attachment %s not found", getAttachmentName)
			case len(attachments[getAttachmentName]) == 1:
				if err := getAttachment(output, reader, attachments[getAttachmentName][0]); err != nil {
					die("failed to get attachment: %s", err)
				}
			case len(attachments[getAttachmentName]) > 1:
				if getAttachmentOffset == 0 {
					return fmt.Errorf(
						"multiple attachments named %s exist (specify an offset)",
						getAttachmentName,
					)
				}
				for _, idx := range attachments[getAttachmentName] {
					if idx.Offset == getAttachmentOffset {
						return getAttachment(output, reader, idx)
					}
				}
				return fmt.Errorf(
					"failed to find attachment %s at offset %d",
					getAttachmentName,
					getAttachmentOffset,
				)
			}
			return nil
		}
		newReader := utils.NewMCAPReader(ctx, filename)
		err = utils.WithReader(ctx, filename, func(_ bool, rs io.ReadSeeker) error {
			reader, err := newReader(rs)
			if err != nil {
				return fmt.Errorf("failed to construct reader: %w", err)
			}
			defer reader.Close()
			return extract(reader)
		})
		if err != nil {
			die("failed to extract attachment: %s", err)
		}
	},
}

var addAttachmentCmd = &cobra.Command{
	Use:   "attachment",
	Short: "Add an attachment to an MCAP file",
	Run: func(_ *cobra.Command, args []string) {
		if len(args) != 1 {
			die("Unexpected number of args")
		}
		filename := args[0]

		f, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
		if err != nil {
			die("failed to open file: %s", err)
		}
		defer f.Close()

		attachment, err := os.Open(addAttachmentFilename)
		if err != nil {
			die("failed to open attachment file: %s", err)
		}
		defer attachment.Close()

		stat, err := attachment.Stat()
		if err != nil {
			die("failed to stat file: %s", err)
		}
		contentLength := stat.Size()
		fi, err := os.Stat(addAttachmentFilename)
		if err != nil {
			die("failed to stat file %s", addAttachmentFilename)
		}
		createTime := uint64(fi.ModTime().UTC().UnixNano())
		if addAttachmentCreationTime != "" {
			date, err := parseDateOrNanos(addAttachmentCreationTime)
			if err != nil {
				die("failed to parse creation date: %s", err)
			}
			createTime = date
		}
		logTime := uint64(time.Now().UTC().UnixNano())
		if addAttachmentLogTime != "" {
			date, err := parseDateOrNanos(addAttachmentLogTime)
			if err != nil {
				die("failed to parse log date: %s", err)
			}
			logTime = date
		}
		err = utils.AmendMCAP(f, []*mcap.Attachment{
			{
				LogTime:    logTime,
				CreateTime: createTime,
				Name:       utils.DefaultString(addAttachmentName, addAttachmentFilename),
				MediaType:  addAttachmentMediaType,
				DataSize:   uint64(contentLength),
				Data:       attachment,
			},
		}, nil)
		if err != nil {
			die("failed to add attachment: %s. You may need to run `mcap recover` to repair the file.", err)
		}
	},
}

func init() {
	addCmd.AddCommand(addAttachmentCmd)
	addAttachmentCmd.PersistentFlags().StringVarP(&addAttachmentFilename, "file", "f", "", "filename of attachment to add")
	addAttachmentCmd.PersistentFlags().StringVarP(
		&addAttachmentName, "name", "n", "", "name of attachment to add (defaults to filename)",
	)
	addAttachmentCmd.PersistentFlags().StringVarP(
		&addAttachmentMediaType, "content-type", "", "application/octet-stream", "content type of attachment",
	)
	addAttachmentCmd.PersistentFlags().StringVarP(
		&addAttachmentLogTime,
		"log-time",
		"",
		"",
		"attachment log time in nanoseconds or RFC3339 format (defaults to current timestamp)",
	)
	addAttachmentCmd.PersistentFlags().StringVarP(
		&addAttachmentCreationTime,
		"creation-time",
		"",
		"",
		"attachment creation time in nanoseconds or RFC3339 format (defaults to ctime)",
	)
	err := addAttachmentCmd.MarkPersistentFlagRequired("file")
	if err != nil {
		die("failed to mark --file flag as required: %s", err)
	}

	getCmd.AddCommand(getAttachmentCmd)
	getAttachmentCmd.PersistentFlags().StringVarP(&getAttachmentName, "name", "n", "", "name of attachment to extract")
	getAttachmentCmd.PersistentFlags().Uint64VarP(&getAttachmentOffset, "offset", "", 0, "offset of attachment to extract")
	getAttachmentCmd.PersistentFlags().StringVarP(
		&getAttachmentOutput,
		"output",
		"o",
		"",
		"location to write attachment to",
	)
	err = getAttachmentCmd.MarkPersistentFlagRequired("name")
	if err != nil {
		die("failed to mark --name flag as required: %s", err)
	}
}
