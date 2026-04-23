package logsobj

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/dataset"
	"github.com/grafana/loki/v3/pkg/dataobj/internal/result"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/util/loser"
)

// sortMergeIterator returns an iterator that performs a k-way merge of records from multiple logs sections.
// It requires that the input sections are sorted by the same order.
func sortMergeIterator(ctx context.Context, sections []*dataobj.Section, sort logs.SortOrder) (result.Seq[logs.Record], error) {
	sequences := make([]*sectionSequence, 0, len(sections))
	for _, s := range sections {
		sec, err := logs.Open(ctx, s)
		if err != nil {
			return nil, fmt.Errorf("failed to open logs section: %w", err)
		}

		ds, err := logs.MakeColumnarDataset(sec)
		if err != nil {
			return nil, fmt.Errorf("creating columnar dataset: %w", err)
		}

		columns, err := result.Collect(ds.ListColumns(ctx))
		if err != nil {
			return nil, err
		}

		r := dataset.NewRowReader(dataset.RowReaderOptions{
			Dataset:  ds,
			Columns:  columns,
			Prefetch: true,
		})
		if err := r.Open(ctx); err != nil {
			return nil, fmt.Errorf("opening dataset row reader: %w", err)
		}

		sequences = append(sequences, &sectionSequence{
			section:         sec,
			DatasetSequence: logs.NewDatasetSequence(r, 8<<10),
		})
	}

	maxValue := result.Value(dataset.Row{
		Index: math.MaxInt,
		Values: []dataset.Value{
			dataset.Int64Value(math.MaxInt64), // StreamID
			dataset.Int64Value(math.MinInt64), // Timestamp
		},
	})

	tree := loser.New(sequences, maxValue, sectionSequenceAt, logs.CompareForSortOrder(sort), sectionSequenceClose)

	return result.Iter(
		func(yield func(logs.Record) bool) error {
			defer tree.Close()
			for tree.Next() {
				seq := tree.Winner()

				row, err := sectionSequenceAt(seq).Value()
				if err != nil {
					return err
				}

				var record logs.Record
				err = logs.DecodeRow(seq.section.Columns(), row, &record, nil)
				if err != nil || !yield(record) {
					return err
				}
			}
			return nil
		}), nil
}

// sortMergeIteratorWithSchema returns a k-way merge in schema key order.
// All input sections must already be sorted by schema key
func sortMergeIteratorWithSchema(
	ctx context.Context, sections []*dataobj.Section, sortKeys map[int64]string,
) (result.Seq[logs.Record], error) {
	sequences, err := openSchemaMergeSequences(ctx, sections)
	if err != nil {
		return nil, err
	}
	return newSchemaMergeIterator(sequences, sortKeys), nil
}

func newSchemaMergeIterator(sequences []*sectionSequence, sortKeys map[int64]string) result.Seq[logs.Record] {
	maxValue := result.Value(dataset.Row{
		Index: math.MaxInt,
		Values: []dataset.Value{
			dataset.Int64Value(math.MaxInt64), // StreamID
			dataset.Int64Value(math.MinInt64), // Timestamp
		},
	})
	tree := loser.New(sequences, maxValue, sectionSequenceAt, logs.CompareForSortSchema(sortKeys), sectionSequenceClose)
	return result.Iter(
		func(yield func(logs.Record) bool) error {
			defer tree.Close()
			for tree.Next() {
				seq := tree.Winner()
				row, err := sectionSequenceAt(seq).Value()
				if err != nil {
					return err
				}
				var record logs.Record
				if err = logs.DecodeRow(seq.section.Columns(), row, &record, nil); err != nil {
					return err
				}
				record.SortKey = sortKeys[record.StreamID]
				if !yield(record) {
					return nil
				}
			}
			return nil
		})
}

// sortAllRecordsWithSchema reads all records from all sections, assigns sort keys,
// sorts them in memory by schema key, and returns an iterator over the sorted records.
// This is used when transitioning from non-schema-sorted sections to schema-sorted sections.
func sortAllRecordsWithSchema(
	ctx context.Context, sections []*dataobj.Section, sortKeys map[int64]string,
) (result.Seq[logs.Record], error) {
	var records []logs.Record
	for _, s := range sections {
		sec, err := logs.Open(ctx, s)
		if err != nil {
			return nil, fmt.Errorf("failed to open logs section: %w", err)
		}
		ds, err := logs.MakeColumnarDataset(sec)
		if err != nil {
			return nil, fmt.Errorf("creating columnar dataset: %w", err)
		}
		columns, err := result.Collect(ds.ListColumns(ctx))
		if err != nil {
			return nil, err
		}
		r := dataset.NewRowReader(dataset.RowReaderOptions{
			Dataset:  ds,
			Columns:  columns,
			Prefetch: true,
		})
		if err := r.Open(ctx); err != nil {
			return nil, fmt.Errorf("opening dataset row reader: %w", err)
		}
		seq := logs.NewDatasetSequence(r, 8<<10)
		for seq.Next() {
			row, err := seq.At().Value()
			if err != nil {
				seq.Close()
				return nil, err
			}
			var record logs.Record
			if err := logs.DecodeRow(sec.Columns(), row, &record, nil); err != nil {
				seq.Close()
				return nil, err
			}
			record.SortKey = sortKeys[record.StreamID]
			records = append(records, record)
		}
		seq.Close()
	}

	sortRecordsBySchema(records)

	return result.Iter(func(yield func(logs.Record) bool) error {
		for _, rec := range records {
			if !yield(rec) {
				return nil
			}
		}
		return nil
	}), nil
}

// sortRecordsBySchema sorts records by [schema key ASC, timestamp DESC]
func sortRecordsBySchema(records []logs.Record) {
	slices.SortFunc(records, func(a, b logs.Record) int {
		if res := cmp.Compare(a.SortKey, b.SortKey); res != 0 {
			return res
		}
		return b.Timestamp.Compare(a.Timestamp)
	})
}

func openSchemaMergeSequences(ctx context.Context, sections []*dataobj.Section) ([]*sectionSequence, error) {
	sequences := make([]*sectionSequence, 0, len(sections))
	for _, s := range sections {
		seq, err := newSectionSequence(ctx, s)
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, seq)
	}
	return sequences, nil
}

func newSectionSequence(ctx context.Context, s *dataobj.Section) (*sectionSequence, error) {
	sec, err := logs.Open(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("failed to open logs section: %w", err)
	}
	ds, err := logs.MakeColumnarDataset(sec)
	if err != nil {
		return nil, fmt.Errorf("creating columnar dataset: %w", err)
	}
	columns, err := result.Collect(ds.ListColumns(ctx))
	if err != nil {
		return nil, err
	}
	r := dataset.NewRowReader(dataset.RowReaderOptions{
		Dataset:  ds,
		Columns:  columns,
		Prefetch: true,
	})
	if err := r.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening dataset row reader: %w", err)
	}
	return &sectionSequence{
		section:         sec,
		DatasetSequence: logs.NewDatasetSequence(r, 8<<10),
	}, nil
}

type sectionSequence struct {
	logs.DatasetSequence
	section *logs.Section
}

var _ loser.Sequence = (*sectionSequence)(nil)

func sectionSequenceAt(seq *sectionSequence) result.Result[dataset.Row] { return seq.At() }
func sectionSequenceClose(seq *sectionSequence)                         { seq.Close() }
