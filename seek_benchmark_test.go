package parquet_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

type BenchmarkRow struct {
	Value string `parquet:",snappy"`
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func BenchmarkSeekToRow(b *testing.B) {
	// Create test data with many rows and pages
	const numRows = 10000

	// Create parquet file in memory with small page buffer to force many pages
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[BenchmarkRow](buf, parquet.PageBufferSize(128))

	// Write rows
	for i := 0; i < numRows; i++ {
		// generate a random string
		randomValue := generateRandomString(100)

		if _, err := w.Write([]BenchmarkRow{{Value: randomValue}}); err != nil {
			b.Fatal(err)
		}
	}

	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	// Open parquet file for reading
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		b.Fatal(err)
	}

	rgs := pf.RowGroups()
	if len(rgs) != 1 {
		b.Fatalf("expected exactly 1 row group, got %d", len(rgs))
	}

	b.Run("sequential_forward", func(b *testing.B) {
		pages := rgs[0].ColumnChunks()[0].Pages()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			rowIndex := int64(i % numRows)
			if err := pages.SeekToRow(rowIndex); err != nil {
				b.Fatal(err)
			}

			pg, err := pages.ReadPage()
			if err != nil {
				b.Fatal(err)
			}

			vals := make([]parquet.Value, 1)
			n, err := pg.Values().ReadValues(vals)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}

			_, _ = pages.ReadPage()

			if n != 1 {
				b.Fatalf("expected 1 value, got %d", n)
			}
		}
	})

	b.Run("sequential_backward", func(b *testing.B) {
		pages := rgs[0].ColumnChunks()[0].Pages()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			rowIndex := int64(numRows - 1 - (i % numRows))
			if err := pages.SeekToRow(rowIndex); err != nil {
				b.Fatal(err)
			}

			pg, err := pages.ReadPage()
			if err != nil {
				b.Fatal(err)
			}

			_, _ = pages.ReadPage()

			vals := make([]parquet.Value, 1)
			n, err := pg.Values().ReadValues(vals)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}

			if n != 1 {
				b.Fatalf("expected 1 value, got %d", n)
			}
		}
	})

	b.Run("random_seeks", func(b *testing.B) {
		pages := rgs[0].ColumnChunks()[0].Pages()
		rng := rand.New(rand.NewSource(42))

		// Pre-generate random row indices
		randomRows := make([]int64, b.N)
		for i := range randomRows {
			randomRows[i] = int64(rng.Intn(numRows))
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			rowIndex := randomRows[i]
			if err := pages.SeekToRow(rowIndex); err != nil {
				b.Fatal(err)
			}

			pg, err := pages.ReadPage()
			if err != nil {
				b.Fatal(err)
			}

			_, _ = pages.ReadPage()

			vals := make([]parquet.Value, 1)
			n, err := pg.Values().ReadValues(vals)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}

			if n != 1 {
				b.Fatalf("expected 1 value, got %d", n)
			}
		}
	})

	b.Run("localized_seeks", func(b *testing.B) {
		pages := rgs[0].ColumnChunks()[0].Pages()
		b.ResetTimer()

		// Seek within same page repeatedly (rows 100-199 should be in same page)
		for i := 0; i < b.N; i++ {
			rowIndex := int64(100 + (i % 100))
			if err := pages.SeekToRow(rowIndex); err != nil {
				b.Fatal(err)
			}

			pg, err := pages.ReadPage()
			if err != nil {
				b.Fatal(err)
			}

			_, _ = pages.ReadPage()

			vals := make([]parquet.Value, 1)
			n, err := pg.Values().ReadValues(vals)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}

			if n != 1 {
				b.Fatalf("expected 1 value, got %d", n)
			}
		}
	})
}
