package parquet

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

type seekResult struct {
	seekedPage       Page
	fullPage         Page
	fullPageFirstRow int64
}

type FilePagesV2 struct {
	chunk    *FileColumnChunk
	rbuf     *bufio.Reader
	rbufpool *sync.Pool
	section  io.SectionReader

	protocol thrift.CompactProtocol
	decoder  thrift.Decoder

	baseOffset int64
	dataOffset int64
	dictOffset int64
	dictionary Dictionary

	curPage     Page
	curFirstRow int64

	rowToSeek  int64
	bufferSize int
}

func (f *FilePagesV2) init(c *FileColumnChunk, reader io.ReaderAt) {
	f.chunk = c
	f.baseOffset = c.chunk.MetaData.DataPageOffset
	f.dataOffset = f.baseOffset
	f.bufferSize = c.file.config.ReadBufferSize
	f.rowToSeek = -1

	if c.chunk.MetaData.DictionaryPageOffset != 0 {
		f.baseOffset = c.chunk.MetaData.DictionaryPageOffset
		f.dictOffset = f.baseOffset
	}

	f.section = *io.NewSectionReader(reader, f.baseOffset, c.chunk.MetaData.TotalCompressedSize)
	f.rbuf, f.rbufpool = getBufioReader(&f.section, f.bufferSize)
	f.decoder.Reset(f.protocol.NewReader(f.rbuf))
}

// ReadPages reads the next from from f.
func (f *FilePagesV2) ReadPage() (Page, error) {
	if f.chunk == nil {
		return nil, io.EOF
	}

	if f.rowToSeek >= 0 {
		result, err := f.seekToRow(f.rowToSeek)
		if err != nil {
			return nil, err
		}

		f.setFullPage(result)
		f.rowToSeek = -1

		// Return the skipPage directly if present
		if result.seekedPage != nil { // jpe - possible for seekedPage to be nil?
			return result.seekedPage, nil
		}
	}

	page, err := f.readNextPage()
	if err != nil {
		return nil, err
	}

	nr := int64(0)
	if f.curPage != nil {
		nr = f.curPage.NumRows() // jpe what's it mean if curPage is nil?
	}

	f.setFullPage(seekResult{ // jpe - test that mixes seeks and reads
		fullPage:         page,
		fullPageFirstRow: f.curFirstRow + nr,
	})

	return page, nil
}

func (f *FilePagesV2) setFullPage(res seekResult) {
	/*	if f.curPage != nil {
		//		Release(f.curPage) jpe - do i need retain/release here?
		f.curPage = nil
		f.curFirstRow = -1
	}*/

	if res.fullPage != nil {
		//		Retain(res.fullPage)
		f.curPage = res.fullPage
		f.curFirstRow = res.fullPageFirstRow
	}
}

// reads the next page from the column chunk, returning it as a Page. doesn't return dictionary pages
func (f *FilePagesV2) readNextPage() (Page, error) {
	for {
		// Instantiate a new format.PageHeader for each page.
		//
		// A previous implementation reused page headers to save allocations.
		// https://github.com/segmentio/parquet-go/pull/484
		// The optimization turned out to be less effective than expected,
		// because all the values referenced by pointers in the page header
		// are lost when the header is reset and put back in the pool.
		// https://github.com/parquet-go/parquet-go/pull/11
		//
		// Even after being reset, reusing page headers still produced instability
		// issues.
		// https://github.com/parquet-go/parquet-go/issues/70
		header := new(format.PageHeader)
		if err := f.decoder.Decode(header); err != nil {
			return nil, err
		}

		data, err := f.readPage(header, f.rbuf)
		if err != nil {
			return nil, err
		}

		var page Page
		switch header.Type {
		case format.DataPageV2:
			page, err = f.readDataPageV2(header, data)
		case format.DictionaryPage:
			// Sometimes parquet files do not have the dictionary page offset
			// recorded in the column metadata. We account for this by lazily
			// reading dictionary pages when we encounter them.
			err = f.readDictionaryPage(header, data)
		default:
			err = fmt.Errorf("cannot read values of type %s from page", header.Type)
		}

		data.unref()

		if err != nil {
			return nil, fmt.Errorf("decoding page of column %q: %w", f.columnPath(), err)
		}

		if page == nil {
			continue
		}

		return page, nil
	}
}

func (f *FilePagesV2) readDictionary() error {
	chunk := io.NewSectionReader(f.section.Outer())
	rbuf, pool := getBufioReader(chunk, f.bufferSize)
	defer putBufioReader(rbuf, pool)

	decoder := thrift.NewDecoder(f.protocol.NewReader(rbuf))

	header := new(format.PageHeader)

	if err := decoder.Decode(header); err != nil {
		return err
	}

	page := buffers.get(int(header.CompressedPageSize))
	defer page.unref()

	if _, err := io.ReadFull(rbuf, page.data); err != nil {
		return err
	}

	return f.readDictionaryPage(header, page)
}

func (f *FilePagesV2) readDictionaryPage(header *format.PageHeader, page *buffer) error {
	if header.DictionaryPageHeader == nil {
		return ErrMissingPageHeader
	}
	d, err := f.chunk.column.decodeDictionary(DictionaryPageHeader{header.DictionaryPageHeader}, page, header.UncompressedPageSize)
	if err != nil {
		return err
	}
	f.dictionary = d
	return nil
}

func (f *FilePagesV2) readDataPageV2(header *format.PageHeader, page *buffer) (Page, error) {
	if header.DataPageHeaderV2 == nil {
		return nil, ErrMissingPageHeader
	}
	if isDictionaryFormat(header.DataPageHeaderV2.Encoding) && f.dictionary == nil {
		// If the program seeked to a row passed the first page, the dictionary
		// page may not have been seen, in which case we have to lazily load it
		// from the beginning of column chunk.
		if err := f.readDictionary(); err != nil {
			return nil, err
		}
	}
	return f.chunk.column.decodeDataPageV2(DataPageHeaderV2{header.DataPageHeaderV2}, page, f.dictionary, header.UncompressedPageSize)
}

func (f *FilePagesV2) readPage(header *format.PageHeader, reader *bufio.Reader) (*buffer, error) {
	page := buffers.get(int(header.CompressedPageSize))
	defer page.unref()

	if _, err := io.ReadFull(reader, page.data); err != nil {
		return nil, err
	}

	if header.CRC != 0 {
		headerChecksum := uint32(header.CRC)
		bufferChecksum := crc32.ChecksumIEEE(page.data)

		if headerChecksum != bufferChecksum {
			// The parquet specs indicate that corruption errors could be
			// handled gracefully by skipping pages, tho this may not always
			// be practical. Depending on how the pages are consumed,
			// missing rows may cause unpredictable behaviors in algorithms.
			//
			// For now, we assume these errors to be fatal, but we may
			// revisit later and improve error handling to be more resilient
			// to data corruption.
			return nil, fmt.Errorf("crc32 checksum mismatch in page of column %q: want=0x%08X got=0x%08X: %w",
				f.columnPath(),
				headerChecksum,
				bufferChecksum,
				ErrCorrupted,
			)
		}
	}

	page.ref()
	return page, nil
}

func (f *FilePagesV2) SeekToRow(rowIndex int64) (err error) {
	f.rowToSeek = rowIndex
	return nil // jpe - handle out of range seek err return?
}

// SeekToRow seeks to the given row index in the column chunk. (jpe - everywhere we set curPage we need to release if non-nil)
func (f *FilePagesV2) seekToRow(rowIndex int64) (seekResult, error) {
	var result seekResult
	if f.chunk == nil {
		return result, io.ErrClosedPipe
	}

	skip := int64(0)

	if f.curPage != nil && rowIndex >= f.curFirstRow && rowIndex < f.curFirstRow+f.curPage.NumRows() {
		// we are already in the right page, just skip within it
		skip = rowIndex - f.curFirstRow
		result.seekedPage = f.curPage.Slice(skip, f.curPage.NumRows())
		Release(result.seekedPage) // have to release b/c of the slice? jpe?
		// Don't update curFirstRow or curPage - they still refer to the original page
		// for potential future optimizations
		return result, nil
	}

	var err error
	if index := f.chunk.offsetIndex.Load(); index == nil {
		_, err = f.section.Seek(f.dataOffset-f.baseOffset, io.SeekStart)
		skip = rowIndex
	} else {
		pages := index.index.PageLocations
		index := sort.Search(len(pages), func(i int) bool {
			return pages[i].FirstRowIndex > rowIndex
		}) - 1
		if index < 0 {
			return result, ErrSeekOutOfRange
		}
		_, err = f.section.Seek(pages[index].Offset-f.baseOffset, io.SeekStart)
		skip = rowIndex - pages[index].FirstRowIndex
	}
	f.rbuf.Reset(&f.section)

	if err != nil {
		return result, err
	}

	// read pages forward until we reach the desired row
	for {
		page, err := f.readNextPage()
		if err != nil {
			return result, err
		}

		numRows := page.NumRows()
		if numRows <= skip {
			skip -= numRows
			Release(page)
			continue
		}

		var tail Page
		if skip == 0 {
			tail = page // jpe - this is going to break when we release tail or page and forget they are the same object
		} else {
			// this is the page!
			tail = page.Slice(skip, numRows)
			Release(tail) // have to release b/c of the slice? jpe?
		}

		result.seekedPage = tail
		result.fullPage = page                    // Keep the original page for potential optimizations
		result.fullPageFirstRow = rowIndex - skip // First row of the original page
		return result, nil
	}

}

// Close closes the page reader.
func (f *FilePagesV2) Close() error {
	putBufioReader(f.rbuf, f.rbufpool)
	f.chunk = nil
	f.section = io.SectionReader{}
	f.rbuf = nil
	f.rbufpool = nil
	f.baseOffset = 0
	f.dataOffset = 0
	f.dictOffset = 0
	f.dictionary = nil

	return nil
}

func (f *FilePagesV2) columnPath() columnPath {
	return columnPath(f.chunk.column.Path())
}
