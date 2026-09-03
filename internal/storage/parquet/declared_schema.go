package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SchemaAs returns the reader's schema with the CALLER's declared types
// installed wherever the file's own bytes can carry them — the columnar
// analogue of ReadRowsAs, and the same admission (retypeFromCatalog).
//
// The reader's own schema already restores the declared types of a file
// written from v0.18.0 on, because that writer stamps them into the footer
// under DeclaredSchemaKey. A file written before it carries no such key, so
// its IPv4/IPv6/MAC/UUID/BYTES/PORT/PROTOCOL/DURATION columns come back as
// the plain INT64/BYTE_ARRAY leaves parquet can express — raw storage form,
// which is #396's symptom on existing data (#423). The catalog knows what
// those files cannot say; this is how a columnar caller says so.
//
// Where the footer blob and the caller agree, the substitution is a no-op
// (retypeFromCatalog skips a column whose types already match). Where they
// disagree, the CALLER's type wins if the file's bytes can carry it and the
// call FAILS if they cannot — a named error rather than a decode of one
// type's bytes as another's. Passing nil returns the file's own schema
// unchanged, which is what every caller did before this existed.
func (r *Reader) SchemaAs(declared []Column) ([]Column, error) {
	if len(declared) == 0 {
		return r.schema.Columns, nil
	}
	return retypeFromCatalog(r.schema.Columns, declared, r.fr.SchemaRoot(), r.fr.Leaves())
}

// StripDeclaredSchema rewrites a parquet file's footer WITHOUT its
// DeclaredSchemaKey entry, producing the bytes a pre-v0.18.0 writer produced:
// a file that cannot say what nine of its column types are.
//
// It exists so the migration case can be CONSTRUCTED. Every suite here
// generates its fixtures with the CURRENT writer, so every file it reads
// carries the key and the case cannot occur however the corpus grows — the
// gate-coverage audit's finding, and the reason #423 stayed open behind a
// green #396. This is the fixture tool that closes it, and the only thing it
// is for: it never writes new data, only re-encodes an existing footer.
//
// The data pages are untouched and every column-chunk offset is absolute into
// the region BEFORE the footer, so shortening the footer cannot move them.
func StripDeclaredSchema(data []byte) ([]byte, error) {
	const magic = "PAR1"
	const trailer = 4 + len(magic) // footer length + magic
	if len(data) < trailer+len(magic) {
		return nil, fmt.Errorf("parquet: file too short to carry a footer (%d bytes)", len(data))
	}
	if !bytes.Equal(data[len(data)-len(magic):], []byte(magic)) {
		return nil, fmt.Errorf("parquet: file does not end with the %q magic", magic)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-trailer : len(data)-len(magic)]))
	start := len(data) - trailer - footerLen
	if footerLen <= 0 || start < len(magic) {
		return nil, fmt.Errorf("parquet: footer length %d does not fit a %d-byte file", footerLen, len(data))
	}
	meta, err := DecodeFileMetaData(data[start : len(data)-trailer])
	if err != nil {
		return nil, fmt.Errorf("parquet: decoding footer to strip %q: %w", DeclaredSchemaKey, err)
	}
	kept := meta.KeyValueMetadata[:0:0]
	stripped := false
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == DeclaredSchemaKey {
			stripped = true
			continue
		}
		kept = append(kept, kv)
	}
	if !stripped {
		return nil, fmt.Errorf("parquet: file carries no %q footer key, so there is nothing to strip "+
			"— the fixture is already in its pre-v0.18.0 form, or it was not written by this writer",
			DeclaredSchemaKey)
	}
	meta.KeyValueMetadata = kept

	out := make([]byte, 0, len(data))
	out = append(out, data[:start]...)
	footer := EncodeFileMetaData(meta)
	out = append(out, footer...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footer)))
	out = append(out, lenBuf[:]...)
	return append(out, magic...), nil
}

// StripCidrStatsOrder rewrites a parquet file's footer WITHOUT its
// CidrStatsOrderKey entry, producing the bytes a pre-#523 writer produced: a
// file whose CIDR column(s), if any, carry text-order min/max a reader must
// withhold rather than trust for pruning.
//
// Same tool, same reason as StripDeclaredSchema: every suite here generates
// its fixtures with the CURRENT writer, which always stamps the flag when it
// applies, so the old-footer case cannot occur however the corpus grows
// unless it is constructed. This is how #523's regression pins it on both
// sides — a file with the flag prunes, the same file stripped of it does
// not, and the ANSWER stays identical either way.
func StripCidrStatsOrder(data []byte) ([]byte, error) {
	const magic = "PAR1"
	const trailer = 4 + len(magic)
	if len(data) < trailer+len(magic) {
		return nil, fmt.Errorf("parquet: file too short to carry a footer (%d bytes)", len(data))
	}
	if !bytes.Equal(data[len(data)-len(magic):], []byte(magic)) {
		return nil, fmt.Errorf("parquet: file does not end with the %q magic", magic)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-trailer : len(data)-len(magic)]))
	start := len(data) - trailer - footerLen
	if footerLen <= 0 || start < len(magic) {
		return nil, fmt.Errorf("parquet: footer length %d does not fit a %d-byte file", footerLen, len(data))
	}
	meta, err := DecodeFileMetaData(data[start : len(data)-trailer])
	if err != nil {
		return nil, fmt.Errorf("parquet: decoding footer to strip %q: %w", CidrStatsOrderKey, err)
	}
	kept := meta.KeyValueMetadata[:0:0]
	stripped := false
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == CidrStatsOrderKey {
			stripped = true
			continue
		}
		kept = append(kept, kv)
	}
	if !stripped {
		return nil, fmt.Errorf("parquet: file carries no %q footer key, so there is nothing to strip "+
			"— the fixture is already in its pre-#523 form, or it holds no CIDR column",
			CidrStatsOrderKey)
	}
	meta.KeyValueMetadata = kept

	out := make([]byte, 0, len(data))
	out = append(out, data[:start]...)
	footer := EncodeFileMetaData(meta)
	out = append(out, footer...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footer)))
	out = append(out, lenBuf[:]...)
	return append(out, magic...), nil
}
