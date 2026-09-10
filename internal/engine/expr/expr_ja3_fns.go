// This file holds expr ja3 fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// --- JA3 TLS Fingerprinting ---

func fnJA3Fingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3str := buildJA3String(toBytes(args[0]))
	if ja3str == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3str))
	return hex.EncodeToString(hash[:])
}

func fnJA3String(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3String(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func fnJA3SFingerprint(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ja3s := buildJA3SString(toBytes(args[0]))
	if ja3s == "" {
		return nil
	}
	hash := md5.Sum([]byte(ja3s))
	return hex.EncodeToString(hash[:])
}

func fnJA3SString(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	result := buildJA3SString(toBytes(args[0]))
	if result == "" {
		return nil
	}
	return result
}

func buildJA3String(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 1 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34 // version + random
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	csLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+csLen > len(data) {
		return ""
	}
	var ciphers []string
	for i := 0; i < csLen; i += 2 {
		cs := int(binary.BigEndian.Uint16(data[offset+i : offset+i+2]))
		if !isGREASE(uint16(cs)) {
			ciphers = append(ciphers, strconv.Itoa(cs))
		}
	}
	offset += csLen
	if offset >= len(data) {
		return ""
	}
	compLen := int(data[offset])
	offset += 1 + compLen

	var extensions, ellipticCurves, ecPointFormats []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			if offset+extLen > extEnd {
				break
			}
			if extType == 10 && extLen >= 2 {
				listLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
				for j := 2; j+1 < 2+listLen && offset+j+1 < extEnd; j += 2 {
					curve := binary.BigEndian.Uint16(data[offset+j : offset+j+2])
					if !isGREASE(curve) {
						ellipticCurves = append(ellipticCurves, strconv.Itoa(int(curve)))
					}
				}
			}
			if extType == 11 && extLen >= 1 {
				fmtLen := int(data[offset])
				for j := 1; j < 1+fmtLen && offset+j < extEnd; j++ {
					ecPointFormats = append(ecPointFormats, strconv.Itoa(int(data[offset+j])))
				}
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%s,%s,%s,%s",
		tlsVersion,
		strings.Join(ciphers, "-"),
		strings.Join(extensions, "-"),
		strings.Join(ellipticCurves, "-"),
		strings.Join(ecPointFormats, "-"))
}

func buildJA3SString(data []byte) string {
	if len(data) < 5 || data[0] != 22 {
		return ""
	}
	if len(data) < 9 || data[5] != 2 {
		return ""
	}
	offset := 9
	if offset+2 > len(data) {
		return ""
	}
	tlsVersion := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 34
	if offset >= len(data) {
		return ""
	}
	sessIDLen := int(data[offset])
	offset += 1 + sessIDLen
	if offset+2 > len(data) {
		return ""
	}
	cipher := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 3 // cipher + compression

	var extensions []string
	if offset+2 <= len(data) {
		extTotalLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		extEnd := offset + extTotalLen
		if extEnd > len(data) {
			extEnd = len(data)
		}
		for offset+4 <= extEnd {
			extType := binary.BigEndian.Uint16(data[offset : offset+2])
			extLen := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
			offset += 4
			if !isGREASE(extType) {
				extensions = append(extensions, strconv.Itoa(int(extType)))
			}
			offset += extLen
		}
	}
	return fmt.Sprintf("%d,%d,%s", tlsVersion, cipher, strings.Join(extensions, "-"))
}

func isGREASE(v uint16) bool {
	return v&0x0f0f == 0x0a0a
}
