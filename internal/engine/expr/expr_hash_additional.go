// This file holds expr hash additional; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash/crc32"
)

// --- Hash: additional ---

func fnSHA1(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha1.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnCRC32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return float64(crc32.ChecksumIEEE([]byte(toString(args[0]))))
}

func fnHMACSHA256(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha256.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}

func fnHMACSHA512(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	mac := hmac.New(sha512.New, []byte(toString(args[1])))
	mac.Write([]byte(toString(args[0])))
	return hex.EncodeToString(mac.Sum(nil))
}
