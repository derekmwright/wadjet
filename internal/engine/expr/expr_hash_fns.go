// This file holds expr hash fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
)

// --- Hash functions ---

func fnMD5(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := md5.Sum([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA256(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha256.Sum256([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}

func fnSHA512(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	h := sha512.Sum512([]byte(toString(args[0])))
	return hex.EncodeToString(h[:])
}
