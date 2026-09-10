// This file holds expr tls fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
)

// --- TLS inspection functions ---

// fnTLSSNI extracts the Server Name Indication from a TLS ClientHello.
// tls_sni(payload_hex) → 'www.example.com'
func fnTLSSNI(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	return parseTLSSNI(data)
}

// fnTLSVersion extracts the TLS version from a TLS record header.
// Returns human-readable version: 'TLS 1.0', 'TLS 1.2', 'TLS 1.3', etc.
func fnTLSVersion(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 5 {
		return nil
	}
	// TLS record: type(1) + version(2) + length(2)
	major := data[1]
	minor := data[2]
	return tlsVersionString(major, minor)
}

// fnTLSRecordType identifies the TLS record content type.
// 20=ChangeCipherSpec, 21=Alert, 22=Handshake, 23=ApplicationData
func fnTLSRecordType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 1 {
		return nil
	}
	switch data[0] {
	case 20:
		return "ChangeCipherSpec"
	case 21:
		return "Alert"
	case 22:
		return "Handshake"
	case 23:
		return "ApplicationData"
	default:
		return fmt.Sprintf("Unknown(%d)", data[0])
	}
}

// fnIsTLSClientHello tests if payload starts with a TLS ClientHello.
func fnIsTLSClientHello(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	// TLS record: type 22 (Handshake), then version, then length, then handshake type 1 (ClientHello)
	if len(data) < 6 {
		return false
	}
	return data[0] == 22 && data[5] == 1
}

// fnTLSHandshakeType returns the handshake message type.
// 1=ClientHello, 2=ServerHello, 11=Certificate, 12=ServerKeyExchange, etc.
func fnTLSHandshakeType(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := toBytes(args[0])
	if len(data) < 6 || data[0] != 22 {
		return nil
	}
	hsType := data[5]
	switch hsType {
	case 0:
		return "HelloRequest"
	case 1:
		return "ClientHello"
	case 2:
		return "ServerHello"
	case 4:
		return "NewSessionTicket"
	case 11:
		return "Certificate"
	case 12:
		return "ServerKeyExchange"
	case 13:
		return "CertificateRequest"
	case 14:
		return "ServerHelloDone"
	case 15:
		return "CertificateVerify"
	case 16:
		return "ClientKeyExchange"
	case 20:
		return "Finished"
	default:
		return fmt.Sprintf("Unknown(%d)", hsType)
	}
}
