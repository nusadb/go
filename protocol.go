package nusadb

// Low-level Nusa Wire Protocol codec (docs/wire-protocol.md, PROTOCOL_VERSION 1.1).
//
// A frame is [type:u8][len:u32][payload]; len is the total size including the
// 5-byte header. Big-endian throughout. Strings are length-prefixed UTF-8
// ([len:u32][bytes]), NOT null-terminated.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	protocolMagic = 0x4E555341 // "NUSA"
	protocolMajor = 1
	// Request minor 1 to receive the typed RowDescription (per-column type tags). A 1.0 server
	// ignores it and answers with the classic untyped form, which the driver still handles.
	protocolMinor  = 1
	headerLen      = 5
	maxFrameLen    = 256 * 1024 * 1024
	scramMechanism = "SCRAM-SHA-256"
)

// Frontend (client -> server) message type bytes.
const (
	tStartup      = 'S'
	tQuery        = 'Q'
	tParse        = 'P'
	tBind         = 'B'
	tDescribe     = 'D'
	tExecute      = 'E'
	tSync         = 'Y'
	tSaslInitial  = 'p'
	tSaslResponse = 'r'
	tCancel       = 'K'
	tTerminate    = 'X'
	tCopyData     = 'd' // COPY ... FROM STDIN data chunk (§12.1)
	tCopyDone     = 'c' // end of the client's COPY data stream
	tCopyFail     = 'f' // abort the in-progress COPY: [message:Str]
)

// Backend (server -> client) message type bytes.
const (
	bAuth                = 'R'
	bBackendKey          = 'K'
	bReady               = 'Z'
	bCommandComplete     = 'C'
	bError               = 'E'
	bRowDescription      = 'T'
	bRowDescriptionTyped = 'y' // protocol 1.1 (typed columns)
	bDataRow             = 'D'
	bParseComplete       = '1'
	bBindComplete        = '2'
	bCloseComplete       = '3'
	bParamDesc           = 't'
	bNoData              = 'n'
	bPortalSuspended     = 'z'
	bCopyIn              = 'G'
	bCopyOut             = 'H'
	bCopyData            = 'd'
	bCopyDone            = 'c'
	bNotification        = 'A' // async LISTEN/NOTIFY: [pid:u32][channel:str][payload:str]
)

// Authentication sub-codes (leading u32 of an `R` message).
const (
	authOK           = 0
	authSASL         = 10
	authSASLContinue = 11
	authSASLFinal    = 12
)

// typeTags maps a RowDescriptionTyped type tag (protocol 1.1, wire-protocol.md §9.2) to a canonical
// type name. An unknown/0x00 tag is UNKNOWN (treated as text).
var typeTags = map[byte]string{
	0x00: "UNKNOWN", 0x01: "BOOL", 0x02: "INT", 0x03: "FLOAT", 0x04: "NUMERIC",
	0x05: "TEXT", 0x06: "BYTES", 0x07: "DATE", 0x08: "TIME", 0x09: "TIMETZ",
	0x0A: "TIMESTAMP", 0x0B: "TIMESTAMPTZ", 0x0C: "INTERVAL", 0x0D: "UUID",
	0x0E: "JSON", 0x0F: "ARRAY", 0x10: "VECTOR",
}

// typeName returns the canonical type name for a type tag (UNKNOWN if unrecognised).
func typeName(tag byte) string {
	if name, ok := typeTags[tag]; ok {
		return name
	}
	return "UNKNOWN"
}

// writer accumulates a message payload using the protocol's primitive encodings.
type writer struct {
	buf []byte
}

func (w *writer) u8(v byte)    { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }

func (w *writer) str(s string) {
	w.u32(uint32(len(s)))
	w.buf = append(w.buf, s...)
}

// fields encodes a Fields list (DataRow / Bind params): [count:u16] then per
// field a present byte (0 = NULL, 1 = [len:u32][bytes]).
func (w *writer) fields(values [][]byte, present []bool) {
	w.u16(uint16(len(values)))
	for i, v := range values {
		if !present[i] {
			w.u8(0)
			continue
		}
		w.u8(1)
		w.u32(uint32(len(v)))
		w.raw(v)
	}
}

// frame wraps the payload in [type][len][payload].
func (w *writer) frame(msgType byte) []byte {
	total := len(w.buf) + headerLen
	out := make([]byte, 0, total)
	out = append(out, msgType)
	out = binary.BigEndian.AppendUint32(out, uint32(total))
	out = append(out, w.buf...)
	return out
}

// reader reads the protocol's primitive encodings from a payload buffer.
type reader struct {
	buf []byte
	pos int
}

func (r *reader) take(n int) ([]byte, error) {
	if r.pos+n > len(r.buf) {
		return nil, fmt.Errorf("nusadb: truncated payload")
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *reader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *reader) u16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) str() (string, error) {
	n, err := r.u32()
	if err != nil {
		return "", err
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *reader) rest() []byte {
	b := r.buf[r.pos:]
	r.pos = len(r.buf)
	return b
}

// fields decodes a Fields list; a nil entry is SQL NULL.
func (r *reader) decodeFields() ([][]byte, error) {
	count, err := r.u16()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, count)
	for i := range out {
		present, err := r.u8()
		if err != nil {
			return nil, err
		}
		if present == 0 {
			out[i] = nil
			continue
		}
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		b, err := r.take(int(n))
		if err != nil {
			return nil, err
		}
		field := make([]byte, len(b))
		copy(field, b)
		out[i] = field
	}
	return out, nil
}

// message is one decoded backend frame.
type message struct {
	typ     byte
	payload []byte
}

func (m message) reader() *reader { return &reader{buf: m.payload} }

// readMessage reads one backend frame from r.
func readMessage(r *bufio.Reader) (message, error) {
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return message{}, err
	}
	total := binary.BigEndian.Uint32(header[1:5])
	if total < headerLen {
		return message{}, fmt.Errorf("nusadb: malformed frame length %d", total)
	}
	if total > maxFrameLen {
		return message{}, fmt.Errorf("nusadb: frame too large: %d", total)
	}
	payload := make([]byte, total-headerLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return message{}, err
	}
	return message{typ: header[0], payload: payload}, nil
}

// --- frontend message builders ---

func encStartup(user, database string) []byte {
	var w writer
	w.u32(protocolMagic)
	w.u16(protocolMajor)
	w.u16(protocolMinor)
	w.str(user)
	w.str(database)
	return w.frame(tStartup)
}

func encQuery(sql string) []byte {
	var w writer
	w.str(sql)
	return w.frame(tQuery)
}

func encParse(name, sql string) []byte {
	var w writer
	w.str(name)
	w.str(sql)
	w.u16(0)
	return w.frame(tParse)
}

func encBind(portal, statement string, values [][]byte, present []bool) []byte {
	var w writer
	w.str(portal)
	w.str(statement)
	w.fields(values, present)
	w.u16(0) // result format count: empty => all text
	return w.frame(tBind)
}

func encDescribePortal(name string) []byte {
	var w writer
	w.u8('P')
	w.str(name)
	return w.frame(tDescribe)
}

func encExecute(portal string, maxRows uint32) []byte {
	var w writer
	w.str(portal)
	w.u32(maxRows)
	return w.frame(tExecute)
}

func encSync() []byte      { var w writer; return w.frame(tSync) }
func encTerminate() []byte { var w writer; return w.frame(tTerminate) }

func encCopyData(data []byte) []byte {
	var w writer
	w.raw(data)
	return w.frame(tCopyData)
}

func encCopyDone() []byte { var w writer; return w.frame(tCopyDone) }

func encCopyFail(message string) []byte {
	var w writer
	w.str(message)
	return w.frame(tCopyFail)
}

func encSaslInitial(mechanism string, data []byte) []byte {
	var w writer
	w.str(mechanism)
	w.u32(uint32(len(data)))
	w.raw(data)
	return w.frame(tSaslInitial)
}

func encSaslResponse(data []byte) []byte {
	var w writer
	w.raw(data)
	return w.frame(tSaslResponse)
}

func encCancel(pid, secret uint32) []byte {
	var w writer
	w.u32(pid)
	w.u32(secret)
	return w.frame(tCancel)
}
