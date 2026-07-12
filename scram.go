package nusadb

// Client-side SCRAM-SHA-256 (RFC 5802 / RFC 7677), docs/wire-protocol.md §7.2.
// Dependency-free: crypto/pbkdf2 is in the Go 1.24 standard library.

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

const gs2Header = "n,,"

// channelBinding is base64("n,,") — the `c=` value in client-final.
var channelBinding = base64.StdEncoding.EncodeToString([]byte(gs2Header))

func hmacSHA256(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

// scramClientFirst returns (clientFirstBare, fullClientFirst) with a fresh nonce.
func scramClientFirst(user string) (string, string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	nonce := base64.StdEncoding.EncodeToString(raw)
	bare := fmt.Sprintf("n=%s,r=%s", user, nonce)
	return bare, gs2Header + bare, nil
}

// scramClientFinal builds the client-final message and the expected server
// signature (base64) for mutual-auth verification.
func scramClientFinal(password, clientFirstBare, serverFirst string) (final []byte, expectedSig string, err error) {
	combinedNonce, salt, iterations, err := parseServerFirst(serverFirst)
	if err != nil {
		return nil, "", err
	}

	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, 32)
	if err != nil {
		return nil, "", err
	}
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	withoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, combinedNonce)
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	clientSig := hmacSHA256(storedKey[:], []byte(authMessage))

	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	finalStr := fmt.Sprintf("%s,p=%s", withoutProof, base64.StdEncoding.EncodeToString(proof))

	serverKey := hmacSHA256(salted, []byte("Server Key"))
	serverSig := hmacSHA256(serverKey, []byte(authMessage))
	expected := base64.StdEncoding.EncodeToString(serverSig)
	return []byte(finalStr), expected, nil
}

func parseServerFirst(message string) (combinedNonce string, salt []byte, iterations int, err error) {
	for _, field := range strings.Split(message, ",") {
		key, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch key {
		case "r":
			combinedNonce = value
		case "s":
			salt, err = base64.StdEncoding.DecodeString(value)
			if err != nil {
				return "", nil, 0, err
			}
		case "i":
			iterations, err = strconv.Atoi(value)
			if err != nil {
				return "", nil, 0, err
			}
		}
	}
	if combinedNonce == "" || len(salt) == 0 || iterations <= 0 {
		return "", nil, 0, fmt.Errorf("nusadb: malformed server-first message")
	}
	return combinedNonce, salt, iterations, nil
}

// verifyServerFinal checks the server-final `v=<signature>` against the expected
// value in constant time.
func verifyServerFinal(serverFinal, expectedSig string) bool {
	for _, field := range strings.Split(serverFinal, ",") {
		key, value, found := strings.Cut(field, "=")
		if found && key == "v" {
			return subtle.ConstantTimeCompare([]byte(value), []byte(expectedSig)) == 1
		}
	}
	return false
}
