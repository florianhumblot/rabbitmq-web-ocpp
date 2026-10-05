// Package ocpp holds OCPP-J framing helpers and the CSMS answers to
// charger-initiated CALLs, for OCPP 1.6 and 2.x.
package ocpp

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	Call            = 2
	CallResult      = 3
	CallError       = 4
	CallResultError = 5
	Send            = 6
)

// Routing keys of frames sent by chargers: ocpp16.BootNotification.req.
var chargerKey = regexp.MustCompile(`^(ocpp\d+)\.([A-Za-z0-9]+)\.(req|conf|error)$`)

// ChargerKey is a parsed routing key of a charger-originated frame.
type ChargerKey struct {
	Version, Action, Direction string
}

// ParseRoutingKey returns false for frames published by the CSMS, whose
// routing key is the charge point id.
func ParseRoutingKey(key string) (ChargerKey, bool) {
	m := chargerKey.FindStringSubmatch(key)
	if m == nil {
		return ChargerKey{}, false
	}
	return ChargerKey{Version: m[1], Action: m[2], Direction: m[3]}, true
}

// IsV2 tells the OCPP 2.x (ocpp20, ocpp201, ocpp21) payload family from 1.x.
func IsV2(version string) bool { return strings.HasPrefix(version, "ocpp2") }

// SecurityProfile extracts the profile the demo encodes in the charge point id (...-sp3).
func SecurityProfile(chargerID string) string {
	i := strings.LastIndex(chargerID, "-sp")
	if i < 0 || len(chargerID)-i != 4 || chargerID[i+3] < '0' || chargerID[i+3] > '9' {
		return ""
	}
	return chargerID[i+3:]
}

func KindName(messageType int) string {
	switch messageType {
	case Call:
		return "CALL"
	case CallResult:
		return "CALLRESULT"
	case CallError:
		return "CALLERROR"
	case CallResultError:
		return "CALLRESULTERROR"
	case Send:
		return "SEND"
	}
	return "UNKNOWN"
}

// Frame is a decoded OCPP-J message: [type, id, ...].
type Frame []json.RawMessage

// Decode parses a frame and its first two elements.
func Decode(data []byte) (Frame, int, string, bool) {
	var f Frame
	if json.Unmarshal(data, &f) != nil || len(f) < 3 {
		return nil, 0, "", false
	}
	var messageType int
	var messageID string
	if json.Unmarshal(f[0], &messageType) != nil || json.Unmarshal(f[1], &messageID) != nil {
		return nil, 0, "", false
	}
	return f, messageType, messageID, true
}

// String decodes the string element at index i ("" when absent).
func (f Frame) String(i int) string {
	var s string
	if i < len(f) {
		_ = json.Unmarshal(f[i], &s)
	}
	return s
}

// Object decodes the object element at index i (empty when absent).
func (f Frame) Object(i int) map[string]any {
	m := map[string]any{}
	if i < len(f) {
		_ = json.Unmarshal(f[i], &m)
	}
	return m
}

// IsSyntheticOffline recognizes the StatusNotification the plugin publishes
// when a charger disconnects. It must not be answered: the charger is gone.
func IsSyntheticOffline(action string, payload map[string]any) bool {
	if action != "StatusNotification" {
		return false
	}
	source := payload
	if custom, ok := payload["customData"].(map[string]any); ok {
		source = custom
	}
	return source["vendorId"] == "rabbitmq" && source["vendorErrorCode"] == "Offline"
}

func Now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

var transactionIDs atomic.Int64

// Reply builds the complete CALLRESULT or CALLERROR frame for a charger CALL.
func Reply(version, messageID, action string, payload map[string]any, heartbeatInterval int) []any {
	accepted := map[string]any{"status": "Accepted"}
	var result map[string]any
	switch action {
	case "BootNotification":
		result = map[string]any{"status": "Accepted", "currentTime": Now(), "interval": heartbeatInterval}
	case "Heartbeat":
		result = map[string]any{"currentTime": Now()}
	case "Authorize":
		if IsV2(version) {
			result = map[string]any{"idTokenInfo": accepted}
		} else {
			result = map[string]any{"idTagInfo": accepted}
		}
	case "StartTransaction":
		result = map[string]any{"transactionId": transactionIDs.Add(1), "idTagInfo": accepted}
	case "StopTransaction":
		result = map[string]any{"idTagInfo": accepted}
	case "TransactionEvent":
		result = map[string]any{}
		if _, ok := payload["idToken"]; ok {
			result["idTokenInfo"] = accepted
		}
	case "DataTransfer":
		result = accepted
	case "StatusNotification", "MeterValues", "DiagnosticsStatusNotification", "FirmwareStatusNotification",
		"SecurityEventNotification", "LogStatusNotification", "NotifyReport", "NotifyEvent",
		"NotifyMonitoringReport":
		result = map[string]any{}
	default:
		return []any{CallError, messageID, "NotImplemented",
			fmt.Sprintf("Action %s is not supported by this CSMS", action), map[string]any{}}
	}
	return []any{CallResult, messageID, result}
}
