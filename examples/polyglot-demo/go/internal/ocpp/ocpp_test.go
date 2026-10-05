package ocpp

import "testing"

func TestParseRoutingKey(t *testing.T) {
	k, ok := ParseRoutingKey("ocpp201.BootNotification.req")
	if !ok || k.Version != "ocpp201" || k.Action != "BootNotification" || k.Direction != "req" {
		t.Fatalf("unexpected %+v %v", k, ok)
	}
	for _, key := range []string{"cp00001-v16-sp1", "ocpp16.a.b.req", "ocpp16.Heartbeat.other"} {
		if _, ok := ParseRoutingKey(key); ok {
			t.Errorf("%s must not parse", key)
		}
	}
}

func TestSecurityProfileAndOffline(t *testing.T) {
	if SecurityProfile("cp00001-v16-sp3") != "3" || SecurityProfile("charger") != "" {
		t.Fatal("security profile")
	}
	v2 := map[string]any{"customData": map[string]any{"vendorId": "rabbitmq", "vendorErrorCode": "Offline"}}
	if !IsSyntheticOffline("StatusNotification", v2) || IsSyntheticOffline("StatusNotification", map[string]any{}) {
		t.Fatal("synthetic offline")
	}
}

func TestReply(t *testing.T) {
	r := Reply("ocpp21", "m1", "BootNotification", nil, 60)
	if r[0] != CallResult || r[2].(map[string]any)["interval"] != 60 {
		t.Fatalf("boot reply %v", r)
	}
	if e := Reply("ocpp16", "m2", "Bogus", nil, 60); e[0] != CallError || e[2] != "NotImplemented" {
		t.Fatalf("error reply %v", e)
	}
	if n := Now(); len(n) != 24 || n[23] != 'Z' {
		t.Fatalf("timestamp %s", n)
	}
}
