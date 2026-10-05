// Command provision generates everything the demo needs before the broker
// starts: the charger fleet (chargers.csv), the RabbitMQ definitions with one
// user per charger, and a small PKI for the TLS-based security profiles.
//
// Output layout (under -out):
//
//	chargers.csv                 id,ocppVersion,securityProfile,password
//	definitions.json             vhosts, users, permissions, queues, bindings
//	pki/ca.crt                   demo root CA (trusted by the broker for mTLS)
//	pki/server.crt, server.key   broker certificate for the wss:// listener
//	pki/clients/<id>.crt|.key    client certificates for Security Profile 3
//	params.json                  parameters used, to skip identical re-runs
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// schema changes whenever the generated output changes for the same
// parameters, so that existing ./generated directories are refreshed.
const schema = 3

type params struct {
	Schema     int      `json:"schema"`
	Chargers   int      `json:"chargers"`
	Ocpp21Pct  int      `json:"ocpp21Pct"`
	SP1Pct     int      `json:"sp1Pct"`
	SP2Pct     int      `json:"sp2Pct"`
	SP3Pct     int      `json:"sp3Pct"`
	Vhosts     []string `json:"vhosts"`
	CsmsUser   string   `json:"csmsUser"`
	CsmsPass   string   `json:"csmsPass"`
	ServerSANs []string `json:"serverSans"`
	Seed       uint64   `json:"seed"`
}

type charger struct {
	ID       string
	Version  string // "1.6" or "2.1"
	Profile  int    // OCPP security profile 1, 2 or 3
	Password string // empty for profile 3 (client certificate)
}

func main() {
	p := params{Schema: schema}
	var vhosts, sans string
	out := flag.String("out", envOr("OUT_DIR", "generated"), "output directory")
	force := flag.Bool("force", false, "regenerate even if parameters are unchanged")
	flag.IntVar(&p.Chargers, "chargers", envInt("CHARGERS", 10000), "number of charge points")
	flag.IntVar(&p.Ocpp21Pct, "ocpp21-pct", envInt("OCPP21_PCT", 50), "percentage of OCPP 2.1 chargers (rest is 1.6)")
	flag.IntVar(&p.SP1Pct, "sp1-pct", envInt("SP1_PCT", 40), "percentage on Security Profile 1 (ws:// + Basic auth)")
	flag.IntVar(&p.SP2Pct, "sp2-pct", envInt("SP2_PCT", 30), "percentage on Security Profile 2 (wss:// + Basic auth)")
	flag.IntVar(&p.SP3Pct, "sp3-pct", envInt("SP3_PCT", 30), "percentage on Security Profile 3 (wss:// + client certificate)")
	flag.StringVar(&vhosts, "vhosts", envOr("VHOSTS", "csms-java,csms-rust,csms-go"), "comma separated vhosts, one per CSMS implementation")
	flag.StringVar(&p.CsmsUser, "csms-user", envOr("CSMS_USER", "csms"), "backend user")
	flag.StringVar(&p.CsmsPass, "csms-pass", envOr("CSMS_PASS", "csms"), "backend password")
	flag.StringVar(&sans, "server-sans", envOr("SERVER_SANS", "rabbitmq,localhost,127.0.0.1"), "broker certificate subject alternative names")
	flag.Uint64Var(&p.Seed, "seed", uint64(envInt("SEED", 42)), "seed for the fleet mix")
	flag.Parse()
	p.Vhosts = splitList(vhosts)
	p.ServerSANs = splitList(sans)

	if p.SP1Pct+p.SP2Pct+p.SP3Pct != 100 {
		log.Fatalf("security profile percentages must add up to 100, got %d", p.SP1Pct+p.SP2Pct+p.SP3Pct)
	}
	if !*force && unchanged(*out, p) {
		log.Printf("%s is up to date (%d chargers), nothing to do", *out, p.Chargers)
		return
	}

	start := time.Now()
	must(os.MkdirAll(filepath.Join(*out, "pki", "clients"), 0o755))
	fleet := buildFleet(p)
	must(writeCSV(filepath.Join(*out, "chargers.csv"), fleet))
	must(writeDefinitions(filepath.Join(*out, "definitions.json"), p, fleet))
	must(writePKI(filepath.Join(*out, "pki"), p, fleet))
	must(writeJSON(filepath.Join(*out, "params.json"), p))

	counts := map[string]int{}
	for _, c := range fleet {
		counts["ocpp"+c.Version]++
		counts["SP"+strconv.Itoa(c.Profile)]++
	}
	log.Printf("generated %d chargers %v in %s", len(fleet), counts, time.Since(start).Round(time.Millisecond))
}

// buildFleet assigns version and security profile to every charger. The
// shuffle is seeded so the same parameters always yield the same fleet.
func buildFleet(p params) []charger {
	rng := mrand.New(mrand.NewPCG(p.Seed, p.Seed^0x9e3779b97f4a7c15))
	versions := spread(p.Chargers, []int{100 - p.Ocpp21Pct, p.Ocpp21Pct}, rng)
	profiles := spread(p.Chargers, []int{p.SP1Pct, p.SP2Pct, p.SP3Pct}, rng)

	fleet := make([]charger, p.Chargers)
	for i := range fleet {
		v := []string{"1.6", "2.1"}[versions[i]]
		sp := profiles[i] + 1
		c := charger{
			// The id encodes version and profile so dashboards can break the
			// fleet down without any side channel.
			ID:      fmt.Sprintf("cp%05d-v%s-sp%d", i+1, strings.ReplaceAll(v, ".", ""), sp),
			Version: v,
			Profile: sp,
		}
		if sp != 3 {
			// OCPP 1.6 security whitepaper / 2.x: AuthorizationKey of 20 bytes, hex encoded.
			c.Password = randomHex(20)
		}
		fleet[i] = c
	}
	return fleet
}

// spread returns n bucket indexes distributed according to the percentages
// and shuffled, so every bucket is represented evenly over time while ramping.
func spread(n int, pcts []int, rng *mrand.Rand) []int {
	out := make([]int, 0, n)
	for b, pct := range pcts {
		count := n * pct / 100
		if b == len(pcts)-1 {
			count = n - len(out)
		}
		for range count {
			out = append(out, b)
		}
	}
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func writeCSV(path string, fleet []charger) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"id", "ocppVersion", "securityProfile", "password"})
	for _, c := range fleet {
		_ = w.Write([]string{c.ID, c.Version, strconv.Itoa(c.Profile), c.Password})
	}
	w.Flush()
	return w.Error()
}

// writeDefinitions produces a RabbitMQ definitions file imported at boot.
//
// Every charger gets its own user (username == charge point id, as OCPP
// requires for Basic auth and as the plugin requires for the certificate CN).
// Users on the TLS profiles are tagged "tlsonly" so the plugin refuses them on
// the plain ws:// listener (no security profile downgrade).
//
// Topic permissions pin each charger to its own identity: it may only bind
// its queue with its own id as routing key, and may only publish OCPP style
// routing keys.
func writeDefinitions(path string, p params, fleet []charger) error {
	type m = map[string]any
	users := []m{{
		"name":              p.CsmsUser,
		"password_hash":     hashPassword(p.CsmsPass),
		"hashing_algorithm": "rabbit_password_hashing_sha256",
		"tags":              []string{"administrator"},
	}}
	var vhosts, perms, topicPerms, queues, bindings []m
	for _, vh := range p.Vhosts {
		vhosts = append(vhosts, m{"name": vh})
		perms = append(perms, m{"user": p.CsmsUser, "vhost": vh, "configure": ".*", "write": ".*", "read": ".*"})
		// Shared work queues, consumed by any number of CSMS instances. Every
		// implementation declares them identically on startup. They are
		// durable classic queues: the plugin publishes into them directly, and
		// it does not yet keep the client state quorum queues need.
		for _, q := range []string{"csms.requests", "csms.responses"} {
			queues = append(queues, m{"name": q, "vhost": vh, "durable": true, "auto_delete": false,
				"arguments": m{}})
		}
		for _, b := range [][2]string{
			{"csms.requests", "*.*.req"},          // charger-initiated CALLs, for the workers
			{"csms.responses", "*.response.conf"}, // charger answers to CSMS commands, for the APIs
			{"csms.responses", "*.response.error"},
		} {
			bindings = append(bindings, m{
				"source": "amq.topic", "vhost": vh, "destination": b[0],
				"destination_type": "queue", "routing_key": b[1], "arguments": m{},
			})
		}
	}
	for _, c := range fleet {
		u := m{"name": c.ID, "hashing_algorithm": "rabbit_password_hashing_sha256", "password_hash": "", "tags": []string{}}
		if c.Password != "" {
			u["password_hash"] = hashPassword(c.Password)
		}
		if c.Profile >= 2 {
			u["tags"] = []string{"tlsonly"}
		}
		users = append(users, u)
		for _, vh := range p.Vhosts {
			perms = append(perms, m{
				"user": c.ID, "vhost": vh,
				"configure": `^ocpp\.`, "write": `^(ocpp\..*|amq\.topic)$`, "read": `^(ocpp\..*|amq\.topic)$`,
			})
			topicPerms = append(topicPerms, m{
				"user": c.ID, "vhost": vh, "exchange": "amq.topic",
				"write": `^ocpp[0-9]+\.[A-Za-z0-9]+\.(req|conf|error)$`, "read": `^{client_id}$`,
			})
		}
	}
	return writeJSON(path, m{
		"rabbit_version":    "4.3.1",
		"users":             users,
		"vhosts":            vhosts,
		"permissions":       perms,
		"topic_permissions": topicPerms,
		"queues":            queues,
		"bindings":          bindings,
		"exchanges":         []m{},
		"policies":          []m{},
		"parameters":        []m{},
		"global_parameters": []m{},
	})
}

// hashPassword implements rabbit_password_hashing_sha256:
// base64(salt ++ sha256(salt ++ password)) with a 4 byte salt.
func hashPassword(pw string) string {
	salt := make([]byte, 4)
	_, _ = rand.Read(salt)
	sum := sha256.Sum256(append(append([]byte{}, salt...), pw...))
	return base64.StdEncoding.EncodeToString(append(salt, sum[:]...))
}

func writePKI(dir string, p params, fleet []charger) error {
	caKey, caCert, err := newCA()
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, "ca.crt"), "CERTIFICATE", caCert.Raw); err != nil {
		return err
	}
	if err := writeKey(filepath.Join(dir, "ca.key"), caKey); err != nil {
		return err
	}

	srvTmpl := leafTemplate("rabbitmq", x509.ExtKeyUsageServerAuth)
	for _, san := range p.ServerSANs {
		if ip := net.ParseIP(san); ip != nil {
			srvTmpl.IPAddresses = append(srvTmpl.IPAddresses, ip)
		} else {
			srvTmpl.DNSNames = append(srvTmpl.DNSNames, san)
		}
	}
	if err := issue(dir, "server", srvTmpl, caCert, caKey); err != nil {
		return err
	}

	clients := filepath.Join(dir, "clients")
	old, _ := filepath.Glob(filepath.Join(clients, "*"))
	for _, f := range old {
		_ = os.Remove(f)
	}
	// ECDSA P-256 keys are cheap, but thousands of them still benefit from all cores.
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	sem := make(chan struct{}, 16)
	for _, c := range fleet {
		if c.Profile != 3 {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer func() { <-sem; wg.Done() }()
			// The plugin is configured with ssl_cert_login_from = common_name,
			// and requires the CN to equal the charge point id in the URL.
			if err := issue(clients, id, leafTemplate(id, x509.ExtKeyUsageClientAuth), caCert, caKey); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}(c.ID)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func newCA() (*ecdsa.PrivateKey, *x509.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "OCPP Polyglot Demo CA", Organization: []string{"rabbitmq-web-ocpp demo"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(5, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	return key, cert, err
}

func leafTemplate(cn string, usage x509.ExtKeyUsage) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"rabbitmq-web-ocpp demo"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(2, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
}

func issue(dir, name string, tmpl, ca *x509.Certificate, caKey *ecdsa.PrivateKey) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, name+".crt"), "CERTIFICATE", der); err != nil {
		return err
	}
	return writeKey(filepath.Join(dir, name+".key"), key)
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	// World readable on purpose: the broker container runs as a different
	// user than the one generating the files. Demo material only.
	return writePEM(path, "PRIVATE KEY", der)
}

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o644)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func unchanged(out string, p params) bool {
	b, err := os.ReadFile(filepath.Join(out, "params.json"))
	if err != nil {
		return false
	}
	var prev params
	if json.Unmarshal(b, &prev) != nil {
		return false
	}
	return reflect.DeepEqual(prev, p)
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
