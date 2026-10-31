package vectra

// Cypher constraint-DDL end-to-end tests (2026-10-30 unique-constraints
// plan, Task 4): CREATE/DROP CONSTRAINT statements enter through the public
// (*Graph).Cypher entry, survive all FOUR routing layers (root prefilter,
// parser entry gate + clause switch, both classification lists, the exec
// stage), and drive the Task 2 declaration API. Error discipline (spec
// §2.6): writer-method errors propagate RAW out of the Cypher layer —
// never wrapped in a positioned *cypher.Error — so the violation payload
// stays reachable for errors.Is/errors.As (and, in Task 5, the server's
// 400 UNIQUE_VIOLATION face).

import (
	"errors"
	"strings"
	"testing"
)

// constraintDDL are the two grammar forms (spec §2.1: REQUIRE-form only, no
// name form — the (label, prop) pair is the identity).
const (
	constraintCreate = `CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`
	constraintDrop   = `DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`
)

// TestCypherConstraintCreateAndDrop pins the happy path through every
// layer: create declares (summary ConstraintsCreated = 1, the registry
// lists the pair, enforcement is live), drop removes it (counter 0,
// registry empty, enforcement stops).
func TestCypherConstraintCreateAndDrop(t *testing.T) {
	g := New()

	res, err := g.Cypher(constraintCreate)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if res.Summary == nil {
		t.Fatal("constraint DDL must carry a Summary")
	}
	if res.Summary.ConstraintsCreated != 1 || res.Summary.CreatedNodes != 0 ||
		res.Summary.IndexesCreated != 0 || res.Summary.PropertiesSet != 0 {
		t.Fatalf("create summary = %+v want ConstraintsCreated=1 only", res.Summary)
	}
	cs := g.UniqueConstraints()
	if len(cs) != 1 || cs[0] != (UniqueConstraint{Label: "Person", Prop: "email"}) {
		t.Fatalf("registry after create = %v", cs)
	}

	// enforcement is live through the Cypher write path
	if _, err := g.Cypher(`CREATE (n:Person {key:'a', email:'x@y'})`); err != nil {
		t.Fatalf("first owner: %v", err)
	}
	if _, err := g.Cypher(`CREATE (n:Person {key:'b', email:'x@y'})`); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate email must violate, got %v", err)
	}

	res, err = g.Cypher(constraintDrop)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if res.Summary == nil || res.Summary.ConstraintsCreated != 0 {
		t.Fatalf("drop summary = %+v want ConstraintsCreated=0", res.Summary)
	}
	if cs := g.UniqueConstraints(); len(cs) != 0 {
		t.Fatalf("registry after drop = %v", cs)
	}
	// enforcement stopped: the same write now succeeds
	if _, err := g.Cypher(`CREATE (n:Person {key:'b', email:'x@y'})`); err != nil {
		t.Fatalf("after drop the duplicate must pass: %v", err)
	}
}

// TestCypherConstraintDropRoutesThroughWritePath is the layer-1 pin: DROP
// CONSTRAINT spells none of the original write-clause keywords, so the
// root-package prefilter (writeClauseKeywords) must know "constraint" or
// the statement dies on the read path at the parser's entry gate
// ("expected a clause (MATCH, UNWIND, or a write clause), got DROP"). The
// direct prefilter probe plus the end-to-end drop cover layers 1-4.
func TestCypherConstraintDropRoutesThroughWritePath(t *testing.T) {
	g := New()
	if err := g.DeclareUnique("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if !maybeWriteQuery(constraintDrop) {
		t.Fatal("maybeWriteQuery(DROP CONSTRAINT ...) = false: the prefilter must route the DROP form to the write path")
	}
	if _, err := g.Cypher(constraintDrop); err != nil {
		t.Fatalf("drop through Cypher: %v", err)
	}
	if cs := g.UniqueConstraints(); len(cs) != 0 {
		t.Fatalf("registry after drop = %v", cs)
	}
}

// TestCypherConstraintDuplicateAndUnknown pins the declaration guards
// through the Cypher layer: a duplicate CREATE errors ErrConstraintExists
// and a DROP of an unknown pair errors ErrConstraintNotFound — both RAW
// (not positioned *cypher.Error, which would flip the server face to 400
// PARSE), both leaving no result.
func TestCypherConstraintDuplicateAndUnknown(t *testing.T) {
	g := New()
	if _, err := g.Cypher(constraintCreate); err != nil {
		t.Fatal(err)
	}
	_, err := g.Cypher(constraintCreate)
	if !errors.Is(err, ErrConstraintExists) {
		t.Fatalf("duplicate create = %v want ErrConstraintExists", err)
	}
	if _, ok := errors.AsType[*CypherError](err); ok {
		t.Fatalf("duplicate create must stay raw, got positioned %v", err)
	}
	if cs := g.UniqueConstraints(); len(cs) != 1 {
		t.Fatalf("duplicate create must not touch the registry: %v", cs)
	}

	_, err = g.Cypher(`DROP CONSTRAINT FOR (n:Person) REQUIRE n.ssn IS UNIQUE`)
	if !errors.Is(err, ErrConstraintNotFound) {
		t.Fatalf("drop unknown = %v want ErrConstraintNotFound", err)
	}
	if _, ok := errors.AsType[*CypherError](err); ok {
		t.Fatalf("drop unknown must stay raw, got positioned %v", err)
	}
}

// TestCypherConstraintDeclareOnDuplicateData pins the declaration-time
// validation through Cypher (spec §2.2): declaring over live data that
// already duplicates a (label, prop) string value fails the statement with
// the ErrUniqueViolation class, carries the deterministic (value, key)
// listing in the message, and registers nothing.
func TestCypherConstraintDeclareOnDuplicateData(t *testing.T) {
	g := New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "Person", map[string]any{"email": "a@x"}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := g.Cypher(constraintCreate)
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("declare on duplicates = %v want ErrUniqueViolation", err)
	}
	if _, ok := errors.AsType[*CypherError](err); ok {
		t.Fatalf("declaration failure must stay raw, got positioned %v", err)
	}
	for _, want := range []string{"duplicate values", `"a@x" owned by "a"`, `"a@x" owned by "b"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("declaration error %q missing listing part %q", err.Error(), want)
		}
	}
	if cs := g.UniqueConstraints(); len(cs) != 0 {
		t.Fatalf("failed declaration must register nothing: %v", cs)
	}
	// the duplicate nodes themselves are untouched
	for _, k := range []string{"a", "b"} {
		if _, ok := g.NodeByKey(k); !ok {
			t.Fatalf("node %s must survive the failed declaration", k)
		}
	}
}

// TestCypherConstraintViolationPassthrough pins the §2.6 error discipline
// end-to-end: a write-time violation surfacing through a Cypher statement
// keeps its *UniqueViolationError payload (errors.Is class + errors.As
// fields) and is NOT wrapped in a positioned *cypher.Error; the rejected
// statement leaves nothing behind.
func TestCypherConstraintViolationPassthrough(t *testing.T) {
	g := New()
	if _, err := g.Cypher(constraintCreate); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`CREATE (n:Person {key:'a', email:'a@x'})`); err != nil {
		t.Fatal(err)
	}
	_, err := g.Cypher(`CREATE (n:Person {key:'b', email:'a@x'})`)
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("violation = %v want ErrUniqueViolation", err)
	}
	uv, ok := errors.AsType[*UniqueViolationError](err)
	if !ok {
		t.Fatalf("violation payload lost through Cypher: %v", err)
	}
	if uv.Label != "Person" || uv.Prop != "email" || uv.Value != "a@x" ||
		uv.ExistingKey != "a" || uv.AttemptedKey != "b" {
		t.Fatalf("violation payload = %+v", uv)
	}
	if _, ok := errors.AsType[*CypherError](err); ok {
		t.Fatalf("violation must stay raw (a wrap would flip the server face to 400 PARSE): %v", err)
	}
	// zero side effects: the rejected node never existed
	if _, ok := g.NodeByKey("b"); ok {
		t.Fatal("rejected CREATE must leave no node behind")
	}
	// first-error-stops inside one statement: the row before the violation
	// stays, the violating row and everything after it never run (the row
	// value doubles as the email, so row 2 collides with seed's email)
	if _, err := g.AddNode("seed", "Person", map[string]any{"email": "dup"}); err != nil {
		t.Fatal(err)
	}
	_, err = g.Cypher(`UNWIND ['ok', 'dup', 'later'] AS k CREATE (n:Person {key:k, email:k})`)
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bulk create violation = %v", err)
	}
	uv, ok = errors.AsType[*UniqueViolationError](err)
	if !ok || uv.ExistingKey != "seed" || uv.AttemptedKey != "dup" || uv.Value != "dup" {
		t.Fatalf("bulk violation payload = %+v", uv)
	}
	if _, ok := g.NodeByKey("ok"); !ok {
		t.Fatal("row before the violation must be durable")
	}
	for _, k := range []string{"dup", "later"} {
		if _, ok := g.NodeByKey(k); ok {
			t.Fatalf("node %s must not exist after the violation", k)
		}
	}
}
