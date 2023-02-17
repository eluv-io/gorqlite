package gorqlite

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// import "os"

func TestWriteOne(t *testing.T) {
	var wr WriteResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne CTHULHU (should fail, bad SQL)")
	wr, err = conn.WriteOne("CTHULHU")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne CREATE")
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text)")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne INSERT")
	wr, err = conn.WriteOne("INSERT INTO " + testTableName() + " (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("checking WriteOne RowsAffected")
	if wr.RowsAffected != 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

}

func TestWrite(t *testing.T) {
	var results []WriteResult
	var err error
	var s []string

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying Write DROP & CREATE")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	s = append(s, "CREATE TABLE "+testTableName()+" (id integer, name text)")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write INSERT")
	s = make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'aaa bbb ccc' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'ddd eee fff' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'ggg hhh iii' )")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'jjj kkk lll' )")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write DROP")
	s = make([]string, 0)
	s = append(s, "DROP TABLE IF EXISTS "+testTableName()+"")
	results, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

}

func TestWrites(t *testing.T) {
	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	t.Logf("trying Write DROP & CREATE")
	results, err := conn.Write([]string{
		"DROP TABLE IF EXISTS " + testTableName() + "",
		"CREATE TABLE " + testTableName() + " (id integer, name text)",
	})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write INSERT")
	insert := "INSERT INTO " + testTableName() + " (id, name) VALUES ( ?, ? )"
	s := make([]*Statement, 0)
	s = append(s, NewStatement(insert, 1, "aaa bbb ccc"))
	s = append(s, NewStatement(insert, 2, "ddd eee fff"))
	s = append(s, NewStatement(insert, 3, "ggg hhh iii"))
	s = append(s, NewStatement(insert, 4, "jjj kkk lll"))
	results, err = conn.WriteStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(results) != 4 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write DROP")
	results, err = conn.Write([]string{"DROP TABLE IF EXISTS " + testTableName()})
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

}

func TestReadWriteLargeNumbers(t *testing.T) {
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal(err)
	}

	_, err = conn.Write([]string{
		"DROP TABLE IF EXISTS " + testTableName() + "",
		"CREATE TABLE " + testTableName() + " (id integer, nanos integer, length double, name text)",
	})
	if err != nil {
		t.Logf("--> CREATE TABLE FAILED %v", err)
		t.Fatal(err)
	}

	type thing struct {
		id     int64
		nanos  int64
		length float64
		name   string
	}

	now := time.Now().UnixNano()
	makeLen := func(n int64) float64 {
		return float64(n)/10000.0 + 0.45
	}
	toInsert := []*thing{
		{id: 1, nanos: now + 1, length: makeLen(now) + 0.1, name: "aaa"},
		{id: 2, nanos: now + 2, length: makeLen(now) + 0.2, name: "bbb"},
		{id: 3, nanos: now + 3, length: makeLen(now) + 0.3, name: "ccc"},
		{id: 4, nanos: now + 4, length: makeLen(now) + 0.4, name: "ddd"},
	}

	t.Logf("trying Write INSERT")
	insert := "INSERT INTO " + testTableName() + " (id, nanos, length, name) VALUES ( ?, ?, ?, ? )"
	s := make([]*Statement, 0)
	for _, ti := range toInsert {
		s = append(s, NewStatement(insert, ti.id, ti.nanos, ti.length, ti.name))
	}
	_, err = conn.WriteStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> INSERT FAILED %v", err)
		t.Fatal(err)
	}

	qrs, err := conn.QueryStmt(
		context.Background(),
		NewStatement("SELECT id, nanos, length, name FROM "+testTableName()))
	if err != nil {
		t.Logf("--> QUERY FAILED %v", err)
		t.Fatal(err)
	}
	if len(qrs) != 1 {
		t.Fatal("--> QUERY FAILED expected 1 result, got ", len(qrs))
	}
	qr := qrs[0]

	ret := make([]*thing, 0)
	for qr.Next() {
		s := &thing{}
		err = qr.Scan(&s.id, &s.nanos, &s.length, &s.name)
		if err != nil {
			t.Logf("--> SCAN FAILED %v", err)
			t.Fatal(err)
		}
		ret = append(ret, s)
	}
	if len(ret) != len(toInsert) {
		t.Fatal(fmt.Sprintf("--> QUERY FAILED expected %d things, got %d", len(toInsert), len(ret)))
	}
	for _, r := range ret {
		fmt.Println(r.id, r.nanos, fmt.Sprintf("%.3f", r.length), r.name)
	}
	for i, ti := range toInsert {
		if !reflect.DeepEqual(ti, ret[i]) {
			t.Fatal(fmt.Sprintf("--> expected equal %#v and %#v", ti, ret[i]))
		}
	}
}
