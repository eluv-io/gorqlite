package gorqlite

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestQueryOne(t *testing.T) {
	var wr WriteResult
	var qr QueryResult
	var wResults []WriteResult
	var qResults []QueryResult
	var err error

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)
	_ = started

	t.Logf("trying WriteOne CREATE")
	// note: DATETIME DEFAULT CURRENT_TIMESTAMP does not work
	wr, err = conn.WriteOne("CREATE TABLE " + testTableName() + " (id integer, name text, ts INT_DATETIME DEFAULT CURRENT_TIMESTAMP)")
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)
	met := fmt.Sprint(meeting.Unix())

	t.Logf("trying Write INSERT")
	s := make([]string, 0)
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 1, 'Romulan')")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 2, 'Vulcan')")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 3, 'Klingon')")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name) VALUES ( 4, 'Ferengi')")
	s = append(s, "INSERT INTO "+testTableName()+" (id, name, ts) VALUES ( 5, 'Cardassian', "+met+")")
	wResults, err = conn.Write(s)
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying QueryOne")
	qr, err = conn.QueryOne("SELECT name, ts FROM " + testTableName() + " WHERE id > 3")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Map()")
	r, err := qr.Map()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if r["name"].(string) != "Ferengi" {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if mts, ok := r["ts"]; ok {
		if ts, ok := mts.(time.Time); ok {
			// time should not be zero because it defaults to current utc time
			if ts.IsZero() {
				t.Logf("--> FAILED: time is zero")
				t.Fail()
			} else if ts.Before(started) {
				t.Logf("--> FAILED: time %q is before start %q", ts, started)
				t.Fail()
			}
		} else {
			t.Logf("--> FAILED: ts is a real %T", mts)
			t.Fail()
		}
	} else {
		t.Logf("--> FAILED: ts not found")
	}

	t.Logf("trying Scan(), also float64->int64 in Scan()")
	var id int64
	var name string
	var ts time.Time
	err = qr.Scan(&id, &name)
	if err == nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Ferengi" {
		t.Logf("--> FAILED, name should be 'Ferengi' but it's '%s'", name)
		t.Fail()
	}
	qr.Next()
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Cardassian" {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if ts.Unix() != meeting.Unix() {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Close")
	conn.Close()

	t.Logf("trying WriteOne after Close")
	wr, err = conn.WriteOne("DROP TABLE IF EXISTS " + testTableName() + "")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wr

	t.Logf("trying Write after Close")
	t1 := make([]string, 0)
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	t1 = append(t1, "DROP TABLE IF EXISTS "+testTableName()+"")
	wResults, err = conn.Write(t1)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = wResults

	t.Logf("trying QueryOne after Close")
	qr, err = conn.QueryOne("SELECT id FROM " + testTableName() + "")
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qr

	t.Logf("trying Query after Close")
	t2 := make([]string, 0)
	t2 = append(t2, "SELECT id FROM "+testTableName()+"")
	t2 = append(t2, "SELECT name FROM "+testTableName()+"")
	t2 = append(t2, "SELECT id,name FROM "+testTableName()+"")
	qResults, err = conn.Query(t2)
	if err == nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	_ = qResults

}

func TestQueries(t *testing.T) {

	t.Logf("trying Open")
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying WriteOne DROP")
	// note: this returns a syntax error 'near ?'
	//NewStatement("DROP TABLE IF EXISTS ? " , testTableName())
	wr, err := conn.WriteStmt(
		context.Background(),
		NewStatement("DROP TABLE IF EXISTS "+testTableName()))
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// give an extra second for time diff between local and rqlite
	started := time.Now().Add(-time.Second)

	t.Logf("trying WriteOne CREATE")
	// This error: near "?": syntax error
	//NewStatement("CREATE TABLE ? (id integer, name text, ts DATETIME DEFAULT CURRENT_TIMESTAMP)", testTableName())
	// NOTE: for some reason, creating the table with 'ts DATETIME DEFAULT CURRENT_TIMESTAMP'
	//       makes the ts column always be nil. As a consequence values returned by the Map()
	//       function are not parsed and returned as string.
	//       This used to work with rqlite master checked out on August 12
	//       This does not work any more Sept. 16 using a fresh checkout of master.
	wr, err = conn.WriteStmt(
		context.Background(),
		NewStatement("CREATE TABLE "+testTableName()+" (id integer, name text, ts INT_DATETIME DEFAULT CURRENT_TIMESTAMP)"))
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	// When the Federation met the Cardassians
	meeting := time.Date(2424, 1, 2, 17, 0, 0, 0, time.UTC)

	t.Logf("trying Write INSERT")
	s := make([]*Statement, 0)
	insert := "INSERT INTO " + testTableName() + " (id, name) VALUES ( ?, ? )"
	insertTs := "INSERT INTO " + testTableName() + " (id, name, ts) VALUES ( ?, ?, ?)"
	s = append(s, NewStatement(insert, 1, "Romulan"))
	s = append(s, NewStatement(insert, 2, "Vulcan"))
	s = append(s, NewStatement(insert, 3, "Klingon"))
	s = append(s, NewStatement(insert, 4, "Ferengi"))
	s = append(s, NewStatement(insertTs, 5, "Cardassian", meeting.Unix()))
	wResults, err := conn.WriteStmt(context.Background(), s...)
	if err != nil {
		t.Logf("--> FATAL")
		t.Fatal()
	}

	t.Logf("trying Queries")
	qrs, err := conn.QueryStmt(
		context.Background(),
		NewStatement("SELECT name, ts FROM "+testTableName()+" WHERE id > 3", 3))
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if len(qrs) != 1 {
		t.Logf("--> FAILED. Expected one result")
		t.Fail()
	}
	qr := qrs[0]

	t.Logf("trying Next()")
	na := qr.Next()
	if na != true {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Map()")
	r, err := qr.Map()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if r["name"].(string) != "Ferengi" {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if mts, ok := r["ts"]; ok {
		if ts, ok := mts.(time.Time); ok {
			// time should not be zero because it defaults to current utc time
			if ts.IsZero() {
				t.Logf("--> FAILED: time is zero")
				t.Fail()
			} else if ts.Before(started) {
				t.Logf("--> FAILED: time %q is before start %q", ts, started)
				t.Fail()
			}
		} else {
			t.Logf("--> FAILED: ts is a real %T", mts)
			t.Fail()
		}
	} else {
		t.Logf("--> FAILED: ts not found")
	}

	t.Logf("trying Scan(), also float64->int64 in Scan()")
	var id int64
	var name string
	var ts time.Time
	err = qr.Scan(&id, &name)
	if err == nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Ferengi" {
		t.Logf("--> FAILED, name should be 'Ferengi' but it's '%s'", name)
		t.Fail()
	}
	qr.Next()
	err = qr.Scan(&name, &ts)
	if err != nil {
		t.Logf("--> FAILED (%s)", err.Error())
		t.Fail()
	}
	if name != "Cardassian" {
		t.Logf("--> FAILED")
		t.Fail()
	}
	if ts.Unix() != meeting.Unix() {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying WriteOne DROP")
	wr, err = conn.WriteStmt(
		context.Background(),
		NewStatement("DROP TABLE IF EXISTS "+testTableName()))
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Close")
	conn.Close()

	t.Logf("trying WriteOne after Close")
	del := NewStatement("DROP TABLE IF EXISTS " + testTableName())
	wr, err = conn.WriteStmt(context.Background(), del)
	if err != errClosed {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Write after Close")
	t1 := []*Statement{del, del}
	wResults, err = conn.WriteStmt(context.Background(), t1...)
	if err != errClosed {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Queries after Close")
	_, err = conn.QueryStmt(
		context.Background(),
		NewStatement("SELECT id FROM ?", testTableName()))
	if err != errClosed {
		t.Logf("--> FAILED")
		t.Fail()
	}

	t.Logf("trying Query after Close")
	t2 := []*Statement{
		NewStatement("SELECT id FROM ?", testTableName()),
		NewStatement("SELECT name FROM ?", testTableName()),
		NewStatement("SELECT id,name FROM ?", testTableName()),
	}
	_, err = conn.QueryStmt(context.Background(), t2...)
	if err != errClosed {
		t.Logf("--> FAILED")
		t.Fail()
	}

	_ = wResults
	_ = wr
}
