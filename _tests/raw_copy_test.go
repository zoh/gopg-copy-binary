package raw_copy_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	gopg_copybinary "github.com/zoh/gopg-copy-binary"
)

var url = os.Getenv("DATABASE_URL_TEST")

func init() {
	if url == "" {
		url = "postgres://postgres:mysecretpassword@localhost:5432/test?sslmode=disable"
	}
}

func createConnectionDB() (*pg.DB, error) {
	opt, err := pg.ParseURL(url)
	if err != nil {
		return nil, err
	}
	db := pg.Connect(opt)

	_, err = db.Exec("select 1;")
	if err != nil {
		return nil, err
	}

	return db, nil
}

type attribute struct {
	ID          int
	UUID        uuid.UUID
	Name        string
	Omit        *string
	Description string
	Active      bool
	JsVals      []byte `json:"js_vals"`
	CreatedAt   time.Time
}

func TestCopyRead(t *testing.T) {
	db, err := createConnectionDB()
	assert.NoError(t, err)
	defer db.Close()

	tx, _ := db.Begin()
	defer tx.Rollback()

	_, err = tx.Exec(`create temp table attributes (
		id 			serial,
		uuid 		uuid,
		name 		varchar(40) not null,
		omit		text ,
		description text,
		active		boolean,
		js_vals     jsonb,
		created_at	timestamp default now()
	);
	
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	`)
	assert.NoError(t, err)

	// generate some data
	_, err = tx.Exec(`insert into attributes (
		uuid, name, description, omit, active, js_vals
	)
	select
		uuid_generate_v4(),
		left(md5(i::text), 10),
		md5(random()::text),
		md5(random()::text),
		random() > 0.5,
		('{"firstName":"' ||  md5(random()::text) || '", "lastName":"' || md5(random()::text) ||'"}')::jsonb
	from generate_series(1, 100000) s(i)`)
	assert.NoError(t, err)
	log.Println("generated 100000 rows, now read")
	// gopg_copybinary.CopyRead(tx, "attributes")

	defer gopg_copybinary.Duration(gopg_copybinary.Track("copy read"))

	elements := make([]attribute, 0, 100000)
	err = gopg_copybinary.CopyRead(tx, "( select id, uuid, name, omit, description, active, js_vals, created_at::text from attributes )", func(cols [][]byte) error {
		var element attribute
		if err := gopg_copybinary.ScanElement(cols,
			&element.ID,
			&element.UUID,
			&element.Name,
			&element.Omit,
			&element.Description,
			&element.Active,
			&element.JsVals,
			&element.CreatedAt,
		); err != nil {
			return err
		}

		elements = append(elements, element)
		return nil
	})
	assert.NoError(t, err)

	assert.Len(t, elements, 100000)

	fmt.Printf("%+v \n", *elements[5].Omit)
}

func TestCopyWrite(t *testing.T) {
	db, err := createConnectionDB()
	assert.NoError(t, err)
	defer db.Close()

	tx, _ := db.Begin()
	defer tx.Rollback()

	_, err = tx.Exec(`create temp table attributes (
			id 			serial,
			uuid 		uuid,
			name 		varchar(40) not null,
			omit		text,
			description text,
			active		boolean,
			js_vals     jsonb,
			created_at	timestamp default now()
		);
		`)
	assert.NoError(t, err)

	defer gopg_copybinary.Duration(gopg_copybinary.Track("copy write"))

	var buffer = new(bytes.Buffer)
	for i := 0; i < 100000; i++ {
		b, err := gopg_copybinary.ValueElement(
			i,
			uuid.New(),
			"test",
			"omitted text",
			"desc",
			i%2 == 0,
			// []byte(`{}`),
			// time.Now().String,
		)
		if err != nil {
			t.Fatal(err)
		}
		buffer.Write(b)
	}

	columns := strings.Split("id, uuid, name, omit, description, active", ", ")
	err = gopg_copybinary.CopyWriteFromReader(tx, buffer, "attributes", columns)
	assert.NoError(t, err)

	var count int
	_, err = tx.QueryOne(pg.Scan(&count), "select count(*) from attributes;")
	assert.NoError(t, err)
	assert.Equal(t, count, 100000)
}
