package gopg_copybinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/go-pg/pg/v10"
	"github.com/lib/pq"
)

var (
	/// PGCOPY\n\377\r\n\0 11 bites
	pgCopySign = []byte{0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00}

	// -1 for pg
	nilField = []byte{0xFF, 0xFF, 0xFF, 0xFF}

	EofSign = []byte{0xFF, 0xFF}

	extendHeaderPg = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)
var errNilPtr = errors.New("destination pointer is nil") // embedded in descriptive error

func IsNullField(b []byte) bool {
	return bytes.Equal(b, nilField)
}

// CopyRead from db to writer and read external Reader.
func CopyRead(db *pg.Tx, query string, row func([][]byte) error, params ...interface{}) error {
	buff := new(bytes.Buffer)

	defer func() {
		if err := recover(); err != nil {
			log.Print("fatal in db.CopyTo")
		}
	}()
	_, err := db.CopyTo(buff, fmt.Sprintf("COPY %s TO STDOUT WITH (FORMAT binary)", query), params...)
	if err != nil {
		return err
	}

	// sing 11 bytes and 8 bytes extenders
	header := buff.Next(19)

	if !bytes.HasPrefix(header, pgCopySign) {
		return err
	}

	// read rows
	var (
		numberFields = make([]byte, 2)
		lenFiend     = make([]byte, 4) // len in bytes
	)
	for {
		n, err := buff.Read(numberFields)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			return errors.New("not need 0")
		}

		if bytes.Equal(numberFields, EofSign) {
			// done, we read all copy
			break
		}

		count := int(binary.BigEndian.Uint16(numberFields))
		fields := make([][]byte, count)

		for i := 0; i < count; i++ {
			n, err := buff.Read(lenFiend)
			if err != nil && err != io.EOF {
				return err
			}
			if n == 0 {
				return errors.New("0 reading")
			}

			if IsNullField(lenFiend) {
				// -1
				// fields[i] = nil ?
				continue
			}

			s := int(binary.BigEndian.Uint32(lenFiend))
			field := make([]byte, s)

			n, err = buff.Read(field)
			if err != nil && err != io.EOF {
				return err
			}
			if n == 0 {
				//chErr <- fmt.Errorf("we must be reading %d-bytes", s)
				return err
			}
			fields[i] = field
		}

		if err := row(fields); err != nil {
			return err
		}
	}

	return nil
}

func CopyWriteFromReader(db *pg.Tx, buf *bytes.Buffer, table string, columns []string) error {
	stmt := pq.QuoteIdentifier(table) + " ("
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += pq.QuoteIdentifier(col)
	}
	stmt += ") "
	inBuf := new(bytes.Buffer)
	inBuf.Write(pgCopySign)
	inBuf.Write(extendHeaderPg)
	_, err := inBuf.ReadFrom(buf)
	if err != nil {
		return fmt.Errorf("error ReadFrom %w", err)
	}
	inBuf.Write(EofSign)

	_, err = db.CopyFrom(inBuf, fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT binary)", stmt))
	return err
}
