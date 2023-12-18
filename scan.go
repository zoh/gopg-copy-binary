package gopg_copybinary

import (
	"bytes"
	"database/sql"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/go-pg/pg/v10/types"
)

// ScanElement
func ScanElement(cols [][]byte, dest ...interface{}) error {
	if len(cols) != len(dest) {
		return fmt.Errorf("sql: expected %d destination arguments in ScanElement, not %d", len(cols), len(dest))
	}
	for i, col := range cols {
		err := convertAssignRows(dest[i], col)
		if err != nil {
			return fmt.Errorf("error convert col: %d %v", i, err)
		}
	}
	return nil
}

func convertAssignRows(dest interface{}, col []byte) error {
	switch d := dest.(type) {
	case *string:
		*d = asString(col)
		return nil
	case *[]byte:
		*d = col[:]
		return nil
	case *sql.RawBytes:
		*d = sql.RawBytes(col[:])
		return nil
	case *bool:
		*d = bytes.Equal(col, []byte{0x01})
		return nil

	case *time.Time:
		t, err := types.ParseTime(col)
		if err != nil {
			return err
		}
		*d = t
		return nil
	case *interface{}:
		*d = col
		return nil
	}

	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(col)
	}

	if b, ok := dest.(encoding.BinaryUnmarshaler); ok {
		fmt.Println(string(col), col)
		return b.UnmarshalBinary(col)
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	sv := reflect.ValueOf(col)
	dv := reflect.Indirect(dpv)
	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		// string only
		dv.Set(sv)
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	// The following conversions use a string value as an intermediate representation
	// to convert between various numeric types.
	//
	// This also allows scanning into user defined types such as "type Int int64".
	// For symmetry, also check for string destination types.
	switch dv.Kind() {
	case reflect.Ptr:
		if len(col) == 0 {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		}
		dv.Set(reflect.New(dv.Type().Elem()))
		return convertAssignRows(dv.Interface(), col)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(col) == 0 {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		var i64 int64
		switch len(col) {
		case 2:
			u16 := binary.BigEndian.Uint16(col)
			i64 = int64(u16)
		case 4:
			u16 := binary.BigEndian.Uint32(col)
			i64 = int64(u16)
		case 8:
			u16 := binary.BigEndian.Uint64(col)
			i64 = int64(u16)
		default:
			return fmt.Errorf("undefined Int type for bytes size = %d", len(col))
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if len(col) == 0 {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}

		var u64 uint64
		switch len(col) {
		case 2:
			u16 := binary.BigEndian.Uint16(col)
			u64 = uint64(u16)
		case 4:
			u16 := binary.BigEndian.Uint32(col)
			u64 = uint64(u16)
		case 8:
			u16 := binary.BigEndian.Uint64(col)
			u64 = uint64(u16)
		default:
			return fmt.Errorf("undefined uint type for bytes size = %d", len(col))
		}

		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		if len(col) == 0 {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}

		var f64 float64
		switch len(col) {
		case 4:
			f64 = float64(Float32fromBytes(col))
		case 8:
			f64 = Float64fromBytes(col)
		default:
			return fmt.Errorf("undefined float type for bytes size = %d", len(col))
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		if len(col) == 0 {
			return fmt.Errorf("converting NULL to %s is unsupported", dv.Kind())
		}
		dv.SetString(string(col))
		return nil
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", col, dest)
}

func asString(src []byte) string {
	return string(src[:])
}

func Float64fromBytes(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Float32fromBytes(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}
