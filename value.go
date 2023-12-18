package gopg_copybinary

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
)

func ValueElement(params ...interface{}) ([]byte, error) {
	// write count of columns
	var (
		buf = new(bytes.Buffer)

		numberFields = make([]byte, 2)
		lenFiend     = make([]byte, 4) // len in bytes
	)

	binary.BigEndian.PutUint16(numberFields, uint16(len(params)))

	// write size columns
	buf.Write(numberFields)

	for i, p := range params {
		bb, err := convertToBytes(p)
		if err != nil {
			return nil, fmt.Errorf("convertToBytes column=%d %w", i, err)
		}
		if len(bb) == 0 {
			buf.Write(nilField)
			continue
		}
		binary.BigEndian.PutUint32(lenFiend, uint32(len(bb)))
		buf.Write(lenFiend)
		buf.Write(bb)

	}
	return buf.Bytes(), nil
}

func convertToBytes(p interface{}) ([]byte, error) {
	switch d := p.(type) {
	case string:
		return []byte(d), nil
	case []byte:
		return []byte(d)[:], nil
	case sql.RawBytes:
		return []byte(d)[:], nil
	case bool:
		if d {
			return []byte{0x01}, nil
		}
		return []byte{0x00}, nil

	case *interface{}:
		return nil, errors.New("unsupported *interface{}")
	}

	if m, ok := p.(encoding.BinaryMarshaler); ok {
		if reflect.ValueOf(p).Kind() == reflect.Ptr && reflect.ValueOf(p).IsNil() {
			return nil, nil
		}
		return m.MarshalBinary()
	}

	if valuer, ok := p.(driver.Valuer); ok && valuer != nil {
		if reflect.ValueOf(p).Kind() == reflect.Ptr && reflect.ValueOf(p).IsNil() {
			return nil, nil
		}

		v, err := valuer.Value()
		if err != nil {
			return nil, err
		}
		switch vv := v.(type) {
		case string:
			return []byte(vv), nil
		case []byte:
			return vv, nil

		default:
			return nil, fmt.Errorf("error driver.Valuer  %T", v)
		}
	}

	dpv := reflect.ValueOf(p)
	dv := reflect.Indirect(dpv)

	switch dv.Kind() {
	case reflect.Ptr:
		return convertToBytes(dv.Interface())

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64 := dv.Int()
		var b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i64))
		return b, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64 := dv.Uint()
		var b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(u64))
		return b, nil

	case reflect.Float32, reflect.Float64:
		f64 := dv.Float()
		bits := math.Float64bits(f64)
		var b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(bits))
		return b, nil

	case reflect.String:
		return []byte(dv.String()), nil
	}

	return nil, fmt.Errorf("unsupported ValueElement, storing driver.Value type %T", p)
}
