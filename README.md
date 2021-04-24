# PGCOPY to Stdout / from Stdin with binary format

depend on go-pg v10

# About problem
go-pg great library for postgresql, but you can't bulk insert many rows in table thought pq.CopyIn and smtm.Prepare.
And we have only copyFrom, copyTo functions which work in CSV, Text format PGCOPY - **and this is BIg Problem** if we have Special symbols (like \t, \n, \0..) in your data, because it's breaks stream.

But PGCOPY support [binary format](https://postgrespro.com/docs/postgrespro/13/sql-copy) and it's work.

## How Use? 
```golang
import (
    gopg_copybinary "github.com/zoh/gopg-copy-binary"
    // ...
)

# push to table
var buffer = new(bytes.Buffer)
for i := 0; i < 100000; i++ {
    b, err := gopg_copybinary.ValueElement(
        // args , ..,
    )
    buffer.Write(b)
}

columns := strings.Split("column1, column2, column3, ...", ", ")
err = gopg_copybinary.CopyWriteFromReader(tx, buffer, "table", columns)
// done.
```

```golang
# read from 
elements := make([]attribute, 0, 100000)
	err = gopg_copybinary.CopyRead(tx, "( select id, uuid, name, description, active, js_vals, created_at::text from attributes )", func(cols [][]byte) error {
		var element attribute
		if err := gopg_copybinary.ScanElement(cols,
			&element.ID,
			&element.UUID,
			&element.Name,
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
```
ScanElement, ValueElement use reflection. You can use your format serialization to improve performance.


## Test
look at [how in work](_tests/raw_copy_test.go), and measure speed.