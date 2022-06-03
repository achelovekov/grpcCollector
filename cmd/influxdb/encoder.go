package main
import (
	"fmt"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func main() {
	var enc lineprotocol.Encoder
	enc.StartLine("foo")
	enc.SetPrecision(lineprotocol.Microsecond)
	enc.AddTag("tag1", "val1")
	enc.AddTag("tag2", "val2")
	enc.AddField("x", lineprotocol.MustNewValue(1.0))
	enc.AddField("y", lineprotocol.MustNewValue("hello"))
	enc.EndLine(time.Unix(0, 1625823259000000000))
	enc.StartLine("bar")
	enc.AddField("enabled", lineprotocol.BoolValue(true))
	enc.EndLine(time.Time{})
	if err := enc.Err(); err != nil {
		panic(fmt.Errorf("encoding error: %v", err))
	}
	fmt.Println(string(enc.Bytes()))
}
