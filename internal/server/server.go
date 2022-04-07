package server

import (
	"context"
	"log"
	"io"
	"fmt"
	"time"
	"bytes"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	telemetry "github.com/achelovekov/grpcCollector/proto/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type DialOutServer struct {
	cancel context.CancelFunc
	ctx    context.Context
}


func (c *DialOutServer) MdtDialout(stream dialout.GRPCMdtDialout_MdtDialoutServer) error {

	peer, peerOK := peer.FromContext(stream.Context())
	if peerOK {
		log.Printf("Accepted Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}

	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF && c.ctx.Err() == nil {
				fmt.Printf("E! GRPC dialout receive error: %v", err)
			}
		}

		if len(packet.Data) == 0 && len(packet.Errors) != 0 {
			log.Printf("no more data")
			break
		}

		c.handleTelemetry(packet.Data)
	}

	if peerOK {
		log.Printf("Closed Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}
	
	return nil
}

func (c *DialOutServer) handleTelemetry(data []byte) {
	var buf bytes.Buffer
	telemetryData := &telemetry.Telemetry{}
	err := proto.Unmarshal(data, telemetryData)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}

	var tags map[string]interface{}
	var contents map[string]interface{}

	for _, gpbkv := range telemetryData.DataGpbkv {
		measured := gpbkv.Timestamp
		if measured == 0 {
			measured = telemetryData.MsgTimestamp
		}
	
		timestamp := time.Unix(int64(measured/1000), int64(measured%1000)*1000000)

		for _, field := range gpbkv.Fields {
			switch field.Name {
			case "keys":
				fmt.Printf("go for keys\n")
				tags = make(map[string]interface{})
				tags["Producer"] = telemetryData.GetNodeIdStr()
				tags["Target"] = telemetryData.GetSubscriptionIdStr()
				tags["EncodingPath"] = telemetryData.EncodingPath
				tags["TimeStamp"] = timestamp.String()
				for _, subfield := range field.Fields {
					c.parseGPBKVField(subfield,
						&buf,
						telemetryData.EncodingPath,
						timestamp, 
						tags)
				}
			case "content":
				fmt.Printf("go for content\n")
				contents = make(map[string]interface{})
				for _, subfield := range field.Fields {
					c.parseGPBKVField(subfield,
						&buf,
						telemetryData.EncodingPath,
						timestamp, 
						contents)
				}
			}
		}

		if len(tags) > 0 && len(contents) > 0 && len(telemetryData.EncodingPath) > 0 {

			log.Printf("\n**** New Telemetry message from %v ****", tags["Producer"])
			log.Printf("Tags: %v", tags)
			log.Printf("Fields: %v\n", contents)
			//log.Printf(telemetry.EncodingPath, fields, tags, timestamp)

		} else {
			fmt.Printf("I! Cisco MDT invalid field: encoding path or measurement empty")
		}
	}

}


// Recursively parse GPBKV field structure into fields or tags
func (c *DialOutServer) parseGPBKVField(
	field *telemetry.TelemetryField, 
	buf *bytes.Buffer,
	path string, 
	timestamp time.Time, 
	valueMap map[string]interface{}) {
	
	bufLen := buf.Len()
	if bufLen > 0 {
		buf.WriteRune('/')
	}
	buf.WriteString(field.Name)

	var value interface{}
	switch field.ValueByType.(type) {
	case *telemetry.TelemetryField_BytesValue:
		value = field.ValueByType.(*telemetry.TelemetryField_BytesValue).BytesValue
	case *telemetry.TelemetryField_StringValue:
		value = field.ValueByType.(*telemetry.TelemetryField_StringValue).StringValue
	case *telemetry.TelemetryField_BoolValue:
		value = field.ValueByType.(*telemetry.TelemetryField_BoolValue).BoolValue
	case *telemetry.TelemetryField_Uint32Value:
		value = field.ValueByType.(*telemetry.TelemetryField_Uint32Value).Uint32Value
	case *telemetry.TelemetryField_Uint64Value:
		value = field.ValueByType.(*telemetry.TelemetryField_Uint64Value).Uint64Value
	case *telemetry.TelemetryField_Sint32Value:
		value = field.ValueByType.(*telemetry.TelemetryField_Sint32Value).Sint32Value
	case *telemetry.TelemetryField_Sint64Value:
		value = field.ValueByType.(*telemetry.TelemetryField_Sint64Value).Sint64Value
	case *telemetry.TelemetryField_DoubleValue:
		value = field.ValueByType.(*telemetry.TelemetryField_DoubleValue).DoubleValue
	case *telemetry.TelemetryField_FloatValue:
		value = field.ValueByType.(*telemetry.TelemetryField_FloatValue).FloatValue
	}

	if value != nil {
		{
			valueMap[buf.String()] = fmt.Sprint(value)
		}
	}

	for _, subfield := range field.Fields {
		c.parseGPBKVField(subfield,
			buf,
			path,
			timestamp, 
			valueMap)
	}

	buf.Truncate(bufLen)
}

func NewGRPCDialOutSever() *grpc.Server{
	c := &DialOutServer{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	dialout.RegisterGRPCMdtDialoutServer(grpcServer, c)

	return grpcServer
}
