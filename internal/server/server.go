package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"encoding/json"
	"os"
	"io/ioutil"
	"time"
	"sort"
	//"reflect"

	//dialout "github.com/CiscoSE/grpc/proto/mdt_dialout"
	// "encoding/json"

	dialout "github.com/achelovekov/grpcCollector/proto/mdt_dialout"
	telemetry "github.com/achelovekov/grpcCollector/proto/telemetry"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type DialOutServer struct {
	cancel context.CancelFunc
	ctx    context.Context
}

type Filter struct {
	Tags []*Model
	Fields []*Model
}

func FiltersGenerate(models []string) []Filter {

	f := []Filter{}

	for _, model := range models {
		model := ParseModel(model)
		filter := GenerateFilterFromModel(&model)
		f = append(f, filter)
	}

	return f
}

func Sender(buf *[]lineprotocol.Encoder, writeAPI api.WriteAPI) {
	for {
		timer := time.NewTimer(2 * time.Second)
		<-timer.C
		fmt.Println("passed 2 seconds")
		if len(*buf) > 0 {
			for _, item := range *buf {
				writeAPI.WriteRecord(string(item.Bytes()))
				// fmt.Printf("%s", item.Bytes())
				// fmt.Println()
			}
		}
		*buf = nil
	}
}

func (c *DialOutServer) MdtDialout(stream dialout.GRPCMdtDialout_MdtDialoutServer) error {

	peer, peerOK := peer.FromContext(stream.Context())
	if peerOK {
		log.Printf("Accepted Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}


	models := []string{}
	models = append(models, "bgp-model-afi.json")
	models = append(models, "bgp-model-neighbors.json")

	filters := FiltersGenerate(models)
	buf := []lineprotocol.Encoder{}

	// Create a new client using an InfluxDB server base URL and an authentication token
	client := influxdb2.NewClient("http://10.0.17.11:8086", "Ubm4lS0Smd8aGYVI5LlwwAJJVX6BbKDdS4GLU1nVwyR2Ku2_JMsEs4hW8mwmy-TH2L3a8vhVgashOZk5azkqsw==")
	// Use blocking write client for writes to desired bucket
	writeAPI := client.WriteAPI("Neto", "grpcBucket")


	go Sender(&buf, writeAPI)

	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF && c.ctx.Err() == nil {
				fmt.Printf("E! GRPC dialout receive error: %v", err)
			}
			break
		}

		if len(packet.Data) == 0 && len(packet.Errors) != 0 {
			log.Printf("no more data")
			break
		}

		c.handleTelemetry(packet.Data, filters, &buf)
	}

	if peerOK {
		log.Printf("Closed Cisco MDT GRPC dialout connection from %s", peer.Addr)
	}

	return nil
}

type Model struct {
	Name   string `json:"name"`
	Nested []*Model `json:"nested"`
	IsTag bool `json:"isTag"`
	IsField bool `json:"isField"`
	IsList bool `json:"isList"`
}

func (model *Model) String() string {
    return fmt.Sprintf("Name: %v isTag: %v isField: %v isList: %v", model.Name, model.IsTag, model.IsField, model.IsList)
}

func (model *Model) GetNestedByName(name string) *Model {
	for _, item := range model.Nested {
		if item.Name == name {
			return item
		}
	}
    return &Model{}
}

func (model *Model) GetName() string {
	if model != nil {
		return model.Name
	}
	return ""
}

func (model *Model) GetIsTag() bool {
	if model != nil {
		return model.IsTag
	}
	return false
}

func (model *Model) GetIsField() bool {
	if model != nil {
		return model.IsField
	}
	return false
}

func (model *Model) GetNested() []*Model {
	if model != nil {
		return model.Nested
	}
	return []*Model{}
}

func (model *Model) CheckNestedByName(s string) bool {
	for _, item := range model.Nested {
		if item.Name == s {
			return true
		}
	}
	return false
}

func ParseModel(filename string) Model {
	// Open jsonFile
	jsonFile, err := os.Open(filename)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
		return Model{}
	}
	fmt.Println("Successfully Opened:", filename)
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var model Model

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	json.Unmarshal(byteValue, &model)

	return model
}

func GenerateFilterFromModel(model *Model) (Filter) {
	tags := []*Model{}
	fields := []*Model{}

	prefix := []string{}

	for _, item := range model.Nested {
		FilterFlatten(item, &tags, &fields, prefix)
	}

	sort.SliceStable(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	  })

	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	  })

	var filter Filter
	filter.Tags = tags
	filter.Fields = fields

	return filter
}

func FilterFlatten(model *Model, tags *[]*Model, fields *[]*Model, prefix []string) {
	if len(model.Nested) > 0 {
		prefix = append(prefix, model.Name)
		for _, item := range model.Nested {
			FilterFlatten(item, tags, fields, prefix)
		}
	} else {
		prefix = append(prefix, model.Name)
		name := strings.Join(prefix,".")
		newModel := new(Model)
		newModel.Name = name
		if model.IsTag {
			newModel.IsTag = model.IsTag
			*tags = append(*tags, newModel)
		}
		if model.IsField {
			newModel.IsField = model.IsField
			*fields = append(*fields, newModel)
		}
	}
}

func (c *DialOutServer) handleTelemetry(data []byte, filters []Filter, buf *[]lineprotocol.Encoder) {

	telemetryData := &telemetry.Telemetry{}
	err := proto.Unmarshal(data, telemetryData)
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}

	// model := Model{Name: telemetryData.EncodingPath}

	// destructureTelemetry(telemetryData, &model)

	model := ParseModel("bgp-model-afi.json")

	bar(&model, telemetryData)

	//PrintModel(&model,  0)
	// b, err := json.MarshalIndent(model, "", "  ")
    // if err != nil {
    //     fmt.Println(err)
    // }
    // fmt.Print(string(b))
	// b, err = json.Marshal(telemetryData)

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// fmt.Println(string(b))
	//NEW ENTRY!!!
}

func PrintMap(m map[string]Leaf) {
	for k, v := range m {
        fmt.Println(k, ":", v)
    }
}

// func flatten(telemetryData *telemetry.TelemetryField, prefix []string, m map[string]interface{}) {
// 	if len(telemetryData.Fields) > 0 {
// 		if (len(telemetryData.Name) > 0 && telemetryData.Name != "keys" && telemetryData.Name != "content") {
// 			prefix = append(prefix, telemetryData.Name)
// 		}

// 		for _, item := range telemetryData.Fields {
// 			flatten(item, prefix, m)
// 		}
// 	} else {
// 		if (len(telemetryData.Name) > 0 && telemetryData.Name != "keys" && telemetryData.Name != "content") {
// 			fullPath := append(prefix, telemetryData.Name)
// 			i := telemetryData.GetValueByType()
// 			switch i.(type) {
// 			case *telemetry.TelemetryField_BytesValue:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetBytesValue()
// 			case *telemetry.TelemetryField_StringValue:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetStringValue()
// 			case *telemetry.TelemetryField_BoolValue:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetBoolValue()
// 			case *telemetry.TelemetryField_Uint32Value:
// 				m[strings.Join(fullPath,".")] = int64(telemetryData.GetUint32Value())
// 			case *telemetry.TelemetryField_Uint64Value:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetUint64Value()
// 			case *telemetry.TelemetryField_Sint32Value:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetSint32Value()
// 			case *telemetry.TelemetryField_Sint64Value:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetSint64Value()
// 			case *telemetry.TelemetryField_DoubleValue:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetDoubleValue()
// 			case *telemetry.TelemetryField_FloatValue:
// 				m[strings.Join(fullPath,".")] = telemetryData.GetFloatValue()
// 			}
// 		}
// 	}
// }


func containsString(s []string, v string) bool {
	for _, item := range s {
        if item == v {
            return true
        }
    }
    return false
}

func PrepareLine(measurement string, m map[string]interface{}, filter Filter, enc lineprotocol.Encoder) lineprotocol.Encoder {
	enc.SetPrecision(lineprotocol.Microsecond)
	enc.StartLine(measurement)
	for _, item := range filter.Tags {
		val, ok := m[item.Name]
		if ok {
			enc.AddTag(item.Name, val.(string))
		}
	}

	for _, item := range filter.Fields {
		val, ok := m[item.Name]
		if ok {
			enc.AddField(item.Name, lineprotocol.MustNewValue(val))
		}
	}

	fmt.Println(len(enc.Bytes()))
	//enc.EndLine(time.Time{})
	if err := enc.Err(); err != nil {
		panic(fmt.Errorf("encoding error: %v", err))
	}
	//fmt.Printf("--------->%s", enc.Bytes())

	return enc
}

func contains(sli []*Model, elem *Model) bool {
    for _, item := range sli {
        if elem.Name == item.Name {
            return true
        }
    }
    return false
}

func GetContent(model *Model, tf *telemetry.TelemetryField) (*telemetry.TelemetryField, error) {
	modelName := model.GetName()

	if len(tf.GetName()) == 0  && len(tf.GetFields()) > 0 {
		for _, tf := range tf.GetFields() {
			if name := tf.GetName(); name == "keys" || name == "content" {
				for _, tf := range tf.GetFields() {
					if tf.GetName() == modelName {
						return tf, nil
					}
				}
			}
		}
	}
	if len(tf.GetName()) > 0 && len(tf.GetFields()) > 0 {
		for _, tf := range tf.GetFields() {
			if tf.GetName() == modelName {
				return tf, nil
			}
		}
	}

	return &telemetry.TelemetryField{}, fmt.Errorf("no content %v in nested of %s", model.GetName(), tf.GetName())
}

func GetContentList(model *Model, tf *telemetry.TelemetryField) []*telemetry.TelemetryField  {
	result := []*telemetry.TelemetryField{}
	for _, tfField := range tf.GetFields() {
		if tfField.GetName() == model.GetName() {
			newTf := telemetry.TelemetryField{}
			newTf.Name = tf.GetName()
			newTf.Fields = append(newTf.Fields, tfField)
			result = append(result, &newTf)
		}
	}
	return result
}

type Keys struct {
	Leafs []*Model
	Nesteds []*Model
	WLists []*Model
}

func (keys *Keys) String() string {
    return fmt.Sprintf("Leafs: %v Nesteds: %v WLists: %v", keys.Leafs, keys.Nesteds, keys.WLists)
}

func FindKeys(model *Model) Keys {
	var leafs []*Model
	var nesteds []*Model
	var wLists []*Model

	nested := model.GetNested()

	if len(nested) > 0 {
		for _, model := range nested {
			if len(model.GetNested()) == 0 && !model.IsList {
				leafs = append(leafs, model)
			}
			if len(model.GetNested()) > 0 && !model.IsList{
				nesteds = append(nesteds, model)
			}
			if model.IsList {
				wLists = append(wLists, model)
			}
		}
	}

	return Keys{Leafs: leafs, Nesteds: nesteds, WLists: wLists}
}

func CopyMap(ma map[string]Leaf) map[string]Leaf {
	newMap := make(map[string]Leaf)
	for k, v := range ma {
		newMap[k] = v
	}
	return newMap
}

type Leaf struct {
	Value interface{}
	Model
}

func (leaf *Leaf) String() string {
    return fmt.Sprintf("Name: %v isTag: %v isField: %v isList: %v value: %v", leaf.Name, leaf.IsTag, leaf.IsField, leaf.IsList, leaf.Value)
}

func CopySlice(s *[]*Leaf) *[]*Leaf {
    d := make([]*Leaf, len(*s))

    copy(d, *s)

	return &d
}

func PrintSlice(s *[]*Leaf) {
	for _, item := range *s {
		fmt.Println(item)
	}
}

func ProcessSlice(sli *[]*Leaf) ([]*Leaf, []*Leaf) {
	tags := []Leaf{}
	fields := []Leaf{}



}

func PushSlice(sli *[]*Leaf) {
	pass
}

func UpdateSli(model *Model, tf *telemetry.TelemetryField, sli *[]*Leaf, prefix []string) {

	tf, err := GetContent(model, tf)

	if err != nil {
		log.Println(err)
		return
	}

	fullPath := append(prefix, model.GetName())
	newLeaf := Leaf{}
	newLeaf.Name = strings.Join(fullPath,".")
	newLeaf.IsTag = model.GetIsTag()
	newLeaf.IsField = model.GetIsField()
	i := tf.GetValueByType()
	switch i.(type) {
	case *telemetry.TelemetryField_BytesValue:
		newLeaf.Value = tf.GetBytesValue()
	case *telemetry.TelemetryField_StringValue:
		newLeaf.Value = tf.GetStringValue()
	case *telemetry.TelemetryField_BoolValue:
		newLeaf.Value = tf.GetBoolValue()
	case *telemetry.TelemetryField_Uint32Value:
		newLeaf.Value = int64(tf.GetUint32Value())
	case *telemetry.TelemetryField_Uint64Value:
		newLeaf.Value = tf.GetUint64Value()
	case *telemetry.TelemetryField_Sint32Value:
		newLeaf.Value = tf.GetSint32Value()
	case *telemetry.TelemetryField_Sint64Value:
		newLeaf.Value = tf.GetSint64Value()
	case *telemetry.TelemetryField_DoubleValue:
		newLeaf.Value = tf.GetDoubleValue()
	case *telemetry.TelemetryField_FloatValue:
		newLeaf.Value = tf.GetFloatValue()
	}

	*sli = append(*sli, &newLeaf)
}

func foo(model *Model, tf *telemetry.TelemetryField, sli *[]*Leaf, prefix []string) {
	tf, err := GetContent(model, tf)

	if err != nil {
		log.Println(err)
		return
	}

	//fmt.Println("1: ", tf)

	keys := FindKeys(model)

	//fmt.Println("2: ", keys)

	for _, leaf := range keys.Leafs {
		//fmt.Println("3: ", leaf)
		UpdateSli(leaf, tf, sli, prefix)
	}

	if len(keys.Nesteds) > 0 {
		prefix := append(prefix, model.GetName())
		for _, nested := range keys.Nesteds {
			//fmt.Println("4: ", nested)
			newSli := CopySlice(sli)

			var newPrefix []string

    		newPrefix = append(newPrefix, prefix...)
			newPrefix = append(newPrefix, nested.GetName())

			foo(nested, tf, newSli, newPrefix)
		}
	}

	if len(keys.WLists) > 0 {
		for _, wList := range keys.WLists {
			// fmt.Println("Content list for key: ", wList.GetName())
			// fmt.Println(GetContentList(wList, tf))
			for _, content := range GetContentList(wList, tf) {
				newSli := CopySlice(sli)
				var newPrefix []string
				newPrefix = append(newPrefix, prefix...)
				if len(wList.GetNested()) > 0 {
					newPrefix = append(newPrefix, wList.GetName())
				}
				foo(wList, content, newSli, newPrefix)
			}
		}
	}

	if len(keys.Nesteds) == 0 && len(keys.WLists) == 0 {
		//fmt.Println("6: ", m)
		fmt.Println("-------")
		//PrintSlice(sli)
		PushMetric(sli)
		fmt.Println("-------")
	}

}

func bar(model *Model, td *telemetry.Telemetry) {
	sli := []*Leaf{}
	prefix := []string{}
	for _, tf := range td.GetDataGpbkv() {
		foo(model, tf, &sli, prefix)
	}
}

func destructureTelemetry(telemetry *telemetry.Telemetry, model *Model) {
	telemetryData := telemetry.GetDataGpbkv()

	for _, item := range telemetryData {
		destructureFields(item.GetFields(), model)
	}
}

func destructureFields(fields []*telemetry.TelemetryField, model *Model) {
	for _, item := range fields {
		if name := item.GetName(); len(name) >= 0 {
			if len(name) == 0 {
				destructureFields(item.GetFields(), model)
			}
			switch name {
			case "keys", "content":
				destructureFields(item.GetFields(), model)
			default:
				newModel := &Model{Name: item.GetName()}
				if exist := model.GetNestedByName(item.GetName()); len(exist.Name) > 0 {
					exist.IsList = true
				} else {
					model.Nested = append(model.Nested, newModel)
				}
				if len(item.GetFields()) > 0 {
					destructureFields(item.GetFields(), newModel)
				}
			}
		}

	}


}

func PrintModel(model *Model, tab int) {
	fmt.Println(strings.Repeat(" ", tab) + model.Name)
	if len(model.Nested) > 0 {
		tab += 2
		for _, item := range model.Nested {
			PrintModel(item, tab)
		}
	}
}

func NewGRPCDialOutSever() *grpc.Server {
	c := &DialOutServer{}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	dialout.RegisterGRPCMdtDialoutServer(grpcServer, c)

	return grpcServer
}
