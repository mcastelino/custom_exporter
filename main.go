package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"

	"fmt"
	"log"
	"time"

	gen "github.com/mcastelino/custom_exporter/jaeger"

	"go.opencensus.io/trace"
)

type customTraceExporter struct{}

func logme(buffer bytes.Buffer) {
	var sd trace.SpanData

	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(&sd)
	if err != nil {
		log.Fatal("decode:", err)
	}

	fmt.Printf("Name: %s\nTraceID: %x\nSpanID: %x\nParentSpanID: %x\nStartTime: %s\nEndTime: %s\nAnnotations: %+v\n\n",
		sd.Name, sd.TraceID, sd.SpanID, sd.ParentSpanID, sd.StartTime, sd.EndTime, sd.Annotations)
}

// Copied from https://github.com/census-instrumentation/opencensus-go/blob/master/exporter/jaeger/jaeger.go
func bytesToInt64(buf []byte) int64 {
	u := binary.BigEndian.Uint64(buf)
	return int64(u)
}

func name(sd *trace.SpanData) string {
	n := sd.Name
	switch sd.SpanKind {
	case trace.SpanKindClient:
		n = "Sent." + n
	case trace.SpanKindServer:
		n = "Recv." + n
	}
	return n
}

func attributeToTag(key string, a interface{}) *gen.Tag {
	var tag *gen.Tag
	switch value := a.(type) {
	case bool:
		tag = &gen.Tag{
			Key:   key,
			VBool: &value,
			VType: gen.TagType_BOOL,
		}
	case string:
		tag = &gen.Tag{
			Key:   key,
			VStr:  &value,
			VType: gen.TagType_STRING,
		}
	case int64:
		tag = &gen.Tag{
			Key:   key,
			VLong: &value,
			VType: gen.TagType_LONG,
		}
	case int32:
		v := int64(value)
		tag = &gen.Tag{
			Key:   key,
			VLong: &v,
			VType: gen.TagType_LONG,
		}
	case float64:
		v := float64(value)
		tag = &gen.Tag{
			Key:     key,
			VDouble: &v,
			VType:   gen.TagType_DOUBLE,
		}
	}
	return tag
}
func spanDataToThrift(data *trace.SpanData) *gen.Span {
	tags := make([]*gen.Tag, 0, len(data.Attributes))
	for k, v := range data.Attributes {
		tag := attributeToTag(k, v)
		if tag != nil {
			tags = append(tags, tag)
		}
	}

	tags = append(tags,
		attributeToTag("status.code", data.Status.Code),
		attributeToTag("status.message", data.Status.Message),
	)

	var logs []*gen.Log
	for _, a := range data.Annotations {
		fields := make([]*gen.Tag, 0, len(a.Attributes))
		for k, v := range a.Attributes {
			tag := attributeToTag(k, v)
			if tag != nil {
				fields = append(fields, tag)
			}
		}
		fields = append(fields, attributeToTag("message", a.Message))
		logs = append(logs, &gen.Log{
			Timestamp: a.Time.UnixNano() / 1000,
			Fields:    fields,
		})
	}
	var refs []*gen.SpanRef
	for _, link := range data.Links {
		refs = append(refs, &gen.SpanRef{
			TraceIdHigh: bytesToInt64(link.TraceID[0:8]),
			TraceIdLow:  bytesToInt64(link.TraceID[8:16]),
			SpanId:      bytesToInt64(link.SpanID[:]),
		})
	}
	return &gen.Span{
		TraceIdHigh:   bytesToInt64(data.TraceID[0:8]),
		TraceIdLow:    bytesToInt64(data.TraceID[8:16]),
		SpanId:        bytesToInt64(data.SpanID[:]),
		ParentSpanId:  bytesToInt64(data.ParentSpanID[:]),
		OperationName: name(data),
		Flags:         int32(data.TraceOptions),
		StartTime:     data.StartTime.UnixNano() / 1000,
		Duration:      data.EndTime.Sub(data.StartTime).Nanoseconds() / 1000,
		Tags:          tags,
		Logs:          logs,
		References:    refs,
	}
}

func (ce *customTraceExporter) ExportSpan(sd *trace.SpanData) {
	var network bytes.Buffer // Stand-in for a network connection

	thriftData := spanDataToThrift(sd)
	enc := gob.NewEncoder(&network) // Will write to network.

	err := enc.Encode(&thriftData)
	if err != nil {
		log.Fatal("encode error:", err)
	}

	dec := gob.NewDecoder(&network) // Will read from network.

	var onTheOtherSide gen.Span
	err = dec.Decode(&onTheOtherSide)
	if err != nil {
		log.Fatal("decode error:", err)
	}

	fmt.Println(onTheOtherSide)
}

func main() {
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	// Please remember to register your exporter
	// so that it can receive exported spanData.
	trace.RegisterExporter(new(customTraceExporter))

	for i := 0; i < 5; i++ {
		_, span := trace.StartSpan(context.Background(), fmt.Sprintf("sample-%d", i))
		span.Annotate([]trace.Attribute{trace.Int64Attribute("invocations", 1)}, "Invoked it")
		span.End()
		<-time.After(10 * time.Millisecond)
	}
	<-time.After(500 * time.Millisecond)
}