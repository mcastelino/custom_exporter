package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"

	"git.apache.org/thrift.git/lib/go/thrift"
	gen "github.com/mcastelino/custom_exporter/jaeger"
	"google.golang.org/api/support/bundler"
)

type hostExportProxy struct {
	vMListener net.Listener
	client     *agentClientUDP
	bundler    *bundler.Bundler

	// Process contains the information about the exporting process.
	// This unfortunately is currently setup hostside
	// It works if there is a single entity being traced inside
	// the VM. To be generic this has to be setup per Connection
	Process *gen.Process
}

func (he *hostExportProxy) recordTraceFromVM(buffer bytes.Buffer) {
	var span gen.Span

	dec := gob.NewDecoder(&buffer)
	err := dec.Decode(&span)
	if err != nil {
		log.Fatal("decode:", err)
	}

	log.Println("Span:", span)
	he.bundler.Add(&span, 1)
}

func (he *hostExportProxy) upload(spans []*gen.Span) error {
	batch := &gen.Batch{
		Spans:   spans,
		Process: he.Process,
	}
	return he.uploadAgent(batch)
}

func (he *hostExportProxy) uploadAgent(batch *gen.Batch) error {
	if err := he.client.EmitBatch(batch); err != nil {
		return err
	}
	// So that we do not wait for the batch buffer to fill up
	// ensures no spans are lost
	he.bundler.Flush()
	return nil
}

// Flush waits for exported trace spans to be uploaded.
//
// This is useful if your program is ending and you do not want to lose recent spans.
// Host side
func (he *hostExportProxy) Flush() {
	he.bundler.Flush()
}

// Some hardcoding here
func (he *hostExportProxy) Init() {
	var err error

	he.vMListener, err = net.Listen("unix", "/tmp/go.sock")
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	he.client, err = newAgentClientUDP("localhost:6831", udpPacketMaxLength)
	if err != nil {
		log.Fatal("Agent setup error", err)
	}

	bundler := bundler.NewBundler((*gen.Span)(nil), func(bundle interface{}) {
		if err := he.upload(bundle.([]*gen.Span)); err != nil {
			log.Fatal("bundler error:", err)
		}
	})

	bundler.BufferedByteLimit = udpPacketMaxLength
	he.bundler = bundler

	// This is hardcoded now, needs to be passed over out of band from VM to host
	// Note: The proxy here is a per process exporter proxy
	he.Process = &gen.Process{
		ServiceName: "kata-host-proxy",
	}

}

func (he *hostExportProxy) Close() {
}

// In theory we can work with a single proxy
// for all VM's running on the host if we can
// create a different context for each connection.
// For now all we do is setup a server on a specific
// URI for a specific VM to keep things simple
func (he *hostExportProxy) StartServer() {
	log.Println("Starting server")

	c, err := he.vMListener.Accept()
	if err != nil {
		log.Fatal("Accept error: ", err)
	}

	for {
		buf := make([]byte, udpPacketMaxLength)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}

		data := buf[0:nr]
		he.recordTraceFromVM(*bytes.NewBuffer(data))
	}
}

func main() {
	kataExporter := &hostExportProxy{}
	kataExporter.Init()
	defer kataExporter.Close()

	kataExporter.StartServer()
}

// Copied from https://github.com/census-instrumentation/opencensus-go/blob/master/exporter/jaeger/agent.go

// udpPacketMaxLength is the max size of UDP packet we want to send, synced with jaeger-agent
const udpPacketMaxLength = 65000

// agentClientUDP is a UDP client to Jaeger agent that implements gen.Agent interface.
type agentClientUDP struct {
	gen.Agent
	io.Closer

	connUDP       *net.UDPConn
	client        *gen.AgentClient
	maxPacketSize int                   // max size of datagram in bytes
	thriftBuffer  *thrift.TMemoryBuffer // buffer used to calculate byte size of a span
}

// newAgentClientUDP creates a client that sends spans to Jaeger Agent over UDP.
func newAgentClientUDP(hostPort string, maxPacketSize int) (*agentClientUDP, error) {
	if maxPacketSize == 0 {
		maxPacketSize = udpPacketMaxLength
	}

	thriftBuffer := thrift.NewTMemoryBufferLen(maxPacketSize)
	protocolFactory := thrift.NewTCompactProtocolFactory()
	client := gen.NewAgentClientFactory(thriftBuffer, protocolFactory)

	destAddr, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return nil, err
	}

	connUDP, err := net.DialUDP(destAddr.Network(), nil, destAddr)
	if err != nil {
		return nil, err
	}
	if err := connUDP.SetWriteBuffer(maxPacketSize); err != nil {
		return nil, err
	}

	clientUDP := &agentClientUDP{
		connUDP:       connUDP,
		client:        client,
		maxPacketSize: maxPacketSize,
		thriftBuffer:  thriftBuffer}
	return clientUDP, nil
}

// EmitBatch implements EmitBatch() of Agent interface
func (a *agentClientUDP) EmitBatch(batch *gen.Batch) error {
	a.thriftBuffer.Reset()
	a.client.SeqId = 0 // we have no need for distinct SeqIds for our one-way UDP messages
	if err := a.client.EmitBatch(batch); err != nil {
		return err
	}
	if a.thriftBuffer.Len() > a.maxPacketSize {
		return fmt.Errorf("Data does not fit within one UDP packet; size %d, max %d, spans %d",
			a.thriftBuffer.Len(), a.maxPacketSize, len(batch.Spans))
	}
	_, err := a.connUDP.Write(a.thriftBuffer.Bytes())
	return err
}

// Close implements Close() of io.Closer and closes the underlying UDP connection.
func (a *agentClientUDP) Close() error {
	return a.connUDP.Close()
}
