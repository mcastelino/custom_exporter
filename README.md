# custom_exporter

Custom Opencensus exporter over a custom transport protocol

The goal was to carry the spans over something like vsock
from a VM to the host and then push it to the actual 
backend.

The default spans are unfortunately not serializeable, hence
convert the thrift format used by jaeger

# Building 

```
go build -o host host.go
go build -o vm vm.go
```

# Running

First run the proxy as it will be waiting for traces

```
rm /tmp/go.sock ; ./host
```

Then run the VM
```
./vm
```

When the VM terminates the proxy also terminates
