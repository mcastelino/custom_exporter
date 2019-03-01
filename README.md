# custom_exporter

Custom Opencensus exporter over a custom transport protocol

The goal was to carry the spans over something like vsock
from a VM to the host and then push it to the actual 
backend.

The default spans are unfortunately not serializeable, hence
convert the thrift format used by jaeger
