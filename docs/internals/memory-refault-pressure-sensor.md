# Memory refault pressure sensor

Source: internal/engine/memory/pressure_os.go — const refaultPressureRatePerSec = 1000.0, moved 2026-09-11 (#1026)
Superseded: The sensor now optionally subtracts designed streaming rereads before thresholding; raw refault rate is not always the decision rate.

Page-cache pressure sensor (docs/design/scan-decode-pipelining.md §9).

The Go-heap pressure hooks above are blind to the failure mode that
convicted the decode-ahead window: held heap bytes displace the page
cache holding the file pages a scan is about to read, the kernel
absorbs the squeeze silently, and Go-side accounting never crosses any
threshold. The kernel DOES publish the displacement as it happens:
workingset_refault counts faults on pages that were recently evicted —
cache thrash by definition. Cold sequential reads are first-touch
faults, not refaults, so a healthy streaming scan reads ~0/s while
genuine displacement measured 15k-95k pages/s on the 2 GiB capped
repro (2026-07-17) — four orders of magnitude of separation.

The counter is read from the process's own cgroup v2 memory.stat when
available (attributes pressure to this container), falling back to the
host-wide /proc/vmstat (correct on dedicated workers; on shared hosts
it may fire from a co-tenant's thrash, which only costs decode-ahead
width at a moment when the disk is contended anyway — width is
worthless mid-thrash: the capped repro measured window_fulls=0 once
I/O-bound). No readable source disables the sensor permanently.

A sample is taken at most once per second; the rate must exceed the
threshold on two consecutive samples before the sensor reports active
(one-burst damping), and one quiet sample deactivates it.
