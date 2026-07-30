#!/usr/bin/env bash
# Capture a real pvAccess conversation against a loopback soft IOC.
#
# Ground truth for docs/pvaccess-implementation-plan.md phase 1: the pcap is
# what items 1.1-1.5 are checked against, and it settles the CONNECTION_VALIDATED
# direction question flagged in item 2.5.
#
# Writes to tools/captures/:
#   pvacs-*.pcap    raw wire bytes, one file per client implementation
#   *.log           each client's own narration of what it thought it was doing
#
# Requires: EPICS base on PATH (softIocPVA, pvget). pvxs is optional - if
# pvxget is on PATH it is captured too, which is the point of having a second
# implementation. tcpdump needs root; you are prompted for sudo once, up front.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$HERE/captures"
mkdir -p "$OUT"

command -v softIocPVA >/dev/null || { echo "softIocPVA not on PATH - source your EPICS setup.sh" >&2; exit 1; }

# Keep everything on loopback. Note this constrains TCP and beacons but NOT the
# UDP search listener, which still binds your real interfaces - the IOC will
# answer searches from the network for as long as this script runs.
export EPICS_PVA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVAS_INTF_ADDR_LIST=127.0.0.1
export EPICS_PVAS_BEACON_ADDR_LIST=127.0.0.1
export EPICS_CAS_INTF_ADDR_LIST=127.0.0.1
export EPICS_CAS_BEACON_ADDR_LIST=127.0.0.1

IOC_PID=""
cleanup() {
    # tcpdump runs under sudo, so $! is sudo's pid and SIGTERM does not reliably
    # reach the child. Match on the command line instead.
    sudo pkill -f "tcpdump -i lo0 -s 0 -w $OUT" 2>/dev/null || true
    [[ -n "$IOC_PID" ]] && kill "$IOC_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Authorise sudo before backgrounding anything, so the prompt reaches the tty.
echo "==> tcpdump needs root to open /dev/bpf"
sudo -v

echo "==> starting soft IOC (loopback only)"
softIocPVA -S -d "$HERE/capture.db" > "$OUT/ioc.log" 2>&1 &
IOC_PID=$!
sleep 3
kill -0 "$IOC_PID" 2>/dev/null || { echo "IOC failed to start, see $OUT/ioc.log" >&2; exit 1; }

# One capture per client, so each pcap holds a complete connection from
# SET_BYTE_ORDER to close without the other implementation interleaved.
capture_with() {
    local tag="$1" get="$2" info="$3" put="$4" monitor="$5"
    command -v "$get" >/dev/null || { echo "==> skipping $tag ($get not on PATH)"; return; }

    echo "==> capturing $tag"
    sudo tcpdump -i lo0 -s 0 -w "$OUT/pvacs-$tag.pcap" \
        'tcp port 5075 or udp port 5076' 2>/dev/null &
    sleep 2

    "$get"  TEST:DOUBLE TEST:LONG TEST:ENUM TEST:STRING > "$OUT/$tag-get.log"  2>&1 || true
    "$info" TEST:DOUBLE TEST:ENUM                       > "$OUT/$tag-info.log" 2>&1 || true
    # The two pvRequest forms item 2.11 names as de-facto required.
    "$get" -r 'field(value)'                  TEST:DOUBLE >> "$OUT/$tag-get.log" 2>&1 || true
    "$get" -r 'field(value,alarm,timeStamp)'  TEST:DOUBLE >> "$OUT/$tag-get.log" 2>&1 || true
    "$put"  TEST:DOUBLE 43.5                              > "$OUT/$tag-put.log" 2>&1 || true
    timeout 5 "$monitor" TEST:DOUBLE                      > "$OUT/$tag-monitor.log" 2>&1 || true

    sleep 2
    sudo pkill -f "tcpdump -i lo0 -s 0 -w $OUT/pvacs-$tag.pcap" 2>/dev/null || true
    sleep 1
}

capture_with pvaccesscpp pvget  pvinfo  pvput  pvmonitor
capture_with pvxs        pvxget pvxinfo pvxput pvxmonitor

cleanup
trap - EXIT INT TERM

echo
echo "Captured to $OUT:"
ls -la "$OUT"
cat <<EOF

Read a capture with:
  tcpdump -r $OUT/pvacs-pvaccesscpp.pcap -X | less

Every pvAccess frame starts 0xCA <version> <flags> <command>; the first
application byte after the TCP handshake should be a SET_BYTE_ORDER control
message (flags bit 0 set, command 0x02).
EOF
