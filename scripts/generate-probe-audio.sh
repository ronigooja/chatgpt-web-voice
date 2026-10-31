#!/bin/sh
set -eu
out=${1:-/tmp/voice-probe-audio}
mkdir -p "$out"
command -v say >/dev/null || { echo "say unavailable" >&2; exit 2; }
command -v afconvert >/dev/null || { echo "afconvert unavailable" >&2; exit 2; }
say -o "$out/u1.aiff" "Please create an HTML file called hello."
say -o "$out/u2.aiff" "What is my project called?"
afconvert -f WAVE -d LEI16@48000 -c 1 "$out/u1.aiff" "$out/u1.wav"
afconvert -f WAVE -d LEI16@48000 -c 1 "$out/u2.aiff" "$out/u2.wav"
rm -f "$out/u1.aiff" "$out/u2.aiff"
printf '%s\n' "$out/u1.wav" "$out/u2.wav"
