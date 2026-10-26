#!/bin/sh
# Build-time check runs in the final native stage before publishing an image.
set -eu
test "$(getconf GNU_LIBC_VERSION)" = 'glibc 2.41'
test "$(uname -m)" = x86_64
if command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1; then
    echo 'Unexpected Python interpreter in native runtime' >&2
    exit 1
fi
for binary in /usr/local/bin/memento /usr/local/bin/memento-admin /usr/local/bin/memento-restore /usr/local/bin/memento-drive /usr/local/bin/memento-import /usr/local/libexec/memento-native-sanity /opt/memento-native/media/lib/*.so* /opt/memento-native/ort/lib/*.so*; do
    dependencies=$(ldd "$binary" 2>&1) || { echo "Native dependency resolution failed: $binary" >&2; exit 1; }
    case "$dependencies" in *'not found'*) echo "Missing native dependency: $binary" >&2; exit 1;; esac
done
test -s /usr/share/doc/memento/ann/Apache-2.0
test -s /usr/share/doc/memento/model/Pillow-LICENSE.txt
test -s /usr/share/doc/memento/model/PyTorch-LICENSE.txt
test -s /usr/share/doc/memento/media/Pillow
test -s /usr/share/doc/memento/media/libavif
/usr/local/libexec/memento-native-sanity
# CLI help exits before authority/model initialization. Owner configure commands
# are deliberately not invoked by packaging checks.
/usr/local/bin/memento --help >/dev/null
echo 'Native ELF closure and service CLI passed'
