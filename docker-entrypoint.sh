#!/bin/sh
set -eu

# A banner is intentionally supplied only at deployment time. Prefer a
# read-only mounted file for multi-line ASCII art; the environment form is
# useful for small deployments and Compose block scalars.
if [ -n "${TRAIL_STARTUP_BANNER_FILE:-}" ]; then
	if [ ! -r "$TRAIL_STARTUP_BANNER_FILE" ]; then
		echo "trail: startup banner file is not readable: $TRAIL_STARTUP_BANNER_FILE" >&2
		exit 1
	fi
	cat "$TRAIL_STARTUP_BANNER_FILE"
elif [ -n "${TRAIL_STARTUP_BANNER:-}" ]; then
	printf '%s\n' "$TRAIL_STARTUP_BANNER"
fi

exec /usr/local/bin/trail-app "$@"
