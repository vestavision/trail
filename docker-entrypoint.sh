#!/bin/sh
set -eu

# The banner is owned and versioned with the image. It is enabled by default;
# only an explicit false value suppresses it for quiet deployments.
case "${TRAIL_STARTUP_BANNER_ENABLED:-true}" in
	false|FALSE|0|no|NO|off|OFF) ;;
	*) cat /etc/trail/startup-banner.txt ;;
esac

exec /usr/local/bin/trail-app "$@"
