#!/bin/bash
# Fix gpt symlink at runtime
ln -sf /opt/snap/bin/gpt /usr/bin/gpt
# Start the original command
exec "$@"
