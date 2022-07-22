#!/usr/bin/env bash

exit_with_error()
{
    echo "Fatal error: $1" >&2
    # TODO: send email/notification?
    exit 1
}

apt update && apt install p7zip-full wget -y

if ! [ -x "$(command -v 7z)" ]; then
  exit_with_error "Installing 7z failed.  Aborting."
fi
if ! [ -x "$(command -v wget)" ]; then
  exit_with_error "Installing wget failed.  Aborting."
fi