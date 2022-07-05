#!/usr/bin/env bash

exit_with_error()
{
    echo "Fatal error: $1" >&2
    # TODO: send email/notification?
    exit 1
}

apt update && apt install p7zip-full wget -y

if ! [ -x "$(command -v 7z)" ]; then
  exit_with_error "I require 7z but it's not installed.  Aborting."
fi
if ! [ -x "$(command -v wget)" ]; then
  exit_with_error "I require wget but it's not installed.  Aborting."
fi