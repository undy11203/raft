#!/usr/bin/env bash
set -euo pipefail
trap 'read -r -p "
Нажмите Enter для закрытия..." _' EXIT

if [ "$#" -ne 2 ]; then
    echo "Использование: $0 <ip:port> <строка>" >&2
    exit 1
fi

payload="$(printf '%s' "$2" | python -c 'import json, sys; print(json.dumps({"command": sys.stdin.read()}))')"

curl --silent --show-error --fail-with-body \
    --request POST "http://$1/add-entry" \
    --header "Content-Type: application/json" \
    --data-binary "$payload"
