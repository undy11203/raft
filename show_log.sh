#!/usr/bin/env bash
set -euo pipefail
trap 'read -r -p "
Нажмите Enter для закрытия..." _' EXIT

if [ "$#" -ne 1 ]; then
    echo "Использование: $0 <ip:port>" >&2
    exit 1
fi

curl --silent --show-error --fail-with-body \
    --request GET "http://$1/show-log" | python -m json.tool
