#!/bin/sh
#  Create new accounts, like the ga account
#  But other accounts may be create
. $HOME/.exodmrc

  #if [ $# != 2 ]
#then
#    echo "Usage: $0 temperature"
#    exit 255
#fi
# the password (actually erlang node cookie) must be 100% hidden
# so this is only for testing!!!!!

URL=http://localhost:8811
curl -u $USER_AUTH -k -X POST  $URL -d @- << EOF
{
    "jsonrpc": "2.0",
    "method": "register_service",
    "id": "1",
    "params":
    {
      "service": "hvac",
      "network_address": "http://localhost:8901"
    }
}
EOF
