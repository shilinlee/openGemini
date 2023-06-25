#!/bin/sh
set -e

sed -i 's#/tmp/openGemini/#/var/lib/openGemini/#g' $OPENGEMINI_CONFIG
sed -i 's#/var/lib/openGemini/logs/#/var/log/openGemini/#g' $OPENGEMINI_CONFIG

sed -i 's/bind-address = "127.0.0.1:8086"/bind-address = "0.0.0.0:8086"/g' $OPENGEMINI_CONFIG
sed -i 's/bind-address = "127.0.0.1:8087"/bind-address = "0.0.0.0:8087"/g' $OPENGEMINI_CONFIG

ts-server -config $OPENGEMINI_CONFIG
