FROM alpine:3.17

ADD usr/bin/ts-cli usr/bin/ts-meta usr/bin/ts-sql usr/bin/ts-store /usr/local/bin/

COPY docker/scripts/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]