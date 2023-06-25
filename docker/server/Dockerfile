FROM alpine:3.17

#ADD build/ts-server /usr/local/bin/
ADD config/openGemini.singlenode.conf /etc/openGemini/openGemini.singlenode.conf
COPY docker/server/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 8086 8087

VOLUME /var/lib/openGemini
VOLUME /var/log/openGemini

ENV OPENGEMINI_CONFIG /etc/openGemini/openGemini.singlenode.conf

ENTRYPOINT ["/entrypoint.sh"]