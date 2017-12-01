# Got basic install from https://github.com/frol/docker-alpine-python3/blob/master/Dockerfile
FROM alpine:3.4

RUN apk add --no-cache python3 bash git openssh && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache

# This will have to be replaced by github if we ever make it public
RUN mkdir /tmp/jass
COPY . /opt/jass
RUN cd /opt/jass && pip install -e .
#& rm -rf /tmp/jass

EXPOSE 8888

# may need to change the workdir
WORKDIR /opt/jass

# tornado runs its own HTTP server.
# http://www.tornadoweb.org/en/stable/guide/running.html
ENTRYPOINT python3 -m jassrealtime.webapi.app