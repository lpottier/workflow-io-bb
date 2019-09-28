FROM alpine:3.10
LABEL maintainer="lpottier@isi.edu"

RUN apk --update --no-cache add bash bash-completion \
    util-linux autoconf coreutils binutils build-base \
    gcc wget


COPY config ./config
COPY burst_buffers_scripts ./burst_buffers_scripts
COPY run ./run
COPY input.tar.bz2 ./

RUN wget https://www.astromatic.net/download/swarp/swarp-2.38.0.tar.gz && \
    tar -zxvf swarp-2.38.0.tar.gz && \
    rm -f swarp-2.38.0.tar.gz && \
    cd swarp-2.38.0 && \
    ./configure --prefix=/swarp-2.38.0-install && \
    make && \
    make check && \
    make install && \
    cd .. && \
    tar -xf input.tar.bz2 && \
    rm -f input.tar.bz2

ENV RESAMP_DIR .
ENV OUTPUT_DIR .