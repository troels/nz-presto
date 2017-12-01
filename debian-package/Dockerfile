FROM sal:5000/bts/ubuntu:latest

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y uuid-runtime && \
    apt-get autoremove --purge -y && \
    apt-get upgrade -y && \
    apt-get clean

COPY releases/presto-server_*.deb /
RUN dpkg -i /presto-server_*.deb
RUN rm -f /presto-server_*.deb

