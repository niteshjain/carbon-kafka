FROM ubuntu:16.04
ADD tcpserver /opt/server
EXPOSE 9000/tcp
CMD /opt/server
