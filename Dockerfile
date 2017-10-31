FROM ubuntu:artful

RUN apt-get update && \
    apt-get install -y python3-setuptools python3-pip

ADD . /tmp/msgbus/

RUN pip3 install pyzmq==16.0.2 && \
    cd /tmp/msgbus/ ; python3 setup.py install

USER nobody

EXPOSE 7000 7001

ENTRYPOINT ["mbusd"]
