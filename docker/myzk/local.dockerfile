FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash tar \
    && mkdir -p /zookeeper

ADD apache-zookeeper-3.6.1-bin.tar.gz /

RUN tar -C /zookeeper -xzf /apache-zookeeper-3.6.1-bin.tar.gz --strip-components=1 \
    && cp /zookeeper/conf/zoo_sample.cfg /zookeeper/conf/zoo.cfg \
    && mkdir -p /data

EXPOSE 2181 2888 3888

WORKDIR /zookeeper

VOLUME ["/zookeeper/conf", "/data"]

ENTRYPOINT ["/zookeeper/bin/zkServer.sh"]
CMD ["start-foreground"]
