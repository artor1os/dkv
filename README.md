# Distributed Key-Value Store

## Usage

```sh
make zk-master # use zookeeper official image and use zookeeper as config queue
or
make myzk-master # use my built zookeeper image
```

or

```sh
make zk-replica # use zookeeper official image and use zookeeper as config queue and for log replication
or
make myzk-replica # use my built zookeeper image
```

### Get

GET http://localhost:8080/get?key=aaa

### Put

GET http://localhost:8080/put?key=aaa&value=bbb

### Append

GET http://localhost:8080/append?key=aaa&value=222

### Delete

GET http://localhost:8080/delete?key=aaa

### Join(only for testing)

POST http://localhost:8080/join

Body:
{
    "101": 3
    "100": 3
}

### Leave(only for testing)

POST http://localhost:8080/leave

Body:
[101]
