build: $(wildcard *.go) $(wildcard **/*.go)
	mkdir -p output
	docker-compose build

up: build
	docker-compose up >output/log 2>&1

zk-master: build
	docker-compose -f docker-compose-zk-master.yml up >output/log 2>&1

zk-replica: build
	docker-compose -f docker-compose-zk-master-replica.yml up >output/log 2>&1

myzk-master: build
	(cd docker/myzk && sudo sh re-init.sh)
	docker-compose -f docker-compose-myzk-master.yml up >output/log 2>&1

myzk-replica: build
	(cd docker/myzk && sudo sh re-init.sh)
	docker-compose -f docker-compose-myzk-master-replica.yml up >output/log 2>&1
