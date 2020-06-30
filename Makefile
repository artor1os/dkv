build:
	docker-compose build

up:
	docker-compose up >output/log 2>&1

zk-master:
	docker-compose -f docker-compose-zk-master.yml up >output/log 2>&1

zk-replica:
	docker-compose -f docker-compose-zk-master-replica.yml up >output/log 2>&1