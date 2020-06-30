build:
	docker-compose build

up:
	docker-compose up >output/log 2>&1

zk:
	docker-compose -f docker-compose-zk.yml up >output/log 2>&1