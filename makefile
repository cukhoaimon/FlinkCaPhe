start:
	docker-compose up

build:
	mvn install

run19:
	bash ./flink-*19*/bin/flink run target/app-*-shaded.jar

run18:
	bash ./flink-*18*/bin/flink run target/app-*-shaded.jar

start_cluster:
	bash ./flink-1.19.0/bin/start-cluster.sh


.PHONY: start run18 run19 build start_cluster