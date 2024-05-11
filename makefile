start:
	docker-compose up

run:
	bash ./flink-1.19.0/bin/flink run .build/libs/.*-all.jar

.PHONY: start run