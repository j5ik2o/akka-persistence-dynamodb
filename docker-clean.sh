#!/bin/sh

docker ps -aq | xargs docker rm -f
docker network ls -f name=kafka -q | xargs docker network rm
docker images -aq | xargs docker rmi -f
