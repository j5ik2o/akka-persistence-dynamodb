#!/usr/bin/env bash

docker-compose down -v --remove-orphans "$@"
