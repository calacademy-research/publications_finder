#!/bin/bash
# docker run --name some-mysql -e MYSQL_DATABASE=publications -e MYSQL_ROOT_PASSWORD=0b5aTlERkgMBs -p 3399:3306 -d mysql

docker run --name works_mysql -e MYSQL_DATABASE=works -e MYSQL_ROOT_PASSWORD=0b5aTlERkgMBs -p 3399:3306 -d mysql


