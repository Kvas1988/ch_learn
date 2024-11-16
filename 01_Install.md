---
date: 01.10.2024
tags:
  - database
  - clickhouse
---
# Install
## Docker

https://hub.docker.com/r/clickhouse/clickhouse-server/
`docker pull clickhouse/clickhouse-server`

### networking

You can expose your ClickHouse running in docker by [mapping a particular port⁠](https://docs.docker.com/config/containers/container-networking/) from inside the container using host ports:

```bash
docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
echo 'SELECT version()' | curl 'http://localhost:18123/' --data-binary @-
```

Connect via native client:
```bash
docker exec -it some-clickhouse-server clickhouse-client
```
### Volumes

Typically you may want to mount the following folders inside your container to achieve persistency:

- `/var/lib/clickhouse/` - main folder where ClickHouse stores the data
- `/var/log/clickhouse-server/` - logs

```bash
docker run -d \
    -v $(realpath ./ch_data):/var/lib/clickhouse/ \
    -v $(realpath ./ch_logs):/var/log/clickhouse-server/ \
    --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
	```

You may also want to mount:

- `/etc/clickhouse-server/config.d/*.xml` - files with server configuration adjustmenets
- `/etc/clickhouse-server/users.d/*.xml` - files with user settings adjustmenets
- `/docker-entrypoint-initdb.d/` - folder with database initialization scripts (see below).

OR simple **docker-compose.yaml** file:
```yaml
version: '3.8'
services:
  clickhouse:
    image: 'clickhouse/clickhouse-server:${CHVER:-latest}'
    # user: '101:101'
    container_name: clickhouse
    hostname: clickhouse
    volumes:
      - ${PWD}/fs/volumes/clickhouse/etc/clickhouse-server/config.d/config.xml:/etc/clickhouse-server/config.d/config.xml
      - ${PWD}/fs/volumes/clickhouse/etc/clickhouse-server/users.d/users.xml:/etc/clickhouse-server/users.d/users.xml
    ports:
      - '127.0.0.1:8123:8123'
      - '127.0.0.1:9000:9000'
```

```
docker-compose up --build -d
```

# Resources
- [Clickhouse Learn](https://learn.clickhouse.com)
- [Clickhouse Learn repo](https://github.com/ClickHouse/clickhouse-academy/tree/main)
- [Documentation](https://clickhouse.com/docs)
- [MViews in Clickhouse (Blogpost)](https://clickhouse.com/blog/using-materialized-views-in-clickhouse)
- [Joins in Clickhouse (Series of Blogpost)](https://clickhouse.com/blog/clickhouse-fully-supports-joins-part1)

