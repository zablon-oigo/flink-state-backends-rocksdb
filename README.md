
```sh
docker cp target/original-flink-filter.jar  jobmanager:/opt/flink/usrlib
```

```sh
docker compose exec -it jobmanager bash
```

```sh
cd /opt/flink/usrlib

flink run \
  -c kmart.HighValuePurchaseFilter \
  flink-filter.jar

```
```sh
flink cancel <job_id>

```

```sh
jar tf target/flink-filter.jar
```