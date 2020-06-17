# ReservasStock

### bdomnch-reservas-stock-atp

# Disclaimers

- Actually the acceptance tests is using Docker as third-party systems as Kudu.
This framework works with docker-kit dependency and need to launch the properly docker image downloaded before.

The hint to achieve the docker context is run the next command in your console:

```bash
    docker pull usuresearch/kudu-docker-slim:latest
    docker pull spotify/kafka:latest
```


## Testing

We have two kinds of testing in the project:

-  Acceptance Test (Like Acceptance test with docker as mock system.)
-  Units

By default, unit test profile is enabled. **default-test**.

Install artifact only with Unit Test:
```bash
mvn clean install
```

If you want to run acceptance test you need to put profile as follows:  **qa-test**

```bash
mvn clean test -Pqa-test
```


### Start/Stop ####

- Packaging with nominal user profile in Desa environment:

```bash
mvn clean package -Pdesa_<user_name>
```

- Run the job:

cd /proyectos/omnch/streaming/bdomnch-reservas-stock-atp/bin

Launch Customer reservation process

```bash
./customerOrderReservationStreaming.sh
```

### Detected problems ###

- If you can't run the tests through intellij just run from console

```bash
mvn clean test -Pqa-test
```
then you should be able to run the tests from intellij


- If you see some conectivity error referring to spotify-kafka, just run
```bash
sudo netstat -npa | grep 9092
```

and kill the process








