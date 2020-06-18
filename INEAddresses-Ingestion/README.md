# INE Addresses - Ingestion Process

### bdmdata-direc-cod-postales

# Disclaimers

- Actually the narrow tests is using Docker as third-party systems as Kudu.
This framework works with docker-kit dependency and need to launch the properly docker image downloaded before.

The hint to achieve the docker context is run the next command in your console:

```bash
    docker pull usuresearch/kudu-docker-slim:latest
```


## Testing

We have two kinds of testing in the project:

-  Narrow Test (Like Acceptance test with docker as mock system.)
-  Units

By default, unit test profile is enabled. **default-test**.

Install artifact only with Unit Test:
```bash
mvn clean install
```

If you want to run acceptance test you need to put profile as follows:  **qa-test**
**TODO: Currently, QA Testing does not work** 

```bash
mvn clean test -Pqa-test
```

### Start/Stop ####

1.- Run the job.
```bash
cd $HOME/bdmdata-direc-cod-postales/bin
```

2.- Launch Build Master Process
```bash
./buildMasters.sh
```


3.- Launch INE Files ingestion
```bash
./ingestINEFiles.sh
```







