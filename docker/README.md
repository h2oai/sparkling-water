# Docker Support

## Building a container

```
$ ./build.sh
```

## Run bash inside container

```
$ docker run  -i -t sparkling-test-base /bin/bash
```

## Run Sparkling Shell inside container

```
docker run -i -t --rm sparkling-test-base bin/sparkling-shell 
```

## Running examples in container

```
docker run -i -t --rm sparkling-test-base bin/run-example.sh
```
