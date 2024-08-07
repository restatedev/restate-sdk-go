# Test services to run the sdk-test-suite

## To run locally

* Grab the release of sdk-test-suite: https://github.com/restatedev/sdk-test-suite/releases

* Prepare the docker image: 
```shell
KO_DOCKER_REPO=restatedev ko build -B -L github.com/restatedev/sdk-go/test-services
```

* Run the tests (requires JVM >= 17):
```shell
java -jar restate-sdk-test-suite.jar run --exclusions-file exclusions.yaml restatedev/test-services
```

## To debug a single test:

* Run the golang service using your IDE
* Run the test runner in debug mode specifying test suite and test:
```shell
java -jar restate-sdk-test-suite.jar debug --image-pull-policy=CACHED --test-config=lazyState --test-name=dev.restate.sdktesting.tests.State default-service=9080
```

For more info: https://github.com/restatedev/sdk-test-suite