## Origin of source code

The source code orginates from an AWS sample project on github:

https://github.com/aws-samples/emr-presto-query-event-listener

It has been adapted for our current presto version and for more verbose
log output.


## Documentation of the sample project

The AWS sample project is documented here:

https://aws.amazon.com/blogs/big-data/custom-log-presto-query-events-on-amazon-emr-for-auditing-and-performance-insights/


## Build and use in a docker

Build the jar file:

  mvn clean package 

Copy it to a running presto docker container (in the example below with
the id 6d1a65ae8824):

  docker cp target/QueryEventListener-1.0.jar 6d1a65ae8824:/usr/share/presto/plugin/query

Ensure presto event-listener is configured inside the docker.
Attach to the presto docker container and do:

  echo "event-listener.name=event-listener" >> /etc/presto/event-listener.properties

Restart the presto docker container using 'docker restart ...'.

