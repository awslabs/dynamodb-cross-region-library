FROM ubuntu

# Update packages
RUN apt-get update

# Install supervisord
RUN apt-get -y install openjdk-7-jre python-setuptools wget jq
RUN easy_install supervisor awscli

WORKDIR /opt

# Download DynamoDB Replication Coordinator jar files
ADD https://s3.amazonaws.com/dynamodb-cross-region/DynamoDBReplicationCoordinator.jar ./
ADD https://s3.amazonaws.com/dynamodb-cross-region/DynamoDBReplicationServer.jar ./
RUN chmod a+r /opt/DynamoDBReplication*.jar

# Copy scripts to prepare envinroments
ADD scripts ./scripts

EXPOSE 7000

CMD bash /opt/scripts/start.sh
