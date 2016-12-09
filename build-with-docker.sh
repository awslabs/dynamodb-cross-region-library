docker run -it --rm --name dynamodb-cross-region-library -v "$PWD":/usr/src/mymaven -w /usr/src/mymaven maven:3.2-jdk-7 mvn clean install
docker build -t vungle/dynamodb-cross-region-replication:1.1.0 .
