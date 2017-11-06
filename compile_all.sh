#!/bin/bash

mvn clean package;
cd server/target/;
tar -xvf tpe-7-server-1.0-SNAPSHOT-bin.tar.gz;
cd tpe-7-server-1.0-SNAPSHOT;
chmod +x run-server.sh; 
cd ../../../client/target;
tar -xvf tpe-7-client-1.0-SNAPSHOT-bin.tar.gz;
cd tpe-7-client-1.0-SNAPSHOT;
chmod +x run-client.sh;
