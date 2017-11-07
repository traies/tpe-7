# HAZELCAST

Realizar los cambios necesarios en en archivo Hazelcast.xml tanto en "server/src/main/resources/" como en "client/src/main/resources/"

# COMPILACION

El proyecto es un proyecto Maven.

Ejecutar ./compile_all.sh en en la raíz del repositorio. Esto hace "mvn clean package" y prepara scripts de ejecución.

# EJECUCIÓN DEL SERVER

Utilizando el script ./compile_all.sh :

En "server/target/tpe-7-server-1.0-SNAPSHOT/" ejecutar ./run_server.sh

# EJECUCIÓN DEL CLIENTE

En "client/target/tpe-7-client-1.0-SNAPSHOT" ejecutar, por ejemplo: 

java   -Daddresses="192.168.0.31" -DinPath="path al archivo del censo" -Dquery=1 -DoutPath=benchmark/output1.csv   -DtimeOutPath=benchmark/time1.csv  -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"

para correr la query 1 
