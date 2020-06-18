# KafkaToKudu

Proyecto 'KafkaToKudu' para ingestas streaming al Datalake. Recoge eventos de varios topic de Kafka
y los va ingestando en su correspondiente tabla en Apache Kudu. **Event Sourcing**


##### Descripción:
* Lectura de los topic kafka configurados.
* Parseo de datos a schema kudu. **IMPORTANTE**: Aquellos campos que esten declarados en Kudu como timestamp deben de enviarse con el siguiente formato: "yyyy-MM-dd HH:mm:ss"
* Inserción en Datalake - Kudu.

##### Tests

Hay dos perfiles de tests: uno para tests unitarios, y otro para tests de integración
usando Docker. Estos últimos están en un perfil separado.

Para lanzar los tests de integración es necesario añadir el usuario actual al
grupo de Docker y reiniciar: `sudo usermod -aG docker $USER`

Lanzamiento de tests unitarios (default-test profile): `mvn test`

Lanzamiento de tests de integración: `mvn -Pnarrow-test test`
 
### ARRANQUE Y PARADA ####

1.- Ejecución de script levantar el job.

      cd $HOME/bdarq-integracion-kudu/bin
      ./bdarq-integracion-kudu.sh.sh stream_comprod_arqin cluster
      
2.- Parada del Job en CUA:
    hdfs dfs -rm /user/stream_comcua_arqin/.graceful_stop/sparkStreaming.dontRemove.file      
 
    
##### Configuración:

  1.- Configurar los tópicos de Kafka y su base de datos asociada de Kudu en el fichero *application.conf*. Por ejemplo:
   ```
     topic = [
       {name = "mdtin_bdi_sap_datalake", description = "Eventos de tablas asociadas a esquema mdata_stream", schema = "mdata_stream", max_message_bytes = 1000000, format = "json"},
       {name = "supin_bdi_sap_datalake", description = "Eventos de tablas asociadas a esquema supply_stream", schema = "supply_stream", max_message_bytes = 1000000, format = "json"},
       {name = "salin_bdi_sap_datalake", description = "Eventos de tablas asociadas a esquema sales_stream", schema = "sales_stream", max_message_bytes = 1000000, format = "json"},
       {name = "astin_bdi_sap_datalake", description = "Eventos de tablas asociadas a esquema assortment_stream", schema = "assortment_stream", max_message_bytes = 1000000, format = "json"},
       {name = "negin_bdi_sap_datalake", description = "Eventos de tablas asociadas a esquema negotiation_stream", schema = "negotiation_stream", max_message_bytes = 1000000, format = "json"}
     ]
   ```

   2.- Configurar los parámetros del proceso en el fichero *application.conf*. En particular el fichero en hdfs para la parada controlada (el usuario con el que se ejecute el 
   proceso debe tener permisos para crearlo) y también el grupo de consumo de Kafka si se ejecutan varias instancias del proceso.
   ```
    parameters = {
      sparkMaster="yarn"
      kuduHost="localhost:7051"
      kafkaConsumerGroup="stream_comprod_arqin-gid"
      kafkaServers="localhost:9092"
      kafkaWithKerberos=true
      sparkBatchDuration=5
      kuduMaxNumberOfColumns=296
      gracefulShutdownFile="/user/stream_comprod_arqin/.graceful_stop/sparkStreaming.dontRemove.file"
      datalakeUser="stream_comprod_arqin"
      historyTable="storeoperations_stream.zcc_logcont_head,storeoperations_stream.zcc_logcont_item,mdata_stream.scwm_binmat,supply_stream.daily_valuated_stock,supply_stream.monthly_valuated_stock"
    }
   ```

   3.- Modificar el fichero *kafka_jaas_ex.conf* con el fichero keytab de kerberos y el principal del usuario. 
   ```
   KafkaClient {
     com.sun.security.auth.module.Krb5LoginModule required
     useKeyTab=true
     keyTab="stream_comprod_arqin_kafka.keytab"
     serviceName="kafka"
     principal="stream_comprod_arqin@DOMAIN";
   }
   ```
   En el HOME del usuario deben de existir los ficheros *${HOME}/${USER}.keytab* y *${HOME}/${USER}_kafka.keytab* con el keytab de kerberos.




