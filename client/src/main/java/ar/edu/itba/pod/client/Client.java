package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.*;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static ch.qos.logback.classic.Logger timeLogger = ((LoggerContext)LoggerFactory.getILoggerFactory()).getLogger(Client.class);
    private static HazelcastInstance hz;
    private static IMap<Long, InhabitantRecord> map;
    private static final ExecutorService pool = Executors.newFixedThreadPool(50);
    private static final Long MAP_INSERT_CHUNK = 5000L;

    public static void main(String[] args) {
        logger.info("tpe-7 Client Starting ...");


        try {
            /* Required parameters */
            String[] addresses = Optional.ofNullable(System.getProperty("addresses")).map( x -> x.split(";")).orElseThrow(() ->
                    new MissingArgumentException("Missing addresses property")
            );

            Integer queryNumber = Optional.ofNullable(System.getProperty("query")).map(Integer::valueOf).orElseThrow(() ->
                    new MissingArgumentException("Missing queryNumber property")
            );

            String inPath = Optional.ofNullable(System.getProperty("inPath")).orElseThrow(() ->
                    new MissingArgumentException("Missing inPath property")
            );

            String outPath = Optional.ofNullable(System.getProperty("outPath")).orElseThrow(() ->
                    new MissingArgumentException("Missing outPath property")
            );
            String timeOutPath =  Optional.ofNullable(System.getProperty("timeOutPath")).orElseThrow(() ->
                    new MissingArgumentException("Missing timeOutPath property")
            );

            /* Optional Parameters */
            Integer n =  Optional.ofNullable(System.getProperty("n")).map(Integer::valueOf).orElse(null);

            Province prov = Optional.ofNullable(System.getProperty("prov")).map(Province::getProvince).orElse(null);

            /* Check queries 2, 6 and 7 have necessary parameters */
            switch (queryNumber) {
                case 2:
                    if (prov == null) {
                        throw new MissingArgumentException("Query 2 needs prov property");
                    }
                case 6:
                case 7:
                    if (n == null) {
                        throw new MissingArgumentException(String.format("Query %d needs n property", queryNumber));
                    }
                    break;
                case 1:
                case 3:
                case 4:
                case 5:
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Query %d is not available", queryNumber));
            }

            /* Set up timing logger to output to file */
            setLogger(timeOutPath);

            /* Set up Hazelcast, read CSV file and start Job */
            Job<Long, InhabitantRecord> job = hazelcastSetUp(addresses, inPath, InhabitantRecordSerializationMode.getMode(queryNumber));
            Query query = new Query(job);

            List<String> list;
            timeLogger.info("Comienzo del trabajo map/reduce.");
            Long start = System.currentTimeMillis();
            /* Switch on map/reduce query to perform */
            switch (queryNumber) {
                case 1: {
                    /* Total​ ​de​ ​habitantes​ ​por​ ​región​ ​del​ ​país,​ ​ordenado​ ​descendentemente​ ​por​ ​el total​ ​de​ ​habitantes. */
                    List<Map.Entry<Region, Long>> queryList = query.populationPerRegion();
                    list = Query.mapToStringList(queryList);
                    break;
                }
                case 2: {
                    /* Los​ ​"​n"​ ​departamentos​ ​más​ ​habitados​ ​de​ ​la​ ​provincia​ ​"p​rov". */
                    List<Map.Entry<String, Long>> queryList = query.nDepartmentsByPopulation(prov, n);
                    list = Query.mapToStringList(queryList);
                    break;
                }
                case 3: {
                    /* Índice​ ​de​ ​desempleo​ ​por​ ​cada​ ​región​ ​del​ ​país,​ ​ordenado​ ​descendentemente por​ ​el​ ​índice​ ​de​
                     *  ​desempleo
                     */
                    List<Map.Entry<Region, Double>> queryList = query.employmentPerRegion();
                    list = queryList.stream()
                            .map(x -> String.format(Locale.US, "%s, %.2f", x.getKey(), x.getValue()) )
                            .collect(Collectors.toList());
                    break;
                }
                case 4: {
                    /* Total​ ​de​ ​hogares​ ​por​ ​cada​ ​región​ ​del​ ​país​ ​ordenado​ ​descendentemente​ ​por​ ​el total​ ​de​ ​hogares. */
                    List<Map.Entry<Region, Integer>> queryList = query.householdsPerRegion();
                    list = Query.mapToStringList(queryList);
                    break;
                }
                case 5: {
                    /*  Por​ ​cada​ ​región​ ​del​ ​país​ ​el​ ​promedio​ ​de​ ​habitantes​ ​por​ ​hogar,​ ​ordenado descendentemente​ ​por​ ​el
                     *​  ​promedio​ ​de​ ​habitantes,​ ​mostrándose​ ​siempre​ ​dos decimales.
                     */
                    List<Map.Entry<Region, Double>> queryList = query.householdRatioPerRegion();

                    list = queryList.stream()
                            .map(x -> String.format(Locale.US, "%s, %.2f", x.getKey(), x.getValue()) )
                            .collect(Collectors.toList());
                    break;
                }
                case 6: {
                    /* Los​ ​nombres​ ​de​ ​departamentos​ ​que​ ​aparecen​ ​en​ ​al​ ​menos​ ​"n​ "​ ​provincias, ordenado​ ​descendentemente​
                     * por​ ​el​ ​número​ ​de​ ​apariciones
                     */
                    List<Map.Entry<String, Integer>> queryList = query.sharedDepartmentsAmongProvices(n);
                    list = Query.mapToStringList(queryList);
                    break;
                }
                case 7: {
                    /* Los​ ​pares​ ​de​ ​provincias​ ​que​ ​comparten​ ​al​ ​menos​ ​"n​ "​ ​nombres​ ​de departamentos,​ ​ordenado​ ​
                     * descendentemente​ ​por​ ​la​ ​cantidad​ ​de​ ​coincidencias
                     */
                    List<Map.Entry<ProvincePair, Long>> queryList = query.pairsOfProvincesThatHaveSharedDepartments(n);
                    list = Query.mapToStringList(queryList);
                    break;
                }
                default:
                    list = new ArrayList<>();
                    logger.warn("invalid query requested.");
                    start = System.currentTimeMillis();
                    break;
            }
            long end = System.currentTimeMillis();
            timeLogger.info("Fin del trabajo map/reduce. Tardo {} segundos", (end - start) / 1000.0);

            /* Write results to output file */
            writeToOutput(list, outPath);
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error("ERROR: {}", e.getMessage());
        } catch (MissingArgumentException | IllegalArgumentException e) {
            logger.error("User error: {}", e.getMessage());
        } finally {
            /* Close client Hazelcast instance */
            Optional.ofNullable(map).ifPresent(DistributedObject::destroy);
            Optional.ofNullable(hz).ifPresent(HazelcastClient::shutdown);
        }
    }

    private static Job<Long, InhabitantRecord> hazelcastSetUp(String[] addresses, String path, InhabitantRecordSerializationMode mode) throws IOException, InterruptedException {
        final ClientConfig ccfg = new ClientConfig();
        GroupConfig groupConfig = ccfg.getGroupConfig();
        groupConfig.setName("tpe-7");
        groupConfig.setPassword("tpe-7");
        ccfg.getNetworkConfig().addAddress(addresses);
        hz = HazelcastClient.newHazelcastClient(ccfg);
        map = hz.getMap(String.format("censoPodGrupo7{%s}", new Date()));
        long start = System.currentTimeMillis();
        timeLogger.info("Inicio de la lectura del archivo.");
        try (Reader r = new FileReader(path)) {
            CSVFormat format = CSVFormat.RFC4180.withHeader(RecordEnum.class);
            Long id = 0L;
            Iterator<CSVRecord> iterator = format.parse(r).iterator();

            while (iterator.hasNext()) {
                Map<Long, InhabitantRecord> recordMap = new HashMap<>();
                for (int i = 0; i < MAP_INSERT_CHUNK  && iterator.hasNext(); i++) {
                    CSVRecord record = iterator.next();
                    Long current = id + i;
                    EmploymentCondition condition = EmploymentCondition.getCondition(
                            Integer.valueOf(record.get(RecordEnum.EMPLOYMENT_CONDITION))
                    );
                    Integer homeId = Integer.valueOf(record.get(RecordEnum.HOMEID));
                    String departmentName = record.get(RecordEnum.DEPARTMENT_NAME);
                    Province province = Province.getProvince(record.get(RecordEnum.PROVINCE_NAME));

                    recordMap.put(current, InhabitantRecordFactory.create(condition, homeId, departmentName, province, mode));
                }
                pool.execute(() -> map.putAll(recordMap));
                id += MAP_INSERT_CHUNK;
            }
            pool.shutdown();
            if (!pool.awaitTermination(5, TimeUnit.MINUTES)) {
                pool.shutdownNow();
                throw new RuntimeException("CSV Parsing took too long (more than 5 minutes).");
            }


        }
        long end = System.currentTimeMillis();
        timeLogger.info("Fin de la lectura del archivo. Tardo {} segundos" , (end - start) / 1000.0);
        KeyValueSource<Long, InhabitantRecord> source = KeyValueSource.fromMap(map);
        JobTracker jobTracker = hz.getJobTracker("test");
        return jobTracker.newJob(source);
    }

    private static void writeToOutput(List<String> list, String path) {
        try (Writer w = new FileWriter(path)) {
            for (String string: list) {
                w.append(string);
                w.append('\n');
            }
        } catch (IOException e) {
            logger.error("ERROR {}", e);
        }
    }

    private static void setLogger(String timeOutPath) {
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext((LoggerContext)LoggerFactory.getILoggerFactory());
        encoder.setPattern("%d{dd/MM/yyyy HH:mm:ss.SSS} %-5level[%thread] %logger{36}:%line - %msg %n");
        encoder.start();

        FileAppender<ILoggingEvent> appender = new FileAppender<>();
        appender.setFile(timeOutPath);
        appender.setContext((LoggerContext)LoggerFactory.getILoggerFactory());
        appender.setEncoder(encoder);
        appender.setAppend(false);
        appender.start();
        timeLogger.addAppender(appender);
    }
}
