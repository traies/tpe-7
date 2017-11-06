package ar.edu.itba.pod.client;

import ar.edu.itba.pod.*;
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
import org.apache.commons.cli.*;
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
    private static MultiMap<Province, InhabitantRecord> multiMap;
    private static final ExecutorService pool = Executors.newFixedThreadPool(50);

    public static void main(String[] args) {
        logger.info("tpe-7 Client Starting ...");

        Options options = new Options();
        Option ips = Option.builder("D")
                .required(true)
                .valueSeparator('=')
                .hasArgs()
                .longOpt("address")
                .build();
        options.addOption(ips);

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);
            Properties properties = cmd.getOptionProperties("D");

            /* Required parameters */
            String[] addresses = properties.getProperty("address", "127.0.0.1").split(";");
            Integer queryNumber = Integer.valueOf(properties.getProperty("query", "1"));
            String inPath = properties.getProperty("inPath", "censo.csv");
            String outPath = properties.getProperty("outPath", "output.txt");
            String timeOutPath = properties.getProperty("timeOutPath", "time.txt");

            /* Optional Parameters */
            Integer n = Integer.valueOf(properties.getProperty("n", "5"));
            Province prov = Province.getProvince(properties.getProperty("prov", "Buenos Aires"));

            /* Set up timing logger to output to file */
            setLogger(timeOutPath);

            /* Set up Hazelcast, read CSV file and start Job */
            Job<Province, InhabitantRecord> job = hazelcastSetUp(addresses, inPath);
            Query query = new Query(job);

            List<String> list;
            timeLogger.info("Comienzo del trabajo map/reduce.");
            long start = System.currentTimeMillis();

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
                    list = Query.mapToStringList(queryList);
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
                            .map(x -> String.format("%s, %.2f", x.getKey(), x.getValue()) )
                            .collect(Collectors.toList());
                    break;
                }
                case 6: {
                    /* Los​ ​nombres​ ​de​ ​departamentos​ ​que​ ​aparecen​ ​en​ ​al​ ​menos​ ​"n​ "​ ​provincias, ordenado​ ​descendentemente​
                     * por​ ​el​ ​número​ ​de​ ​apariciones
                     */
                    List<Map.Entry<String, Long>> queryList = query.sharedDepartmentsAmongProvices(n);
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
                    break;
            }
            long end = System.currentTimeMillis();
            timeLogger.info("Fin del trabajo map/reduce. Tardo {} segundos", (end - start) / 1000.0);

            /* Write results to output file */
            writeToOutput(list, outPath);
        } catch (ParseException | IOException | InterruptedException | ExecutionException e) {
            logger.error("ERROR", e);
        } finally {
            /* Close client Hazelcast instance */
            Optional.ofNullable(multiMap).ifPresent(DistributedObject::destroy);
            Optional.ofNullable(hz).ifPresent(HazelcastClient::shutdown);
        }
    }

    static Job<Province, InhabitantRecord> hazelcastSetUp(String[] addresses, String path) throws IOException, InterruptedException {
        final ClientConfig ccfg = new ClientConfig();
        GroupConfig groupConfig = ccfg.getGroupConfig();
        groupConfig.setName("tpe-7");
        groupConfig.setPassword("tpe-7");
        ccfg.getNetworkConfig().addAddress(addresses);
        hz = HazelcastClient.newHazelcastClient(ccfg);
        multiMap = hz.getMultiMap(String.format("censoPodGrupo7{%s}", new Date()));
        long start = System.currentTimeMillis();
        timeLogger.info("Inicio de la lectura del archivo.");
        try (Reader r = new FileReader(path)) {
            CSVFormat format = CSVFormat.RFC4180.withHeader(RecordEnum.class);
            for (CSVRecord record : format.parse(r)) {
                pool.execute(() -> {
                    EmploymentCondition condition = EmploymentCondition.getCondition(
                            Integer.valueOf(record.get(RecordEnum.EMPLOYMENT_CONDITION))
                    );
                    Integer homeId = Integer.valueOf(record.get(RecordEnum.HOMEID));
                    String departmentName = record.get(RecordEnum.DEPARTMENT_NAME);
                    Province province = Province.getProvince(record.get(RecordEnum.PROVINCE_NAME));
                    multiMap.put(province, new InhabitantRecord(condition, homeId, departmentName, province));
                });
            }
            pool.shutdown();
            if (!pool.awaitTermination(5, TimeUnit.MINUTES)) {
                pool.shutdownNow();
                throw new RuntimeException("CSV Parsing took too long (more than 5 minutes).");
            }
        }
        long end = System.currentTimeMillis();
        timeLogger.info("Fin de la lectura del archivo. Tardo {} segundos" , (end - start) / 1000.0);
        KeyValueSource<Province, InhabitantRecord> source = KeyValueSource.fromMultiMap(multiMap);
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
