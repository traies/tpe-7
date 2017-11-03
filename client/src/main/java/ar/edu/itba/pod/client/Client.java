package ar.edu.itba.pod.client;

import ar.edu.itba.pod.EmploymentCondition;
import ar.edu.itba.pod.InhabitantRecord;
import ar.edu.itba.pod.Province;
import ar.edu.itba.pod.Region;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
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
import java.net.InetAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws FileNotFoundException, ExecutionException, InterruptedException {
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

            String[] addresses = properties.getProperty("address", "127.0.0.1").split(";");
            Integer queryNumber = Integer.valueOf(properties.getProperty("query", "1"));
            String inPath = properties.getProperty("inPath", "censo.csv");
            String outPath = properties.getProperty("outPath", "output.txt");
            String timeOutPath = properties.getProperty("timeOutPath", "time.txt");

            /* Optional Parameters */
            Integer n = Integer.valueOf(properties.getProperty("n", "1"));
            String prov = properties.getProperty("prov", "Buenos Aires");


            Job<Province, InhabitantRecord> job = hazelcastSetUp(addresses, inPath);
            Query query = new Query(job);
            List<String> list;
            switch (queryNumber) {
                case 1:
                    Map<Region, Long> queryMap = query.populationPerRegion();
                    logger.debug("result {}", queryMap.toString());
                    list = queryMap.entrySet().stream()
                            .map(x -> String.format("%s, %s",  x.getKey(), x.getValue()))
                            .collect(Collectors.toList());
                    break;
                case 2:
                    List<Map.Entry<String, Long>> queryList = query.nDepartmentsByPopulation(n);
                    logger.debug("result {}", queryList.toString());
                    list = queryList.stream().map(x -> String.format("%s, %s", x.getKey(), x.getValue()))
                            .collect(Collectors.toList());
                    break;
                default:
                    list = new ArrayList<>();
                    logger.warn("invalid query requested.");
                    break;
            }
            writeToOutput(list, outPath);

        } catch (ParseException e) {
            logger.error("ERROR", e);
        }
    }

    public static Job<Province, InhabitantRecord> hazelcastSetUp(String[] addresses, String path) {
        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("tpe-7");
        ccfg.getGroupConfig().setPassword("tpe-7");
        ccfg.getNetworkConfig().addAddress(addresses);
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(ccfg);
        MultiMap<Province, InhabitantRecord> map = hz.getMultiMap("censoPodGrupo7");

        try (Reader r = new FileReader(path)) {
            CSVFormat format = CSVFormat.RFC4180.withHeader(RecordEnum.class);
            for (CSVRecord record : format.parse(r)) {
                EmploymentCondition condition = EmploymentCondition.getCondition(
                        Integer.valueOf(record.get(RecordEnum.EMPLOYMENT_CONDITION))
                );
                Integer homeId = Integer.valueOf(record.get(RecordEnum.HOMEID));
                String departmentName = record.get(RecordEnum.DEPARTMENT_NAME);
                Province province = Province.getProvince(record.get(RecordEnum.PROVINCE_NAME));
                map.put(province, new InhabitantRecord(condition, homeId, departmentName, province));
            }
        } catch (IOException e) {
            logger.error("ERROR", e);
        }
        KeyValueSource<Province, InhabitantRecord> source = KeyValueSource.fromMultiMap(map);
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
}
