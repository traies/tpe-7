package ar.edu.itba.pod.server;

import ar.edu.itba.pod.EmploymentCondition;
import ar.edu.itba.pod.InhabitantRecord;
import ar.edu.itba.pod.Province;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("tpe-7 Server Starting ...");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        File file = new File("census100.csv");
        IList<InhabitantRecord> list = hz.getList("censoPodGrupo7");
        try (Scanner scanner = new Scanner(file)) {
            String [] fields ;
            while(scanner.hasNextLine()){
                fields = scanner.nextLine().split(",");
                if (fields.length != 4){
                    throw new IllegalArgumentException("Wrong csv format");
                }
                list.add(new InhabitantRecord(EmploymentCondition.getCondition(Integer.valueOf(fields[0])),
                                                           Integer.valueOf(fields[1]),
                                                           fields[2],
                                                           Province.getProvince(fields[3])));


            }
        }catch(FileNotFoundException e){
            logger.error("File not found");
        }
        logger.info(file.getName());


    }
}
