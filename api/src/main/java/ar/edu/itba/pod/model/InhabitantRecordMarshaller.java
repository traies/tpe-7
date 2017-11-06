package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public interface InhabitantRecordMarshaller {
    void serialize(InhabitantRecord record, ObjectDataOutput dataOutput) throws IOException;
    void deserialize(InhabitantRecord record, ObjectDataInput dataInput) throws IOException;
}
