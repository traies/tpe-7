package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public enum InhabitantRecordSerializationMode implements InhabitantRecordMarshaller{

    /* EmploymentCondition, HomeId, DepartmentName, Province */
    QUERY_0(0x01, false, false, false, true),
    QUERY_1(0x02, false, false, true, true),
    QUERY_2(0x03, true, false, false, true),
    QUERY_3(0x04, false, true, false, true),
    QUERY_4(0x05, false, true, false, true),
    QUERY_5(0x06, false, false, true, true),
    QUERY_6(0x07, false, false, true, true);

    private int b;
    private boolean condition, homeId, departmentName, province;

    InhabitantRecordSerializationMode(int b, boolean condition, boolean homeId, boolean departmentName, boolean province){
        this.b = b;
        this.condition = condition;
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.province = province;
    }

    public static InhabitantRecordSerializationMode getMode(int b) {
        InhabitantRecordSerializationMode mode;
        switch (b) {
            case 0x01:
                mode = QUERY_0;
                break;
            case 0x02:
                mode = QUERY_1;
                break;
            case 0x03:
                mode = QUERY_2;
                break;
            case 0x04:
                mode = QUERY_3;
                break;
            case 0x05:
                mode = QUERY_4;
                break;
            case 0x06:
                mode = QUERY_5;
                break;
            case 0x07:
                mode = QUERY_6;
                break;
            default:
                throw new RuntimeException(String.format("Unknown serialization mode %X", b));
        }
        return mode;
    }

    public boolean isCondition() {
        return condition;
    }

    public boolean isHomeId() {
        return homeId;
    }

    public boolean isDepartmentName() {
        return departmentName;
    }

    public boolean isProvince() {
        return province;
    }

    public void serialize(InhabitantRecord record, ObjectDataOutput objectDataOutput) throws IOException{
        objectDataOutput.writeByte(this.b);
        if (condition) {
            objectDataOutput.writeInt(record.getCondition().getNumber());
        }
        if (homeId) {
            objectDataOutput.writeInt(record.getHomeId());
        }
        if (departmentName) {
            objectDataOutput.writeUTF(record.getDepartmentName());
        }
        if (province) {
            objectDataOutput.writeInt(record.getProvince().ordinal());
        }
    }
    public void deserialize(InhabitantRecord record, ObjectDataInput objectDataInput) throws IOException{
        if (condition)
            record.setCondition(EmploymentCondition.getCondition(objectDataInput.readInt()));
        if (homeId)
            record.setHomeId(objectDataInput.readInt());
        if (departmentName)
            record.setDepartmentName(objectDataInput.readUTF());
        if (province)
            record.setProvince(Province.getProvinceByOrdinal(objectDataInput.readInt()));
    }
}
