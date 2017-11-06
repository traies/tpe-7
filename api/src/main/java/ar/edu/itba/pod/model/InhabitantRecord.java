package ar.edu.itba.pod.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
/**
 * Created by traies on 27/10/17.
 */
public class InhabitantRecord implements DataSerializable {
    private static int ID = 0;

    private int id;
    private EmploymentCondition condition ;
    private Integer homeId;
    private String departmentName;
    private Province province;
    private InhabitantRecordSerializationMode serializationMode;

    public InhabitantRecord() {

    }

    public InhabitantRecord(EmploymentCondition condition, Integer homeId, String departmentName, Province province, InhabitantRecordSerializationMode serializationMode) {
        id = ID++;
        this.condition = condition;
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.province = province;
        this.serializationMode = serializationMode;
    }

    public EmploymentCondition getCondition() {
        return condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InhabitantRecord that = (InhabitantRecord) o;

        return id == that.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public InhabitantRecordSerializationMode getSerializationMode() {
        return serializationMode;
    }

    public Integer getHomeId() {
        return homeId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public Province getProvince() {
        return province;
    }

    public void setCondition(EmploymentCondition condition) {
        this.condition = condition;
    }

    public void setHomeId(Integer homeId) {
        this.homeId = homeId;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public void setProvince(Province province) {
        this.province = province;
    }

    public String toString() {
        return "[ Estado:"+this.condition.name()+", Hogar: "+this.homeId+", Departamento: "+this.departmentName+", Provincia: "+this.province.name()+"]";
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        getSerializationMode().serialize(this, objectDataOutput);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        serializationMode = InhabitantRecordSerializationMode.getMode(objectDataInput.readByte());
        serializationMode.deserialize(this, objectDataInput);
    }
}
