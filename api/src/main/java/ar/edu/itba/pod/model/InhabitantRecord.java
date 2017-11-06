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

    public InhabitantRecord() {

    }

    public InhabitantRecord(EmploymentCondition condition, Integer homeId, String departmentName, Province province) {
        id = ID++;
        this.condition = condition;
        this.homeId = homeId;
        this.departmentName = departmentName;
        this.province = province;
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

    public Integer getHomeId() {
        return homeId;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public Province getProvince() {
        return province;
    }

    public String toString() {
        return "[ Estado:"+this.condition.name()+", Hogar: "+this.homeId+", Departamento: "+this.departmentName+", Provincia: "+this.province.name()+"]";
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(condition.getNumber());
        objectDataOutput.writeInt(homeId);
        objectDataOutput.writeUTF(departmentName);
        objectDataOutput.writeUTF(province.getName());
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        condition = EmploymentCondition.getCondition(objectDataInput.readInt());
        homeId = objectDataInput.readInt();
        departmentName = objectDataInput.readUTF();
        province = Province.getProvince(objectDataInput.readUTF());
    }
}
