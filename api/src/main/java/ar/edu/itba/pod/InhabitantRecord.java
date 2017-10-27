package ar.edu.itba.pod;

import java.io.Serializable;

/**
 * Created by traies on 27/10/17.
 */
public class InhabitantRecord implements Serializable {

    private final Long serialVersionUID = 1L;

    private static int ID = 0;

    private int id;

    private EmploymentCondition condition ;

    private Integer homeId;

    private String departmentName;

    private Province province;

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
}
