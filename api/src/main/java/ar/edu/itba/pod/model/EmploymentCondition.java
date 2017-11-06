package ar.edu.itba.pod.model;

/**
 * Created by traies on 27/10/17.
 */
public enum EmploymentCondition {
    NO_DATA(0),
    EMPLOYED(1),
    UNEMPLOYED(2),
    INACTIVE(3);

    private int id;

    EmploymentCondition(int i){
        this.id = i;
    }

    public static EmploymentCondition getCondition(int i){
        switch (i){
            case 0:
                return NO_DATA;
            case 1:
                return EMPLOYED;
            case 2:
                return UNEMPLOYED;
            case 3:
                return INACTIVE;
            default:
                throw new IllegalArgumentException("Invalid employment identifier");
        }
    }

    public int getNumber() {
        return id;
    }

}
