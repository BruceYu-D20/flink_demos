package table_sql;

public class StudentPojo {

    private Integer id;
    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StudentPojo(){}
    public StudentPojo(Integer id, String name){
        this.id = id;
        this.name = name;
    }
}
