package eu.placko.examples.spark.datafactory;

import java.io.Serializable;

public class SchemaVersion1 implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private String col_a;

    public Integer getId() {return id;}
    public String getCol_A() {return col_a;}

    public void setId(Integer id) {this.id = id;}
    public void setCol_A(String col_a) {this.col_a = col_a;}
}