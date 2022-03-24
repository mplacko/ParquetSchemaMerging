package eu.placko.examples.spark.datafactory;

import java.io.Serializable;

public class SchemaVersion3 implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer id;
    private String col_b;

    public Integer getId() {return id;}
    public String getCol_B() {return col_b;}

    public void setId(Integer id) {this.id = id;}
    public void setCol_B(String col_b) {this.col_b = col_b;}
}