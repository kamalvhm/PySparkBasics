

"""
<dependency>
     <groupId>com.databricks</groupId>
     <artifactId>spark-xml_2.11</artifactId>
     <version>0.6.0</version>
</dependency>


spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .xml("src/main/resources/persons.xml")

Alternatively, you can also use the short form format("xml") and load("src/main/resources/persons.xml")
"""
