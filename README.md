# sqlInterpreter  
A JAVA postgreSQL interpreter  
  
This is a postgresSQL interpreter capable of running basic queries in the form SELECT-FROM-WHERE, as well as allowing DISTINCT and ORDER BY operators. 
It has been optimized using JAVA.NIO for reading in data, and uses block nested loop joins, sort merge joins, and external sorting. It also supprots building indices on relations using B+ data structures. It will also analyze relations and generate an optimal join and select order automatically.  

Project Structure:  
  sqlInterpreter.jar - an executable for running the interpreter  
  input-  
      config.txt- three line file containing input directory, output directory, and temporary directory locations  
      queries.sql - the SQL queries to be run  
      db-  
         schema.txt - file describing name and field names of each index  
         index_info.txt- describes what indexes the interpreter should build in the form RELATIONS FIELD CLUSTERED (1) / UNCLUSTERED(0)  
         indexes-  
           byte representations of the indexes built  
         data-  
           byte representations of the relations the queries are to be run on  
     output- directory where query results will be stored   



To Run:
  Enter the following command java -jar sqlInterpreter.jar {path-to-config.txt}
