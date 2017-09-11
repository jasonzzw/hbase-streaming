# hbase-streaming
Integrate HBase with hadoop streaming (support HBase 1.2)

dependencies:  
libs/  
├── hadoop-common-2.6.0-cdh5.12.0.jar 
├── hadoop-core-2.6.0-mr1-cdh5.12.0.jar 
├── hbase-client-1.2.0-cdh5.12.0.jar  
├── hbase-common-1.2.0-cdh5.12.0.jar  
└── hbase-server-1.2.0-cdh5.12.0.jar  
  
  
Configurable options: 
  
HBaseInputFormat: 
-D map.input.table            //specify the input table 
-D map.input.value.format     //sepcify the value format, suppport "json" and "list", default "json"  
-D map.input.value.separator  //sepcify the value separator in "list" format, default "\t"  
-D map.input.omitcf           //sepcify whether omit the column family name, if yes, use a default one. default "true"  
-D map.input.defaultcf        //specify the default column family name, default "default" 
-D map.input.timestamp        //sepcify whether include timestamp, only available when using "json" format  
-D map.input.table            //specify the wanted columns, if omitcf is true, only set the column name, otherwise, should specify in the format of "cf_name:col_name" `  
  
HBaseOutputFormat:  
-D reduce.output.table            //specify the output table  
-D reduce.output.field.separator  //sepcify the output separator, default "\t"  
-D reduce.output.omitcf           //specify whether omit the column family name. default "true" 
-D reduce.output.defaultcf        //specify the default column family name, default "default" 
#output format:   
put _tab_ row_key _separator_ column_name _separtor_ value _separator_ timestamp                                //omitcf is true  
put _tab_ row_key _separator_ column_family_name _sepsrator_ column_name _separtor_ value _separator_ timestamp   //omitcf is false 
The timestamp is optional, if omitted, the current timestamp is used. e.g., 
put 123456 name "test" 1505111399                 //omitcf=true 
put 123456 name "test"                            //omitcf=true 
put 123456 properties name "test" 1505111399      //omitcf=false  
put 123456 properties name "test"                 //omitcf=false  
