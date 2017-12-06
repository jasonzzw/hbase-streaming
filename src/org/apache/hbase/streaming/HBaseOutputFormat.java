package org.apache.hbase.streaming;

import java.io.IOException;
import java.util.Date;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class HBaseOutputFormat implements OutputFormat<Text, Text>,
       JobConfigurable {

           public static final String TABLE_KEY = "reduce.output.table";
           public static final String SEPARATOR_KEY = "reduce.output.field.separator";
           public static final String OMITCF_KEY = "reduce.output.omitcf";
           public static final String DEFAULTCF_KEY = "reduce.output.defaultcf";

           public static final String DEFAULT_SEPARATOR = "\t";
           public static final String DEFAULT_CF = "default";

           private String separator;
           private TableOutputFormat tableOutputFormat;
           private boolean omitcf;
           private String defaultCF;

           public HBaseOutputFormat() {
               tableOutputFormat = new TableOutputFormat();
           }

           private boolean argToBoolean(String arg, boolean deft) {
               if (arg == null)
                   return deft;
               return arg.equals("true") || arg.equals("yes") || arg.equals("on")
                   || arg.equals("1");
           }

           @Override
               public void configure(JobConf job) {
                   // TODO Auto-generated method stub
                   job.set(TableOutputFormat.OUTPUT_TABLE, job.get(TABLE_KEY));

                   separator = job.get(SEPARATOR_KEY);
                   if (separator == null)
                       separator = DEFAULT_SEPARATOR;

                   omitcf = argToBoolean(job.get(OMITCF_KEY), true);
                   defaultCF = job.get(DEFAULT_CF);
                   if (defaultCF == null) {
                       defaultCF = DEFAULT_CF;
                   }
               }

           @Override
               public void checkOutputSpecs(FileSystem ignored, JobConf job)
               throws IOException {
                   // TODO Auto-generated method stub
                   // configure(job);
                   tableOutputFormat.checkOutputSpecs(ignored, job);
               }

           @SuppressWarnings("unchecked")
               @Override
               public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
                       JobConf job, String name, Progressable progress) throws IOException {
                   // TODO Auto-generated method stub
                   // configure(job);
                   return new HBaseRecordWriter(tableOutputFormat.getRecordWriter(ignored,
                               job, name, progress));
               }

           private long getTimestampString(String ts) {
               try {
                   return Long.parseLong(ts);
               } catch (NumberFormatException e) {
                   return new Date().getTime();
               }
           }

           private boolean put(Put p, String[] args) {
               try {
                   if (omitcf) {
                       if (args.length > 3) {
                           p.addColumn(defaultCF.getBytes("UTF-8"), args[1].getBytes("UTF-8"),
                                   getTimestampString(args[3]), args[2].getBytes("UTF-8"));
                       } else if (args.length == 3) {
                           p.addColumn(defaultCF.getBytes("UTF-8"), args[1].getBytes("UTF-8"),
                                   args[2].getBytes("UTF-8"));
                       } else {
                           // something is wrong
                           return false;
                       }
                   } else {
                       if (args.length > 4) {
                           p.addColumn(args[1].getBytes("UTF-8"), args[2].getBytes("UTF-8"),
                                   getTimestampString(args[4]), args[3].getBytes("UTF-8"));
                       } else if (args.length == 4) {
                           p.addColumn(args[1].getBytes("UTF-8"), args[2].getBytes("UTF-8"),
                                   args[3].getBytes("UTF-8"));
                       } else {
                           // something is wrong
                           return false;
                       }
                   }
               }catch(Exception e){

               }
               return true;
           }

           private Put createUpdate(String command, String argsString) {
               String[] args = argsString.split(Pattern.quote(separator), -1);
               Put p;
               try {
                   if (command.equals("put")) {
                       p = new Put(args[0].getBytes());
                       if (!put(p, args)) {
                           throw new RuntimeException();
                       }
                   } else {
                       throw new RuntimeException();
                   }
               } catch (Exception e) {
                   e.printStackTrace();
                   throw new RuntimeException(String.format(
                               "HBaseOutputFormat - invalid reduce output: %s / %s",
                               command, argsString));

               }
               return p;
           }

           private class HBaseRecordWriter implements RecordWriter<Text, Text> {
               private RecordWriter<ImmutableBytesWritable, Put> tableRecordWriter;

               public HBaseRecordWriter(
                       RecordWriter<ImmutableBytesWritable, Put> tableRecordWriter) {
                   this.tableRecordWriter = tableRecordWriter;
               }

               public void close(Reporter reporter) throws IOException {
                   tableRecordWriter.close(reporter);
               }

               public void write(Text key, Text value) throws IOException {
                   Put put = createUpdate(new String(key.copyBytes(), StandardCharsets.UTF_8), new String(value.copyBytes(), StandardCharsets.UTF_8));
                   if (put != null) {
                       tableRecordWriter.write(null, put);
                   }
               }
           }

       }
