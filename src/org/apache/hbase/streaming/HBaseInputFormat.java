package org.apache.hbase.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.streaming.json.JSONUtil;


public class HBaseInputFormat implements InputFormat<Text, Text>,
		JobConfigurable {

	public static final String TABLE_KEY = "map.input.table";
	public static final String COLUMNS_KEY = "map.input.columns";
	public static final String FORMAT_KEY = "map.input.value.format";
	public static final String SEPARATOR_KEY = "map.input.value.separator";
	public static final String OMITCF_KEY = "map.input.omitcf";
	public static final String DEFAULTCF_KEY = "map.input.defaultcf";
	public static final String HAS_TIMESTAMP_KEY = "map.input.timestamp";

	public static final String DEFAULT_FORMAT = "json";
	public static final String DEFAULT_SEPARATOR = "\t";
	public static final String DEFAULT_CF = "default";

	private TableInputFormat tableInputFormat;
	private boolean withTimeStamp;
	private boolean omitcf;
	private String format;
	private String separator;
	private String defaultCF;

	public HBaseInputFormat() {
		tableInputFormat = new TableInputFormat();
	}
	
	private  String augmentCF(String columns) {
		if (columns==null || columns=="" ){
			if(omitcf) {
				return defaultCF;
			}else{
				return "";
			}
		}else{
			List<String> list=new ArrayList<String>();
			String[] cls=columns.split(" ");
			for(String s:cls){
				if(s!=""){
					if(omitcf){
						list.add(defaultCF+":"+s);
					}else{
						list.add(s);
					}
				}
			}
			return StringUtils.join(" ", list);
		}
	}
	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		withTimeStamp = argToBoolean(job.get(HAS_TIMESTAMP_KEY), false);
		omitcf = argToBoolean(job.get(OMITCF_KEY), true);
		
		format = job.get(FORMAT_KEY);
		if (format == null) {
			format = DEFAULT_FORMAT;
		} else if (format != "json" && format != "list") {
			format = DEFAULT_FORMAT;
		}
		
		separator=job.get(SEPARATOR_KEY);
		if (separator==null){
			separator=DEFAULT_SEPARATOR;
		}
		
		defaultCF=job.get(DEFAULT_CF);
		if (defaultCF==null) {
			defaultCF=DEFAULT_CF;
		}
		
		FileInputFormat.setInputPaths(job, job.get(TABLE_KEY));
		job.set(TableInputFormat.COLUMN_LIST, augmentCF(job.get(COLUMNS_KEY)));
		tableInputFormat.configure(job);
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		return new HBaseRecordReader(tableInputFormat.getRecordReader(split, job,
				reporter));
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		// TODO Auto-generated method stub
		return tableInputFormat.getSplits(job, numSplits);
	}



	private boolean argToBoolean(String arg, boolean deft) {
		if (arg == null)
			return deft;
		return arg.equals("true") || arg.equals("yes") || arg.equals("on")
				|| arg.equals("1");
	}

	private class HBaseRecordReader implements RecordReader<Text, Text> {
		private RecordReader<ImmutableBytesWritable, Result> tableRecordReader;

		public HBaseRecordReader(
				RecordReader<ImmutableBytesWritable, Result> reader) {
			tableRecordReader = reader;
		}

		public void close() throws IOException {
			tableRecordReader.close();
		}

		public Text createKey() {
			return new Text("");
		}

		public Text createValue() {
			return new Text("");
		}

		public long getPos() throws IOException {
			return tableRecordReader.getPos();
		}

		public float getProgress() throws IOException {
			return tableRecordReader.getProgress();
		}
		
		private  String formatList(Result row) {
			 StringBuilder values = new StringBuilder("");
		        for (Cell cell : row.listCells()) {
		            if (values.length() != 0)
		                values.append(separator);
		            values.append(new String(CellUtil.cloneValue(cell)));
		        }
		        return values.toString();
		}
		
	    private String encodeColumnName(byte[] family, byte[] qualifier) {
	    	int resultSize =  family.length + 1 + qualifier.length;
	    	ByteBuffer bb = ByteBuffer.allocate(resultSize);
	    	
	    	bb.put(family);
	    	bb.put(":".getBytes());
	    	bb.put(qualifier);
	        return new String( bb.array()); 
	    }
	    
		
		private  String formatJson(Result row) {
			Map<String, Map<String, String>> values = new HashMap<String, Map<String, String>>();
	        for (Cell cell : row.listCells()) {
	            Map<String, String> field = new HashMap<String, String>();
	            field.put("value", new String(CellUtil.cloneValue(cell)));
	            if(withTimeStamp)
	            	field.put("timestamp", String.valueOf(cell.getTimestamp()));
	            if (omitcf){
	            	values.put(new String(CellUtil.cloneQualifier(cell)), field);
	            }else{
	            	values.put(encodeColumnName(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)), field);
	            }
	        }
	        return JSONUtil.toJSON(values);
		}

		public boolean next(Text key, Text value) throws IOException {
			Result row = new Result();
			boolean hasNext = tableRecordReader.next(
					new ImmutableBytesWritable(key.getBytes()), row);
			if (hasNext) {
				key.set(row.getRow());
				if (format=="list") {
					//if format is list
					value.set(formatList(row));
				}else if (format=="json") {
					value.set(formatJson(row));
				}
				
			}
			return hasNext;
		}
	}
}
