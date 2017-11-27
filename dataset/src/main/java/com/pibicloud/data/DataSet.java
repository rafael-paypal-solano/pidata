package com.pibicloud.data;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

public class DataSet {
	private static final SimpleDateFormat ISO8601_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");	
	private static final Pattern INTEGER_PATTERN = Pattern.compile("(\\+|\\-)?\\d+");
	private static final Pattern FLOAT_PATTERN = Pattern.compile("(\\+|\\-)?\\d+\\.\\d+");
	
	ArrayList<Serializable[]> data;
	HashMap<Serializable,Integer> rowMap;
	HashMap<String,Integer> columnMap;
	
	private DataSet(ArrayList<Serializable[]> data, HashMap<Serializable,Integer> rowMap, HashMap<String,Integer> columnMap) {
		this.data = data;
		this.columnMap = columnMap;
		this.rowMap = rowMap;
	}
	
	
	public static DataSet create(ResultSet resultSet, int indexColumn) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		HashMap<Serializable,Integer> rowMap = new HashMap<Serializable,Integer>();
		HashMap<String,Integer> columnMap = new HashMap<String,Integer>();		
		ArrayList<Serializable[]> data = new ArrayList<Serializable[]> ();
		int rowNumber = 0;
		
		for(int column = 0; column < columnCount; column ++) {
			columnMap.put(metaData.getColumnName(column+1), column);
		}
		
		while(resultSet.next()) {
			Serializable[] row = new Serializable[columnCount];
			
			for(int column = 0; column < columnCount; column++) {
				row[column] = (Serializable) resultSet.getObject(column+1);				
			}

			data.add(row);
			
			if(indexColumn > 0) {
				rowMap.put((Serializable) resultSet.getObject(indexColumn+1), rowNumber);
			}
			
			rowNumber ++;
		}
		
		return  new DataSet (data, rowMap, columnMap);
	}
	
	/**
	 * 
	 * @param rowNumber Zero-based row number.
	 * @param columnNumber Zero-based column number.
	 * @return data.get(rowNumber)[columnNumber]
	 */
	public Serializable at(int rowNumber, int columnNumber) {
		return data.get(rowNumber)[columnNumber];
	}
	
	/**
	 * 
	 * @param row Value in index column that uniquely identifies a row.
	 * @param column Column name.
	 * @return The value contained in the row @(row, column) or <code>null</code> if no row identified by <code>row</code> exists.
	 */
	public Serializable at(Serializable row, String column) {
		Integer rowNumber = this.rowMap.get(row);
		
		if(rowNumber == null)
			return null;
		
		int columnNumber = this.columnMap.get(column);				
		return at(rowNumber, columnNumber);
	}
	
	public Number number(int rowNumber, int columnNumber) {
		Serializable value = at(rowNumber, columnNumber);
		
		
		if(value == null || value instanceof Number)
			return (Number)value;
		
		String string = value.toString().trim();
		
		if(INTEGER_PATTERN.matcher(string).matches())
			return Integer.parseInt(string);
		
		if(FLOAT_PATTERN.matcher(string).matches())
			return Double.parseDouble(string);
		
		throw new ClassCastException(String.format("Can't cast value at @(%d,%d):'%s' to number", rowNumber, columnNumber, value));
	}
	
	public Number number(Serializable row, String column) {
		int rowNumber = this.rowMap.get(row);
		int columnNumber = this.columnMap.get(column);
		
		return number(rowNumber, columnNumber);
	}	
	
	public String string(int rowNumber, int columnNumber) {
		Serializable value = at(rowNumber, columnNumber);
		
		if(value != null)
			return value.toString();
		
		return null;
	}
	
	public String string(Serializable row, String column) {
		int rowNumber = this.rowMap.get(row);
		int columnNumber = this.columnMap.get(column);
		
		return string(rowNumber, columnNumber);
	}
	
	
	public Date date(int rowNumber, int columnNumber) throws ParseException, ClassCastException {
		Serializable value = at(rowNumber, columnNumber);
		
		if(value == null)
			return null;
		
		if(value instanceof Date)
			return (Date)value;
		
		if(value instanceof String)
			return ISO8601_DATE_FORMAT.parse((String)value);
		
		if(value instanceof Number)
			return new Date(((Number)value).longValue());
		
		throw new ClassCastException(String.format("Can't cast value at @(%d,%d):'%s' to date", rowNumber, columnNumber, value));
		
	}
}
