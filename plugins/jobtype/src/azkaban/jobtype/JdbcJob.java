/*
 * Copyright 2015 GrabTaxi
 */

package azkaban.jobtype;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.utils.Props;
import azkaban.jobExecutor.AbstractProcessJob;

/**
 * A job that runs commands in an .sql file command via jdbc
 */
public class JdbcJob extends AbstractProcessJob {

	public static final String RUN_METHOD_PARAM = "method.run";
	public static final String CANCEL_METHOD_PARAM = "method.cancel";
	public static final String PROGRESS_METHOD_PARAM = "method.progress";
	public static final String JOB_CLASS = "job.class";
	public static final String DEFAULT_CANCEL_METHOD = "cancel";
	public static final String DEFAULT_RUN_METHOD = "run";
	public static final String DEFAULT_PROGRESS_METHOD = "getProgress";
	
	// mandatory
	public static final String FIELD_DBURL = "jdbc.dburl";
	public static final String FIELD_USERNAME = "jdbc.user";
	public static final String FIELD_PASSWORD = "jdbc.password";
	public static final String FIELD_CLASSNAME = "jdbc.classname";
	public static final String FIELD_FILEPATH = "sqlfile.path";
	// optional
	public static final String FIELD_CHARSET = "sqlfile.charset";
	public static final String FIELD_SEPARATOR = "sqlfile.separator";
	public static final String FIELD_BLACKLIST = "sqlfile.blacklist";
	
	
	
	// mandatory
	private String sqlFilePath = null;
	private String dbURL = null; //e.g. "x.y.us-east-1.redshift.amazonaws.com:5439/dev";
	private String userName = null;
	private String userPassword = null;
	private String className = null;
	// optional
	private String charSet = "UTF-8";
	private char statementSeparator = ';';
	private List<String> blackList = new ArrayList<String>(Arrays.asList("wb","select"));

	private BufferedReader br = null;
	private static final long KILL_TIME_MS = 5000;
	private volatile AzkabanProcess process;

	public JdbcJob(final String jobId, final Props sysProps, final Props jobProps, final Logger log) {
		super(jobId, sysProps, jobProps, log);
	}
	
	private void loadProps() throws Exception {
		// mandatory
		dbURL = jobProps.getString(FIELD_DBURL);  // missing prop definition handled inside Props class
		userName = jobProps.getString(FIELD_USERNAME);
		userPassword = jobProps.getString(FIELD_PASSWORD);
		sqlFilePath = jobProps.getString(FIELD_FILEPATH);
		className = jobProps.getString(FIELD_CLASSNAME);
		// optional
		charSet = jobProps.getString(FIELD_CHARSET, charSet);
		statementSeparator = jobProps.getString(FIELD_SEPARATOR, String.valueOf(statementSeparator)).charAt(0);
		String blackListString = (String) jobProps.get(FIELD_BLACKLIST);
		if (blackListString != null)
			blackList = Arrays.asList(blackListString.split("\\s*,\\s*"));
	}
	
	@Override
	public void run() throws Exception {
		try {
			resolveProps();
		} catch (Exception e) {
			//TODO: is handleError really needed?
			handleError("Bad property definition! " + e.getMessage(), e);
		}
		loadProps();

		
		File sqlFile = new File(sqlFilePath);
		if (!sqlFile.exists() || sqlFile.isDirectory()) {
			Exception e = new Exception("No such file");
			handleError("Bad path! " + e.getMessage(), e);
			return;
		} else {
			info("Read SQL file under "+ sqlFilePath);
		}

		long startMs = System.currentTimeMillis();
		try {
			Class.forName(className);
			info("Connecting to database... ");
			Properties props = new Properties();
			props.setProperty("user", userName);
			props.setProperty("password", userPassword);
			Connection conn = DriverManager.getConnection(dbURL, props);
			info("Connected to database!");
			// get statement list
			List<String> statementList = getStatementList();
			info(String.valueOf(statementList.size()) + " SQL statements found in file");
			// replace
			List<String> replacedList = new ArrayList<String>(statementList.size());
			for (String statement : statementList) {
				String replacedStatement = getReplacedStatement(statement);
				replacedList.add(replacedStatement);
			}
			// execute
			for(String sql : replacedList) {
				Statement stmt = conn.createStatement();
				info("Executing SQL statement: "+sql);
				// TODO: allow select statement and write result into value that can be passed around between .job files
				stmt.execute(sql);
			}
		} catch (Exception e) {
			handleError(e.getMessage(), e);
		}

	}
	
	private String getReplacedStatement(String statement) throws Exception {
		String replacedStatement = null;
		HashMap<String, String> replaceMap = new HashMap<String, String>();
		char start = '$';
		char quoteBegin = '[';
		char quoteEnd = ']';
		boolean started = false;
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<statement.length(); i++) {
		    char c = statement.charAt(i);
		    if (started) {
		    	if (c == quoteEnd) {
		    		String paramName = sb.toString();
		    		if (!replaceMap.containsKey(paramName)) {
			    		String value = jobProps.get(paramName).toString();
			    		if (value == null) {
			    			throw new Exception("Missing specification in .job file for parameter '"+paramName+"'");
			    		}
			    		replaceMap.put(paramName, value);
		    		}
		    		sb.delete(0, sb.length());
		    		started = false;
		    	} else {
		    		sb.append(c);
		    	}
		    } else {
			    if (c == start) {
			    	i++;
			    	c = statement.charAt(i);
			    	if (c == quoteBegin) {
			    		started = true;
			    	} else {
			    		throw new Exception("Bad parameter format, missing opening bracket.");
			    	}
			    }
		    }
		}
		// iterate over replacement parameters
		replacedStatement = statement;
		for (Map.Entry<String, String> entry : replaceMap.entrySet()) {
			String replaceString = "\\"+start+"\\"+quoteBegin+entry.getKey()+"\\"+quoteEnd;
			replacedStatement = replacedStatement.replaceAll(replaceString, entry.getValue());
		}
		return replacedStatement;
	}
	
	
	private boolean isBlackListed(String statement) {
		for (String blackListString : blackList) {
			int len = blackListString.length();
			if (statement.substring(0,len).toLowerCase().equals(blackListString.toLowerCase())) {
				return true;
			}
		}
		return false;
	}
	
	protected void handleError(String errorMsg, Exception e) throws Exception {
		error(errorMsg);
		if (e != null) {
			throw new Exception(errorMsg, e);
		} else {
			throw new Exception(errorMsg);
		}
	}

	/**
	 * Splits the input file into a list of sql statements.
	 *
	 * @return list of statements
	 */

	private List<String> getStatementList() throws FileNotFoundException, UnsupportedEncodingException, IOException {
		List<String> statements = new ArrayList<String>();
		br = new BufferedReader(new InputStreamReader(new FileInputStream(sqlFilePath), charSet));
		String nextStatement;
		
		while((nextStatement = readStatement()) != null) {
			// filter blacklist
			if (!isBlackListed(nextStatement)) {
				statements.add(nextStatement);
			}
		}
		return statements;
	}

	/**
	 * Reads the next statement from the input file
	 *
	 * @return Next statement or null if there are no more statements
	 */
	
	private String readStatement() throws IOException {
		int r;
		char c;  // current char
		char p = '\u0000';  // previous char
		StringBuilder sb = new StringBuilder();
		String nextStatement = null;
		boolean isLineComment = false;
		boolean isBlockComment = false;
		while (true) {
			r = br.read();
			// break conditions
			if (r == -1) { // end-of-file
				sb.delete(0, sb.length());
				break;
			}
			c = (char) r;
			if (c == statementSeparator) { // end of statement
				break;
			}
			// check block comment
			if (!isBlockComment && c == '*' && p == '/') {
				sb.deleteCharAt(sb.length()-1);
				isBlockComment = true;
			}
			else if (isBlockComment && c == '/' && p == '*') {
				isBlockComment = false;
			}
			// check line comment
			else if (!isLineComment && c == '-' && p == '-') { 
				sb.deleteCharAt(sb.length()-1);
				isLineComment = true;
			}
			else if (isLineComment && c == '\n') {
				isLineComment = false;
			}
			else if (!isBlockComment && !isLineComment) {
				sb.append(c);
			}
			p = c;
		}
		if (sb.length() > 0) {
			String rawStatement = sb.toString().trim();
			if (rawStatement.length() > 0) {
				nextStatement = rawStatement;
			}
		}
		return nextStatement;
	}

	@Override
	public void cancel() throws InterruptedException {
		if (process == null)
			throw new IllegalStateException("Not started.");
		boolean killed = process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
		if (!killed) {
			warn("Kill with signal TERM failed. Killing with KILL signal.");
			process.hardKill();
		}
	}

	@Override
	public double getProgress() {
		return process != null && process.isComplete() ? 1.0 : 0.0;
	}

	public int getProcessId() {
		return process.getProcessId();
	}

	public String getPath() {
		return _jobPath == null ? "" : _jobPath;
	}



}
