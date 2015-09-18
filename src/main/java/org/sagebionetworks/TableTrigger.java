package org.sagebionetworks;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.SynapseProfileProxy;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseResultNotReadyException;
import org.sagebionetworks.evaluation.model.Evaluation;
import org.sagebionetworks.evaluation.model.Submission;
import org.sagebionetworks.evaluation.model.SubmissionBundle;
import org.sagebionetworks.evaluation.model.SubmissionStatus;
import org.sagebionetworks.evaluation.model.SubmissionStatusEnum;
import org.sagebionetworks.reflection.model.PaginatedResults;
import org.sagebionetworks.repo.model.annotation.Annotations;
import org.sagebionetworks.repo.model.annotation.StringAnnotation;
import org.sagebionetworks.repo.model.file.ExternalFileHandle;
import org.sagebionetworks.repo.model.file.FileHandle;
import org.sagebionetworks.repo.model.file.PreviewFileHandle;
import org.sagebionetworks.repo.model.message.MessageToUser;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;

import com.google.common.io.Files;


/**
 * Framework for applying user-defined processing to tabular data in Synapse
 * 
 * 
 * 
 */
public class TableTrigger {
	    
    private static final int PAGE_SIZE = 100;
    
    private static final String TABLE_ID_ANNOTATION_NAME = "TABLE_ID";
    private static final String COUNT_QUERY_ANNOTATION_NAME = "COUNT_QUERY";
    private static final String DOCKER_REPOSITORY_ANNOTATION_NAME = "DOCKER_REPO";
    
	private static Properties properties = null;

    private SynapseClient synapseAdmin;
    private Evaluation evaluation;
    
    public static void main( String[] args ) throws Exception {
   		TableTrigger sct = new TableTrigger();
   		sct.setUp();
       	sct.execute();
    }
    
    public TableTrigger() throws SynapseException {
    	synapseAdmin = createSynapseClient();
    	String adminUserName = getProperty("ADMIN_USERNAME");
    	String adminPassword = getProperty("ADMIN_PASSWORD");
    	synapseAdmin.login(adminUserName, adminPassword);
    }
    
    public void setUp() throws SynapseException, UnsupportedEncodingException {
    	evaluation = synapseAdmin.getEvaluation(getProperty("EVALUATION_ID"));
    }
    
    /*
     * @param shellCommand
     * @param params: the params to pass to the shellCommand
     * @param workingDirectory: the working directory for the process
     * @return the shell output of the command
     */
    public static String executeShellCommand(String shellCommand, String[] params, List<String> envp, File workingDirectory) throws IOException {
   	    String[] commandAndParams = new String[params.length+1];
   	    int i=0;
  	    commandAndParams[i++] = shellCommand;
   	    if (commandAndParams.length!=i+params.length) throw new IllegalStateException();
   	    System.arraycopy(params, 0, commandAndParams, i, params.length);
   	    System.out.println("\n\n");
  	    for (int j=0; j<commandAndParams.length; j++) {
   	    	System.out.print(commandAndParams[j]+" ");
   	    }
   	    System.out.println("");
   	    Process process = Runtime.getRuntime().exec(commandAndParams, envp.toArray(new String[]{}), workingDirectory);
   	    int exitValue = -1;
   	    long processStartTime = System.currentTimeMillis();
   	    long maxScriptExecutionTimeMillis = Long.parseLong(getProperty("MAX_SCRIPT_EXECUTION_TIME_MILLIS"));
   	    while (System.currentTimeMillis()-processStartTime<maxScriptExecutionTimeMillis) {
   	    	try {
   	    		exitValue = process.exitValue();
   	    		break;
   	    	} catch (IllegalThreadStateException e) {
   	    		// not done yet
   	    	}
   	    	exitValue = -1;
   	    	try {
   	    		Thread.sleep(1000L);
   	    	} catch (InterruptedException e) {
   	    		throw new RuntimeException(e);
   	    	}
   	    }
   	    if (exitValue==-1 && System.currentTimeMillis()-processStartTime>=maxScriptExecutionTimeMillis) {
   	    	throw new RuntimeException("Process exceeded alloted time.");
   	    }
   	    ByteArrayOutputStream resultOS = new ByteArrayOutputStream();
   	    String output = null;
   	    try {
   	    	if (exitValue==0) {
   	    		IOUtils.copy(process.getInputStream(), resultOS);
   	    	} else {
  	    		IOUtils.copy(process.getErrorStream(), resultOS);
   	    	}
   	    	resultOS.close();
   	    	output = new String(resultOS.toByteArray(), "UTF-8");
   	    } finally {
   	    	if (resultOS!=null) resultOS.close();
   	    }
   	    if (exitValue!=0) {
   	   	    System.out.println(output);
  	    	throw new RuntimeException(output);
   	    }
   	    System.out.println(output);
   	    return output;
    }
    
    public static  List<String> getDockerEnvParams() {
   	    List<String> envp = new ArrayList<String>();
   	    if (getProperty("DOCKER_HOST",false)!=null) envp.add("DOCKER_HOST="+getProperty("DOCKER_HOST",false));
   	    if (getProperty("DOCKER_CERT_PATH",false)!=null) envp.add("DOCKER_CERT_PATH="+getProperty("DOCKER_CERT_PATH",false));
  	    if (getProperty("DOCKER_TLS_VERIFY",false)!=null) envp.add("DOCKER_TLS_VERIFY="+getProperty("DOCKER_TLS_VERIFY",false));
  	    if (getProperty("HOME_DIR",false)!=null) envp.add("HOME="+getProperty("HOME_DIR",false));
  	    return envp;
    }
    
    public static String executeDockerCommand(String[] params, File workingDirectory) throws IOException {
   	    String dockerCommand = getProperty("DOCKER_COMMAND");
   	    List<String> envp = getDockerEnvParams();
   		return executeShellCommand(dockerCommand, params, envp, workingDirectory);
    }
    
    public static String getDockerReferenceFromFileHandle(FileHandle fileHandle) {
    	if (!(fileHandle instanceof ExternalFileHandle)) 
    		throw new RuntimeException("Submission is not a Docker reference.  Not an external file handle.");
    	ExternalFileHandle efh = (ExternalFileHandle)fileHandle;
    	String urlString = efh.getExternalURL();
    	URL url = null;
    	try {
    		url = new URL(urlString);
    		
    	} catch (MalformedURLException e) {
    		throw new RuntimeException("Submission is not a valid Docker reference (malformed URL).");
    	}
    	
    	// this should be /username/repository
    	String dockerReference = url.getPath();
    	
    	// make sure it's a valid Docker reference
    	if (!isValidDockerReference(dockerReference)) {
    		throw new IllegalArgumentException("Submitted url is not of the form https://hostname/username/repository");
    	}
    	
    	// need to remove the leading slash ("/")
    	dockerReference = dockerReference.substring(1);
    	
    	return dockerReference;
    }
    
    /*
     * Must be an external file handle referring to a docker image
     * must be able to retrieve the docker image
     * 
     * returns the output of 'docker pull'
     */
    public String pullDockerImage(FileHandle fileHandle) throws IOException {
    	String dockerReference = getDockerReferenceFromFileHandle(fileHandle);
    	// 'shell out' to 'docker pull <reference>'
    	File tmpDir = Files.createTempDir();
    	String result = executeDockerCommand(new String[] {"pull", dockerReference}, tmpDir);
    	return result;
    }
    
    // we expect submissions to be URLs having 'paths' of the form:
    // /username/repository
    // where username is the user's DockerHub username, aka the 'namespace',
    // repository is the name of the image
    public static boolean isValidDockerReference(String s) {
      	Pattern pattern = Pattern.compile("^/[a-z0-9_]{4,}/[a-zA-Z0-9-_.]+$");
		Matcher m = pattern.matcher(s);
    	return m.matches();
    }
    
         
    private void sendMessage(String userId, String subject, String body) throws SynapseException {
    	MessageToUser messageMetadata = new MessageToUser();
    	messageMetadata.setRecipients(Collections.singleton(userId));
    	messageMetadata.setSubject(subject);
    	synapseAdmin.sendStringMessage(messageMetadata, body);
    }
    
    // TODO run in 'detached mode' (-d)
    public static String[] dockerParams(File ioDirectory, String dockerReference, String submissionId) {
    	List<String> result = new ArrayList<String>();
    	result.add("run");
    	result.add("-e");
    	result.add("SUBMISSION_ID="+submissionId+"");
    	result.add("-e");
    	result.add("USER_NAME="+getProperty("ADMIN_USERNAME")+"");
    	result.add("-e");
    	result.add("API_KEY="+getProperty("API_KEY")+"");
    	result.add(dockerReference);
      	result.add("/run.sh");
    	return result.toArray(new String[]{});
    }
    
    public static File modelInputFile(File ioDirectory) {
    	return new File(ioDirectory, "in.txt");
    }
    
    public static File modelOutputFile(File ioDirectory) {
    	return new File(ioDirectory, "out.txt");
    }
    
    public void runOneImage(FileHandle fileHandle, String submissionId) throws IOException {
    	// first retrieve the image
    	String pullResult = pullDockerImage(fileHandle);
    	// now run the image
    	String dockerReference = getDockerReferenceFromFileHandle(fileHandle);
    	File workingDir = null;
    	Boolean usingBoot2Docker = Boolean.valueOf(getProperty("USING_BOOT2_DOCKER"));
    	if (usingBoot2Docker) {
    		// if using boot2Docker make the temporary folder in our home directory
    		workingDir = new File(getProperty("HOME_DIR"), UUID.randomUUID().toString());
    		workingDir.mkdir();
    		workingDir.deleteOnExit();
    	} else {
        	workingDir = Files.createTempDir();
    	}
    	String[] params = dockerParams(workingDir, dockerReference, submissionId);
    	// we could also capture the shell output as an annotation or to send to the submitter by email
    	executeDockerCommand(params, workingDir);
    }
    
    private int executeCountQuery(String tableId, String countQuery) throws SynapseException {
    	String jobToken = synapseAdmin.queryTableEntityBundleAsyncStart(
    			countQuery, 0L, 1L, true, 1, tableId);
		QueryResultBundle qrb=null;
		long backoff = 100L;
		for (int i=0; i<100; i++) {
			try {
				qrb = synapseAdmin.queryTableEntityBundleAsyncGet(jobToken, tableId);
				break;
			} catch (SynapseResultNotReadyException e) {
				// keep waiting
				try {
					Thread.sleep(backoff);
				} catch (InterruptedException ie) {
					// continue
				}
				backoff *=2L;
			}
		}
		if (qrb==null) throw new RuntimeException("Query failed to return");
		List<Row> rows = qrb.getQueryResult().getQueryResults().getRows();
		if (rows.size()!=1) throw new 
			RuntimeException("Expected one row for count query result but found "+rows.size());
		Row row = rows.get(0);
		List<String> rowValues = row.getValues();
		if (rowValues.size()!=1) throw new 
			RuntimeException("Expected one column for count query result but found "+rows.size());
		return Integer.parseInt(rowValues.get(0));
    }
        
    /**
     * Note: There are two types of scoring, that in which each submission is scored alone and that
     * in which the entire set of submissions is rescored whenever a new one arrives. 
     * 
     * @throws SynapseException
     * @throws JSONObjectAdapterException 
     */
    public void execute() throws SynapseException, IOException, JSONObjectAdapterException {
    	long startTime = System.currentTimeMillis();
     	long total = Integer.MAX_VALUE;
     	int started = 0;
       	for (int offset=0; offset<total; offset+=PAGE_SIZE) {
       		PaginatedResults<SubmissionBundle> submissionPGs = null;
       		submissionPGs = synapseAdmin.getAllSubmissionBundlesByStatus(evaluation.getId(), 
     				SubmissionStatusEnum.RECEIVED, offset, PAGE_SIZE);
        	total = (int)submissionPGs.getTotalNumberOfResults();
        	List<SubmissionBundle> page = submissionPGs.getResults();
        	for (int i=0; i<page.size(); i++) {
        		SubmissionBundle bundle = page.get(i);
        		Submission sub = bundle.getSubmission();
        		FileHandle fileHandle = getFileHandleFromEntityBundle(sub.getEntityBundleJSON());
        		SubmissionStatus status = bundle.getSubmissionStatus();
       			// Retrieve and run COUNT_QUERY.  If > 0 then execute Docker image
       			String tableId = readStringAnnotation(status, TABLE_ID_ANNOTATION_NAME);
       			String countQuery = readStringAnnotation(status, COUNT_QUERY_ANNOTATION_NAME);
       			if (tableId!=null && countQuery!=null) {
       				int count = executeCountQuery(tableId, countQuery);
       				if (count==0) {
       					System.out.println("Count query returned 0: "+countQuery);
       					continue; // nothing to process, so skip this submission
       				}
       			}
       			// if no tableId or no countQuery then run the image
       			// it's then the image's job to put this information in the annotations
               	try {
            		String dockerReference = getDockerReferenceFromFileHandle(fileHandle);
            		if (readStringAnnotation(status, DOCKER_REPOSITORY_ANNOTATION_NAME)==null) 
            			addAnnotation(status, DOCKER_REPOSITORY_ANNOTATION_NAME, dockerReference, false);
          			status.setStatus(SubmissionStatusEnum.EVALUATION_IN_PROGRESS);
           	        status = synapseAdmin.updateSubmissionStatus(status);
          			// we run the image if either (1) there are new rows to process of (2) it hasn't been run before
           	        runOneImage(fileHandle, sub.getId()); 
           	        // make sure that the Docker reference is in an annotation so it can be displayed
          	        started++;
           		} catch (Exception e) {
           			status = synapseAdmin.getSubmissionStatus(sub.getId());
           			if (status.getStatus()!=SubmissionStatusEnum.RECEIVED) {
           				status.setStatus(SubmissionStatusEnum.RECEIVED);
           				status = synapseAdmin.updateSubmissionStatus(status);
           			}
           			// TODO send failure notification to submitter
           			e.printStackTrace();
           		}
        	}
       	}
       	
       	long delta = System.currentTimeMillis() - startTime;
       	System.out.println("Started "+started+" jobs.  Elapsed time: "+formatInterval(delta));
    }
    
    private static FileHandle getFileHandleFromEntityBundle(String s) {
    	try {
	    	JSONObject bundle = new JSONObject(s);
	    	JSONArray fileHandles = (JSONArray)bundle.get("fileHandles");
	    	for (int i=0; i<fileHandles.length(); i++) {
	    		String jsonString = fileHandles.getString(i);
    			FileHandle fileHandle = EntityFactory.createEntityFromJSONString(jsonString, FileHandle.class);
    			if (!(fileHandle instanceof PreviewFileHandle)) return fileHandle;
	    	}
	    	throw new IllegalArgumentException("File has no file handle ID");
    	} catch (JSONException e) {
    		throw new RuntimeException(e);
    	} catch (JSONObjectAdapterException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    private static String formatInterval(final long l) {
        final long hr = TimeUnit.MILLISECONDS.toHours(l);
        final long min = TimeUnit.MILLISECONDS.toMinutes(l - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02dh:%02dm:%02d.%03ds", hr, min, sec, ms);
    }
    
    private static String readStringAnnotation(SubmissionStatus status, String key) {
    	Annotations annotations = status.getAnnotations();
    	if (annotations==null) return null;
    	List<StringAnnotation> sas = annotations.getStringAnnos();
    	if (sas==null) return null;
    	for (StringAnnotation sa : sas) {
    		if (sa.getKey().equals(key)) return sa.getValue();
    	}
    	return null;
    }
    
     
	// the 'isPrivate' flag should be set to 'true' for information
	// used by the scoring application but not to be revealed to participants
	// to see 'public' annotations requires READ access in the Evaluation's
	// access control list, as the participant has (see setUp(), above). To
	// see 'private' annotatations requires READ_PRIVATE_SUBMISSION access,
	// which the Evaluation admin has by default
    private static void addAnnotation(SubmissionStatus status, String key, String value, boolean isPrivate) {
    	if (value.length()>499) value = value.substring(0, 499);
    	Annotations annotations = status.getAnnotations();
		if (annotations==null) {
			annotations=new Annotations();
			status.setAnnotations(annotations);
		}
		List<StringAnnotation> sas = annotations.getStringAnnos();
		if (sas==null) {
			sas = new ArrayList<StringAnnotation>();
			annotations.setStringAnnos(sas);
		}
		StringAnnotation matchingSa = null;
		for (StringAnnotation existingSa : sas) {
			if (existingSa.getKey().equals(key)) {
				matchingSa = existingSa;
				break;
			}
		}
		if (matchingSa==null) {
			StringAnnotation sa = new StringAnnotation();
			sa.setIsPrivate(isPrivate);
			sa.setKey(key);
			sa.setValue(value);
			sas.add(sa);
		} else {
			matchingSa.setIsPrivate(isPrivate);
			matchingSa.setValue(value);
		}
    }
        
    private static void removeAnnotation(SubmissionStatus status, String key) {
    	Annotations annotations = status.getAnnotations();
		if (annotations==null) return;
		List<StringAnnotation> sas = annotations.getStringAnnos();
		if (sas==null) return;
		for (StringAnnotation existingSa : sas) {
			if (existingSa.getKey().equals(key)) {
				sas.remove(existingSa);
			}
		}
    }
        
	public static void initProperties() {
		if (properties!=null) return;
		properties = new Properties();
		InputStream is = null;
    	try {
    		is = TableTrigger.class.getClassLoader().getResourceAsStream("global.properties");
    		properties.load(is);
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	} finally {
    		if (is!=null) try {
    			is.close();
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	}
   }
	
	public static String getProperty(String key) {
		return getProperty(key, true);
	}
		
	public static String getProperty(String key, boolean required) {
		initProperties();
		String commandlineOption = System.getProperty(key);
		if (commandlineOption!=null) return commandlineOption;
		String embeddedProperty = properties.getProperty(key);
		if (embeddedProperty!=null) return embeddedProperty;
		if (required) throw new RuntimeException("Cannot find value for "+key);
		return null;
	}	
	  
	private static SynapseClient createSynapseClient() {
		SynapseClientImpl scIntern = new SynapseClientImpl();
		scIntern.setAuthEndpoint("https://repo-prod.prod.sagebase.org/auth/v1");
		scIntern.setRepositoryEndpoint("https://repo-prod.prod.sagebase.org/repo/v1");
		scIntern.setFileEndpoint("https://repo-prod.prod.sagebase.org/file/v1");
		return SynapseProfileProxy.createProfileProxy(scIntern);
  }

}
