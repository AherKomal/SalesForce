import java.io.*;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;


public class BulkQueryPKChunking {
    private String sobjectType = "My_object__c";
    private String field_name = "x__c";
    private String chunk_size = "1500";

    public static void main(String[] args)
            throws AsyncApiException, ConnectionException, IOException {
        BulkQueryPKChunking exampleQuery = new BulkQueryPKChunking();
        System.setProperty("https.protocols", "TLSv1.2");
        BulkConnection bulkConnection = exampleQuery.login();
        bulkConnection.addHeader("Sforce-Enable-PKChunking", "chunkSize=" + exampleQuery.chunk_size);
        exampleQuery.doBulkQuery(bulkConnection);
    }


    private BulkConnection login() {

        String userName = "komal.aher@synerzip.com";
        String passWord = "Salesforce03h11EF5KRdOddR9MJrVH3c1LH";
        String url = "https://login.salesforce.com/services/Soap/u/40.0";

        BulkConnection _bulkConnection = null;
        try {

            ConnectorConfig partnerConfig = new ConnectorConfig();
            partnerConfig.setUsername(userName);
            partnerConfig.setPassword(passWord);
            partnerConfig.setAuthEndpoint(url);
            new PartnerConnection(partnerConfig);
            ConnectorConfig config = new ConnectorConfig();
            config.setSessionId(partnerConfig.getSessionId());
            String soapEndpoint = partnerConfig.getServiceEndpoint();
            String apiVersion = "40.0";
            String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
                    + "async/" + apiVersion;
            config.setRestEndpoint(restEndpoint);
            config.setCompression(true);
            config.setTraceMessage(false);
            _bulkConnection = new BulkConnection(config);
        } catch (AsyncApiException | ConnectionException aae) {
            aae.printStackTrace();
        }
        return _bulkConnection;
    }


    private void doBulkQuery(BulkConnection bulkConnection) {

        try {
            JobInfo job = new JobInfo();
            job.setObject(sobjectType);
            job.setOperation(OperationEnum.query);
            job.setConcurrencyMode(ConcurrencyMode.Parallel);
            job.setContentType(ContentType.CSV);
            job = bulkConnection.createJob(job);

            assert job.getId() != null;
            System.out.println("Job id is " + job.getId());

            job = bulkConnection.getJobStatus(job.getId());

            String query = "SELECT " + field_name + " FROM " + sobjectType;
            ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes());
            bulkConnection.createBatchFromStream(job, bout);
            bulkConnection.closeJob(job.getId());
            BatchInfo[] bListInfo = bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
            System.out.println("Number of Batches : " + (bListInfo.length - 1) + "\n");
            int numberOfBatchesForQueryExtract = 0;

            for (int ib = 1; ib < bListInfo.length; ib++) {
                BatchInfo info = bListInfo[ib];
                numberOfBatchesForQueryExtract++;

                String[] queryResults = null;

                for (int i = 0; i < 10000; i++) {
                    Thread.sleep(1000); //30 sec
                    info = bulkConnection.getBatchInfo(job.getId(), info.getId());

                    if (info.getState() == BatchStateEnum.Completed) {
                        QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId());
                        queryResults = list.getResult();
                        break;
                    } else if (info.getState() == BatchStateEnum.Failed) {
                        System.out.println("Failed Batch ID : " + info.getId() + "\nReason : " + info.getStateMessage());
                        break;
                    } else {
                        System.out.println("Waiting for Batch ID : " + info.getId() + "to be Processed.\n" + "State Info : " + info.getStateMessage());
                        if (info.getStateMessage() == null) break;
                        System.out.println("Warning : Skipping the Batch ID :" + info.getId() + "\nReason : State is NULL");

                    }
                }

                if (queryResults != null) {
                    for (String resultId : queryResults) {
                        InputStream resultStream = bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId);
                        String fileNameCreated = createFileName(job.getId(), info.getId(), convertInputStreamToString(resultStream));
                        System.out.println("Data extracted successfully into CVS file");
                        System.out.println("Filename " + fileNameCreated + ", No of rows in the file is " +
                                countLines("/home/synerzip/Documents/SalesforceSoql_test/PullledData/" + fileNameCreated) + "\n\n");
                    }
                }
            }
            System.out.println("Number of batches created with chunkSize " + chunk_size + " : " + +numberOfBatchesForQueryExtract);
        } catch (AsyncApiException | InterruptedException aae) {
            aae.printStackTrace();
        }
    }

    private String convertInputStreamToString(InputStream inputStream) {
        StringBuilder result = new StringBuilder();
        String line;
        String newLine = System.getProperty("line.separator");
        boolean flag = false;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF8"));

            while ((line = reader.readLine()) != null) {
                result.append(flag ? newLine : "").append(line);
                flag = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.toString();
    }

    private String createFileName(String jobId, String batchId, String dataAsString) {
        String namePrefix = "SFBulkExtract";
        String nameSuffix = ".csv";
        String fileSeperator = "/";
        String fFileName = namePrefix + "_" + jobId + "_" + batchId + nameSuffix;
        createFile(dataAsString, fFileName);
        return fFileName;
    }

    //Create the CVS file
    private void createFile(String dataAsString, String fFileName) {
        OutputStream outFile = null;
        try {
            File oFile = new File("/home/synerzip/Documents/SalesforceSoql_test/PullledData/" + fFileName);
            if (oFile.exists()) {
                boolean deleteFileSucceeded = oFile.delete();
                if (!deleteFileSucceeded) {
                    throw new IOException("Unable to delete file");
                }
            }
            outFile = new BufferedOutputStream(new FileOutputStream("/home/synerzip/Documents/SalesforceSoql_test/PullledData/" + fFileName));
            outFile.write(dataAsString.getBytes());
            outFile.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (outFile != null)
                try {
                    outFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static int countLines(String filename) {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(filename));

            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                assert is != null;
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

}