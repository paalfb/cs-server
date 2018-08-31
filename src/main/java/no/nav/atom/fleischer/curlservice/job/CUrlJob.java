package no.nav.atom.fleischer.curlservice.job;

import no.nav.atom.fleischer.curlservice.repository.mongo.ResponseDocument;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDao;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class CUrlJob implements Job {

    public static final String URL = "url";
    public static final String PROCESS_BUILDER = "processbuilder";
    public static final String REPOSITORY = "curlresponse_db";
    private final static Logger logger = LoggerFactory.getLogger(CUrlJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        logger.info("Execute scheduled task {}",jobExecutionContext.getJobDetail().getDescription());
        JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        String url = jobDataMap.getString(URL);
        ResponseDao responseDao = (ResponseDao) jobDataMap.get(REPOSITORY);
        ProcessBuilder pb = (ProcessBuilder) jobDataMap.get(PROCESS_BUILDER);
        ResponseDocument responseDocument = new ResponseDocument();
        LocalDateTime localDateTime = LocalDateTime.now().withNano(0);
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStreamReader isr = new InputStreamReader(p.getInputStream());
        BufferedReader bufferedReader = new BufferedReader(isr);
        String line;
        try {

            while (null != (line = bufferedReader.readLine())) {
                String[] resArray = line.trim().split("\\s* \\s*");
                responseDocument = new ResponseDocument(url, localDateTime, ZoneId.systemDefault(), resArray[0], resArray[1], resArray[2], resArray[3], resArray[4], resArray[5], resArray[6]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Wait to get exit value
        try {
            int exitValue = p.waitFor();
            logger.debug("Exit Value is " + exitValue);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int len;
        try {
            if ((len = p.getErrorStream().available()) > 0) {
                byte[] buf = new byte[len];
                p.getErrorStream().read(buf);
                logger.error("Command error:\t\"" + new String(buf) + "\"");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        responseDao.create(responseDocument);
    }
}
