package no.nav.atom.fleischer.curlservice;

import no.nav.atom.fleischer.curlservice.job.CUrlJob;
import no.nav.atom.fleischer.curlservice.repository.mongo.CURLDocument;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDaoImpl;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDaoImpl;
import org.quartz.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class DbCurlJobCheckerCallable implements Callable<List<JobAndTrigger>> {
    private static final String DB_SERVER = "localhost";
    private static final int PORT = 27017;
    private static final String DB_NAME = "curlservicedb";
    private static final String CURLS_COLLECTION = "curls_collection";
    private static final String RESPONSE_COLLECTION = "response_collection";

    @Override
    public List<JobAndTrigger> call() {
        CURLDao curlDao = new CURLDaoImpl(DB_SERVER, PORT, DB_NAME, CURLS_COLLECTION);
        ResponseDao responseDao = new ResponseDaoImpl(DB_SERVER, PORT, DB_NAME, RESPONSE_COLLECTION);

        List<CURLDocument> curlDocuments = curlDao.findActive();
        List<JobAndTrigger> jobsAndTriggers = new ArrayList<>();
        for (CURLDocument doc : curlDocuments) {
            String id = doc.getId().toString();
            String url = doc.getUrl();
            String crontab = doc.getCrontab();
            ProcessBuilder pb = new ProcessBuilder(
                    "curl",
                    "-o /dev/null",
                    "-s",
                    "-w %{time_namelookup} %{time_connect} %{time_appconnect} %{time_pretransfer} %{time_redirect} %{time_starttransfer} %{time_total}",
                    url
            );
            JobDetail jobDetail = JobBuilder.newJob(CUrlJob.class).withIdentity("curljob_" + id).build();
            jobDetail.getJobDataMap().put(CUrlJob.URL, url);
            jobDetail.getJobDataMap().put(CUrlJob.PROCESS_BUILDER, pb);
            jobDetail.getJobDataMap().put(CUrlJob.REPOSITORY, responseDao);

            CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                    .withIdentity("cronTrigger", "cronTriggerGroup")
                    .withSchedule(CronScheduleBuilder.cronSchedule(crontab))
                    .build();
            JobAndTrigger jobAndTrigger = new JobAndTrigger(jobDetail, cronTrigger);
            jobsAndTriggers.add(jobAndTrigger);
        }
        return jobsAndTriggers;
    }
}
