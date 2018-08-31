package no.nav.atom.fleischer.curlservice;

import no.nav.atom.fleischer.curlservice.job.CUrlJob;
import no.nav.atom.fleischer.curlservice.repository.mongo.CURLDocument;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDaoImpl;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDaoImpl;
import org.apache.commons.cli.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class CURLService implements Runnable {
    private static final String DB_SERVER_PROP = "dbserver";
    private static final String DB_PORT_PROP = "dbport";
    private static final String DBS_PROP = "dbs";
    private static final String LOG_PROP = "log";

    private static String dbServer = "localhost";
    private static int dbPort = 27017;
    private static String dbs = "curlservicedb";
    private static String log;
    private static ResponseDao responseDao = null;
    private static CURLDao curlDao = null;
    private static SchedulerFactory schFactory = new StdSchedulerFactory();
    private static List<JobAndTrigger> jobsAndTriggersList = new ArrayList<>();
    private static Logger logger = LoggerFactory.getLogger(CURLService.class);

    public static void main(String[] args) {
        loadProperties();
        getCommandLineArguments(args);
        if (log != null)
            updateLog4jConfiguration(log);
        logger.info("Starting cURLService");
        logger.info("dbServer: {}", dbServer);
        logger.info("dbPort: {}", dbPort);
        logger.info("dbs: {}", dbs);
        logger.info("log: {}", log);
        String RESPONSE_COLLECTION = "response_collection";
        responseDao = new ResponseDaoImpl(dbServer, dbPort, dbs, RESPONSE_COLLECTION);
        String CURLS_COLLECTION = "curls_collection";
        curlDao = new CURLDaoImpl(dbServer, dbPort, dbs, CURLS_COLLECTION);
        Runnable runnable = new CURLService();
        Thread thread = new Thread(runnable);
        thread.start();

    }

    private static void loadProperties() {
        Properties properties = new Properties();
        String filename = "config.properties";
        InputStream inputStream = CURLService.class.getClassLoader().getResourceAsStream(filename);
        if (inputStream == null) {
            logger.warn("Unable to read {}. Using default values", filename);
        } else {
            try {
                properties.load(inputStream);
                dbServer = properties.getProperty(DB_SERVER_PROP, dbServer);
                dbPort = Integer.parseInt(properties.getProperty(DB_PORT_PROP, Integer.toString(dbPort)));
                dbs = properties.getProperty(DBS_PROP, dbs);
                log = properties.getProperty(LOG_PROP, log);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void getCommandLineArguments(String[] args) {
        Options options = new Options();
        options.addOption("" + DB_SERVER_PROP, true, "Mongodb hostname");
        options.addOption("" + DB_PORT_PROP, true, "Mongodb port");
        options.addOption("" + DBS_PROP, true, "database");
        options.addOption("" + LOG_PROP, true, "Logging directory");
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = commandLineParser.parse(options, args);
            if (cmd.hasOption(DB_SERVER_PROP)) {
                dbServer = cmd.getOptionValue(DB_SERVER_PROP);
            }
            if (cmd.hasOption(DB_PORT_PROP)) {
                dbPort = Integer.parseInt(cmd.getOptionValue(DB_PORT_PROP));
            }
            if (cmd.hasOption(DBS_PROP)) {
                dbs = cmd.getOptionValue(DBS_PROP);

            }
            if (cmd.hasOption(LOG_PROP)) {
                log = cmd.getOptionValue(LOG_PROP);
            }
        } catch (ParseException e) {
            logger.warn("Could not parse command line arguments");
            e.printStackTrace();
        }
    }

    private static void updateLog4jConfiguration(String logFile) {
        Properties props = new Properties();
        try {
            InputStream configStream = CURLService.class.getClassLoader().getResourceAsStream("log4j.properties");
            props.load(configStream);
            configStream.close();
        } catch (IOException e) {
            System.out.println("Error. cannot laod configuration file ");
        }
        props.setProperty("log4j.appender.FILE.File", logFile);
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);
    }

    private static List<JobAndTrigger> getJobsAndTriggers() {
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
            JobDetail jobDetail = JobBuilder
                    .newJob(CUrlJob.class)
                    .withIdentity("curljob_" + id)
                    .withDescription(doc.toString())
                    .build();

            jobDetail.getJobDataMap().put(CUrlJob.URL, url);
            jobDetail.getJobDataMap().put(CUrlJob.PROCESS_BUILDER, pb);
            jobDetail.getJobDataMap().put(CUrlJob.REPOSITORY, responseDao);

            CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                    .withIdentity("cronTrigger" + id, "cronTriggerGroup")
                    .withSchedule(CronScheduleBuilder.cronSchedule(crontab))
                    .build();
            JobAndTrigger jobAndTrigger = new JobAndTrigger(jobDetail, cronTrigger);
            jobsAndTriggers.add(jobAndTrigger);
        }
        return jobsAndTriggers;
    }

    @Override
    public void run() {
        while (true) {
            CURLService.jobsAndTriggersList = getJobsAndTriggers();
            if (jobsAndTriggersList.size() > 0) {
                try {
                    Scheduler sch = schFactory.getScheduler();
                    sch.clear();
                    sch.start();
                    for (JobAndTrigger jobAndTrigger : jobsAndTriggersList) {
                        sch.scheduleJob(jobAndTrigger.getJobDetail(), jobAndTrigger.getCronTrigger());
                        logger.debug("Job has been scheduled");
                    }
                } catch (SchedulerException e) {
                    e.printStackTrace();
                }
            } else {
                logger.info("No active jobs to run");

            }
            try {
                sleep(300000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
