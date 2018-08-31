package no.nav.atom.fleischer.curlservice;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;

class JobAndTrigger {
    private JobDetail jobDetail;
    private CronTrigger cronTrigger;

    JobAndTrigger(JobDetail jobDetail, CronTrigger cronTrigger) {
        this.jobDetail = jobDetail;
        this.cronTrigger = cronTrigger;
    }

    JobDetail getJobDetail() {
        return jobDetail;
    }

    CronTrigger getCronTrigger() {
        return cronTrigger;
    }

}
