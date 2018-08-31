package no.nav.atom.fleischer.curlservice.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import no.nav.atom.fleischer.curlservice.repository.mongo.CURLDocument;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.CURLDaoImpl;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CURLDaoTest {
    private static final String DB_SERVER_TEST = "localhost";
    private static final String DB_NAME_TEST = "test";
    private static final String CURLS_COLLECTION = "curls_collection";
    private static final int PORT = 27017;
    private static final int QTY1 = 1;
    private static final int QTY2 = 2;
    private static final String URL_1 = "http://www.google.com";
    private static final String URL_2 = "http://www.google.no";
    private static final String URL_3 = "http://www.nrk.no";
    private static final String CRONTAB_1 = "1 * * * * ?";
    private static final String CRONTAB_2 = "2 * * * * ?";
    private static final String CRONTAB_3 = "3 * * * * ?";
    private static MongoClient mongoClient;
    private static CURLDao curlDao = null;
    private static Logger logger = LoggerFactory.getLogger(CURLDaoImpl.class);
    private static boolean ACTIVE_TRUE = true;
    private static boolean ACTIVE_FALSE = false;
    private static ObjectId OBJECT_ID1 = new ObjectId();
    private static ObjectId OBJECT_ID2 = new ObjectId();
    private MongoCollection<CURLDocument> collection;
    private CURLDocument curlDocument1 = null;
    private CURLDocument curlDocument2 = null;


    public CURLDaoTest() {

    }

    private void init() {
        logger.debug("\ninit()");
        curlDao = new CURLDaoImpl(DB_SERVER_TEST, PORT, DB_NAME_TEST, CURLS_COLLECTION);
        curlDao.deleteAll();

        curlDocument1 = new CURLDocument();
        curlDocument1.setId(OBJECT_ID1);
        curlDocument1.setUrl(URL_1);
        curlDocument1.setCrontab(CRONTAB_1);
        curlDocument1.setActive(ACTIVE_TRUE);
        curlDao.create(curlDocument1);

        curlDocument2 = new CURLDocument();
        curlDocument2.setId(OBJECT_ID2);
        curlDocument2.setUrl(URL_2);
        curlDocument2.setCrontab(CRONTAB_2);
        curlDocument2.setActive(ACTIVE_FALSE);
        curlDao.create(curlDocument2);
    }

    @Test
    public void findAllTest() {
        logger.debug("\nfindAllTest()");
        init();
        List<CURLDocument> list = curlDao.findAll();
        assertEquals(QTY2, list.size());
        curlDao.deleteAll();
    }

    @Test
    public void findActive() {
        logger.debug("\nfindActive()");
        init();
        List<CURLDocument> list = curlDao.findActive();
        assertEquals(1, list.size());
        curlDao.deleteAll();
    }

    @Test
    public void findDeactivated() {
        logger.debug("\nfindDeactivated()");
        init();
        List<CURLDocument> list = curlDao.findDeactivated();
        assertEquals(1, list.size());
        curlDao.deleteAll();
    }

    @Test
    public void findByIdTest() {
        logger.debug("\nfindById({})", OBJECT_ID1);
        init();
        CURLDocument res = curlDao.findById(OBJECT_ID1);
        assertEquals(CURLDocument.class, res.getClass());
        curlDao.deleteAll();
    }

    @Test
    public void updateUrlTest() {
        logger.debug("\nupdateUrlTest()");
        init();
        UpdateResult res = curlDao.updateUrl(OBJECT_ID1, URL_3);
        assertEquals(1, res.getModifiedCount());
        assertEquals(null, res.getUpsertedId());
        assertEquals(1, res.getMatchedCount());
        assertTrue(res.wasAcknowledged());
        curlDao.deleteAll();
    }

    @Test
    public void updateCrontabTest() {
        logger.debug("\nupdateCrontabTest");
        init();
        UpdateResult res = curlDao.updateCrontab(OBJECT_ID1, CRONTAB_3);
        assertEquals(1, res.getModifiedCount());
        assertEquals(null, res.getUpsertedId());
        assertEquals(1, res.getMatchedCount());
        assertTrue(res.wasAcknowledged());
        curlDao.deleteAll();
    }

    @Test
    public void setActiveTest() {
        logger.debug("\nsetActiveTest()");
        init();
        UpdateResult res = curlDao.setActive(OBJECT_ID1, ACTIVE_FALSE);
        assertEquals(1, res.getModifiedCount());
        assertEquals(null, res.getUpsertedId());
        assertEquals(1, res.getMatchedCount());
        assertTrue(res.wasAcknowledged());
        curlDao.deleteAll();
    }

    @Test
    public void deleteOneTest() {
        logger.debug("\ndeleteOneTest()");
        init();
        CURLDocument curlDocument = curlDao.delete(OBJECT_ID1);
        assertEquals(curlDocument1, curlDocument);
        curlDao.deleteAll();
    }
}
