package no.nav.atom.fleischer.curlservice.test;

import no.nav.atom.fleischer.curlservice.repository.mongo.ResponseDocument;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDao;
import no.nav.atom.fleischer.curlservice.repository.mongo.dao.ResponseDaoImpl;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ResponseDaoTest {
    private static final String DB_SERVER_TEST = "localhost";
    private static final String DB_NAME_TEST = "test";
    private static final String RESPONSE_COLLECTION = "response_collection";
    private static final int PORT = 27017;
    private static final String URL_1 = "http://www.google.no";
    private static final String URL_2 = "http://www.google.com";
    private static ResponseDao responseDao = null;
    private static Logger logger = LoggerFactory.getLogger(ResponseDaoTest.class);
    private static ObjectId OBJECT_ID1 = new ObjectId();
    private static ObjectId OBJECT_ID2 = new ObjectId();
    private static ObjectId OBJECT_ID3 = new ObjectId();
    private static String FIELD_KEY_1 = "url";
    private static LocalDateTime ldt1 = LocalDateTime.now().withNano(0);
    private static LocalDateTime ldt2 = LocalDateTime.now().withNano(0).minusDays(1);
    private static LocalDateTime ldt3 = LocalDateTime.now().withNano(0).minusDays(2);


    private static int QTY_1 = 1;
    private static int QTY_2 = 2;
    private static int QTY_3 = 3;

    private ResponseDocument responseDocument1 = null;
    private ResponseDocument responseDocument2 = null;
    private ResponseDocument responseDocument3 = null;

    public ResponseDaoTest() {
        //if (mongoClient == null) mongoClient = ConnectionFactory.getMongoClient("localhost", 27017);
        //if (collection == null) collection = ConnectionFactory.getResponseDBCollection(mongoClient, DB_NAME,RESPONSE_COLLECTION);
    }

    private void init() {
        logger.debug("init()");
        responseDao = new ResponseDaoImpl(DB_SERVER_TEST, PORT, DB_NAME_TEST, RESPONSE_COLLECTION);
        responseDao.deleteAll();

        responseDocument1 = new ResponseDocument();
        responseDocument1.setId(OBJECT_ID1);
        responseDocument1.setUrl(URL_1);
        responseDocument1.setLocalDateTime(ldt2);
        responseDocument1.setZoneId(ZoneId.of("Europe/Paris"));
        responseDocument1.setAvgTimeNameLookup("0.004279");
        responseDocument1.setAvgTimeConnect("0.024336");
        responseDocument1.setAvgTimeAppConnect("0.000000");
        responseDocument1.setAvgTimePreTransfer("0.024360");
        responseDocument1.setAvgTimeRedirect("0.000000");
        responseDocument1.setAvgTimeStartTransfer("0.067001");
        responseDocument1.setAvgTimeTotal("0.067045");

        responseDocument2 = new ResponseDocument();
        responseDocument2.setId(OBJECT_ID2);
        responseDocument2.setUrl(URL_2);
        responseDocument2.setLocalDateTime(ldt1);
        responseDocument2.setZoneId(ZoneId.of("Europe/Paris"));
        responseDocument2.setAvgTimeNameLookup("0.004214");
        responseDocument2.setAvgTimeConnect("0.024109");
        responseDocument2.setAvgTimeAppConnect("0.000000");
        responseDocument2.setAvgTimePreTransfer("0.024147");
        responseDocument2.setAvgTimeRedirect("0.000000");
        responseDocument2.setAvgTimeStartTransfer("0.060911");
        responseDocument2.setAvgTimeTotal("0.060956");

        responseDocument3 = new ResponseDocument();
        responseDocument3.setId(OBJECT_ID3);
        responseDocument3.setUrl(URL_1);
        responseDocument3.setLocalDateTime(ldt3);
        responseDocument3.setZoneId(ZoneId.of("Europe/Paris"));
        responseDocument3.setAvgTimeNameLookup("0.004192");
        responseDocument3.setAvgTimeConnect("0.023296");
        responseDocument3.setAvgTimeAppConnect("0.000000");
        responseDocument3.setAvgTimePreTransfer("0.023383");
        responseDocument3.setAvgTimeRedirect("0.000000");
        responseDocument3.setAvgTimeStartTransfer("0.062805");
        responseDocument3.setAvgTimeTotal("0.062846");

        responseDao.create(responseDocument1);
        responseDao.create(responseDocument2);
        responseDao.create(responseDocument3);
    }

    @Test
    public void findByFieldTest() {
        logger.debug("\nfindByFieldTest()");
        init();
        List<ResponseDocument> responseDocuments = responseDao.findByField(FIELD_KEY_1, URL_1);
        assertEquals(QTY_2, responseDocuments.size());
        responseDao.deleteAll();
    }

    @Test
    public void findByDateTest() {
        logger.debug("\nfindByDateTest()");
        init();
        List<ResponseDocument> responseDocuments = responseDao.findByDate(ldt2);
        assertEquals(QTY_1, responseDocuments.size());
        responseDao.deleteAll();
    }

    @Test
    public void findByDateWithUrlTest() {
        logger.debug("\nfindByDateWithUrlTest()");
        init();
        List<ResponseDocument> responseDocuments = responseDao.findByDate(URL_1, ldt2);
        assertEquals(QTY_1, responseDocuments.size());
        responseDao.deleteAll();
    }

    @Test
    public void findBetweenDatesTest() {
        logger.debug("\nfindBetweenDatesTest()");
        init();
        List<ResponseDocument> responseDocuments = responseDao.findBetweenDates(ldt3, ldt1);
        assertEquals(QTY_3, responseDocuments.size());
        responseDao.deleteAll();
    }

    @Test
    public void findBetweenDatesWithUrlTest() {
        logger.debug("\nfindBetweenDatesWithUrlTest()");
        init();
        List<ResponseDocument> responseDocuments = responseDao.findBetweenDates(URL_1, ldt3, ldt1);
        assertEquals(QTY_2, responseDocuments.size());
        responseDao.deleteAll();
    }

    @Test
    public void findFirstTest() {
        logger.debug("\nfindFirst()");
        init();
        ResponseDocument responseDocument = responseDao.findFirst(URL_1);
        assertEquals(responseDocument, responseDocument3);
        logger.debug("response object values are: ");
        logger.debug(responseDocument.toString());
        responseDao.deleteAll();
    }

    @Test
    public void findLastTest() {
        logger.debug("\nfindLast()");
        init();
        ResponseDocument responseDocument = responseDao.findLast(URL_1);
        assertEquals(responseDocument, responseDocument1);
        logger.debug("response object values are: ");
        logger.debug(responseDocument.toString());
        responseDao.deleteAll();
    }

}
