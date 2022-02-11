package com.test.app;

import com.test.app.dao.EventDao;
import com.test.app.dao.EventDaoImpl;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.ClassLoader.getSystemResource;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventProcessorTest extends AbstractEventProcessorTest {
    public static final String LOGFILE_TXT = "logfile.txt";
    private static final EventDao eventDao = EventDaoImpl.getInstance();

    public EventProcessorTest() {
        super(eventDao);
    }

    @Before
    public void beforeTest() throws Exception {
        eventDao.truncateTable();
    }

    @Test
    public void testWithAlertTrueAndFalse() throws Exception {
        List<String> inputLines = Arrays.asList("{\"id\":\"6\",\"state\":\"FINISHED\",\"timestamp\":1635636798468}",
                "{\"id\":\"7\",\"state\":\"FINISHED\",\"timestamp\":1635636798468}",
                "{\"id\":\"6\",\"state\":\"STARTED\",\"timestamp\":1635636798458}",
                "{\"id\":\"7\",\"state\":\"STARTED\",\"timestamp\":1635636798466}");
        executeTest(inputLines.stream());
        assertThat(eventDao.getRecordCount(true), is(1L));
        assertThat(eventDao.getRecordCount(false), is(1L));
    }

    @Test
    public void testWithExecutionTimeIsLessThanThreshold() throws Exception {
        List<String> inputLines = Arrays.asList("{\"id\":\"6\",\"state\":\"FINISHED\",\"timestamp\":1635636798460}",
                "{\"id\":\"7\",\"state\":\"FINISHED\",\"timestamp\":1635636798468}",
                "{\"id\":\"6\",\"state\":\"STARTED\",\"timestamp\":1635636798458}",
                "{\"id\":\"7\",\"state\":\"STARTED\",\"timestamp\":1635636798466}");
        executeTest(inputLines.stream());
        assertThat(eventDao.getRecordCount(true), is(0L));
        assertThat(eventDao.getRecordCount(false), is(2L));
    }

    @Test
    public void testWithExecutionTimeIsMoreThanThreshold() throws Exception {
        List<String> inputLines = Arrays.asList("{\"id\":\"6\",\"state\":\"FINISHED\",\"timestamp\":1635636798465}",
                "{\"id\":\"7\",\"state\":\"FINISHED\",\"timestamp\":1635636798472}",
                "{\"id\":\"6\",\"state\":\"STARTED\",\"timestamp\":1635636798458}",
                "{\"id\":\"7\",\"state\":\"STARTED\",\"timestamp\":1635636798466}");
        executeTest(inputLines.stream());
        assertThat(eventDao.getRecordCount(true), is(2L));
        assertThat(eventDao.getRecordCount(false), is(0L));
    }

    @Test
    public void testWithBulkLoadInput() throws Exception {
        Stream<String> lines = Files.lines(Paths.get(getSystemResource(LOGFILE_TXT).toURI()));
        executeTest(lines);
        assertThat(eventDao.getRecordCount(true), is(2L));
        assertThat(eventDao.getRecordCount(false), is(99997L));
    }
}

