# log-file-processor
log-file-processor

# Build
<code>
mvn clean install
</code>

# Test Class
<code>
com.test.app.Event.EventProcessorTest
</code>


# Test Case With Input as a File
<code>
@Test
    public void testWithBulkLoadInput() throws Exception 
    
    test/resources/logfile.txt (10k records)
    Test Data has been setup with Event IDs {"id":"99920"} and {"id":"99990"} taking more than 4 ms.
    Expected two events with alert=true and remaining 9997 events with alert=false
</code>

# Scope for the improvement
 - Process variables can be externalised in the config/properties file
 - Additional tests can be added to assert optional fields like hostname, type





