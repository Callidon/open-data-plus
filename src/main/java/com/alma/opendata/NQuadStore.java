package com.alma.opendata;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 *
 */
public class NQuadStore {
    private Properties properties = new Properties();
    private BigdataSail sail;
    private Repository repository;

    public NQuadStore(String propertiesFile) throws RepositoryException {
        try {
            FileReader reader = new FileReader(propertiesFile);
            properties.load(reader);

            // store graph in a temporary file
            File journal = File.createTempFile("/tmp/blazegraph/test", ".jnl");
            properties.put(Options.BUFFER_MODE, BufferMode.DiskRW); // persistent file system located journal
            properties.put(Options.FILE, journal.getAbsolutePath()); // journal file location
        } catch (IOException e) {
            e.printStackTrace();
        }
        sail = new BigdataSail(properties); // instantiate a sail
        repository = new BigdataSailRepository(sail); // create a Sesame repository
        repository.initialize();
    }

    public void load(String file, String graph) throws RepositoryException {
        RepositoryConnection connection = repository.getConnection();
        NQuadsLoader loader = new NQuadsLoader(connection);
        loader.load(file, graph);
        connection.close();
    }

    public TupleQueryResult request(String request, String graph) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        RepositoryConnection connection = repository.getConnection();
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, request, graph);
        connection.close();
        return tupleQuery.evaluate();
    }

    public void shutdown() throws RepositoryException {
        repository.shutDown();
    }

    public static void main(String[] args) {
        try {
            NQuadStore store = new NQuadStore("src/main/resources/base-config.properties");
            store.load("/home/thomas/open-data-plus/src/main/resources/data16.nq", "base:");
            TupleQueryResult result = store.request("select * where { GRAPH ?g { ?s ?p ?o . } }", "base:");
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                System.out.println(bindingSet);
            }
            result.close();
            store.shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
