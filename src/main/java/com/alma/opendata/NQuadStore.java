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
import java.util.Scanner;

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
        Scanner sc = new Scanner(System.in);
        try {
            NQuadStore store = new NQuadStore("src/main/resources/base-config.properties");
            System.out.println("File to load ('located in src/main/resources/') : ");
            String file = sc.nextLine();
            store.load("src/main/resources/" + file, "base:");
            System.out.println("Query to execute :");
            while(sc.hasNext()) {
                String request = sc.nextLine();
                //TupleQueryResult result = store.request("select * where { GRAPH ?g { ?s ?p ?o . } }", "base:");
                TupleQueryResult result = null;
                try {
                    result = store.request(request, "base:");
                    while (result.hasNext()) {
                        BindingSet bindingSet = result.next();
                        System.out.println(bindingSet);
                    }
                    result.close();
                } catch (RepositoryException e) {
                    e.printStackTrace();
                } catch (MalformedQueryException e) {
                    System.out.println(e.getMessage());
                    continue;
                } catch (QueryEvaluationException e) {
                    System.out.println(e.getMessage());
                    continue;
                }
                System.out.println("Query to execute :");
            }
            store.shutdown();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }

    }
}
