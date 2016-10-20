package com.alma.opendata;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Statement;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;



public class BlazegraphExample {
	public static void main(String[] args) throws OpenRDFException {
        Properties props = new Properties();
        try {
            File journal = File.createTempFile("/tmp/blazegraph/test", ".jnl");
            props.put(Options.BUFFER_MODE, BufferMode.DiskRW); // persistent file system located journal
            props.put(Options.FILE, journal.getAbsolutePath()); // journal file location
        } catch (IOException e) {
            e.printStackTrace();
        }

		BigdataSail sail = new BigdataSail(props); // instantiate a sail
		Repository repo = new BigdataSailRepository(sail); // create a Sesame repository

		repo.initialize();
        /* prepare a statement
        URIImpl subject = new URIImpl("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        URIImpl predicate = new URIImpl("http://data-vocabulary.org/Breadcrumb");
        Literal object = new LiteralImpl("http://0xwhaii.deviantart.com/art/Punk-America-X-DJ-Reader-Hot-Damn-VIII-310447740");
        Statement stmt = new StatementImpl(subject, predicate, object);
         */
        // open repository connection
        RepositoryConnection cxn = repo.getConnection();

        // upload data to repository
        try {
            cxn.begin();
            cxn.add(new File("src/main/resources/data16.nq"), "base:", RDFFormat.NQUADS);
            cxn.commit();
        } catch (OpenRDFException ex) {
            cxn.rollback();
            throw ex;
        } catch (IOException e) {
            e.printStackTrace();
        }

        // evaluate sparql query

        /*final TupleQuery tupleQuery = cxn
                .prepareTupleQuery(QueryLanguage.SPARQL,
                        "select * where { GRAPH :g { ?s ?p ?o . } }");
        TupleQueryResult result = tupleQuery.evaluate();*/
        RepositoryResult<Statement> result = cxn.getStatements(null, null, null, true);
        int cpt = 0;
        while (result.hasNext()) {
            Statement bindingSet = result.next();
            BigdataStatement bdStmt = (BigdataStatement) bindingSet;
            if(bdStmt.isExplicit()) {
                System.out.println(bdStmt);
                cpt++;
            }
        }
        System.out.println(cpt);
        result.close();
        // close the repository connection
        cxn.close();
        repo.shutDown();
    }
}
