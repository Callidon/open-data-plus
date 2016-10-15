package com.alma.opendata;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;



public class BlazegraphExample {
	public static void main(String[] args) throws OpenRDFException {

		final Properties props = new Properties();
		props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		props.put(Options.FILE, "/tmp/blazegraph/test.jnl"); // journal file location

		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository

		repo.initialize();

		try {
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
				try {
					cxn.add(new File("data16.nq"),"base:",RDFFormat.NQUADS);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				cxn.commit();
			} catch (OpenRDFException ex) {
				cxn.rollback();
				throw ex;
			} finally {
				// close the repository connection
				cxn.close();
			}

			// open connection
			if (repo instanceof BigdataSailRepository) {
				cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
			} else {
				cxn = repo.getConnection();
			}
			

			// evaluate sparql query
			try {

				final TupleQuery tupleQuery = cxn
						.prepareTupleQuery(QueryLanguage.SPARQL,
								"select * where { GRAPH ?g {?s ?p ?o .} }");
				TupleQueryResult result = tupleQuery.evaluate();
				try {
					while (result.hasNext()) {
						BindingSet bindingSet = result.next();
						System.err.println(bindingSet);
					}
				} finally {
					result.close();
				}

			} finally {
				// close the repository connection
				cxn.close();
			}

		} finally {
			repo.shutDown();
		}
	}
}
