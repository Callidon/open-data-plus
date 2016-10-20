package com.alma.opendata;

import org.openrdf.model.Model;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;

/**
 * Created by thomas on 20/10/16.
 */
public class NQuadsLoader {

    private RepositoryConnection cn;

    public NQuadsLoader(RepositoryConnection cn) {
        this.cn = cn;
    }

    public void load(String url) throws RepositoryException {
        final int[] cpt = {0};
        cn.begin();
        try {
            Files.lines(FileSystems.getDefault().getPath(url))
                    .forEach(line -> {
                        StringReader r = new StringReader(line);
                        cpt[0]++;
                        try {
                            Model model = Rio.parse(r, "base:", RDFFormat.NQUADS);
                            cn.add(model);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (RDFParseException e) {
                            System.err.println(e.getMessage() + " line " + cpt[0]);
                        } catch (RepositoryException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            cn.rollback();
            e.printStackTrace();
        } finally {
            cn.commit();
        }
    }
}
