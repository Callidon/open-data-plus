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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by thomas on 20/10/16.
 */
public class NQuadsLoader {

    private RepositoryConnection cn;
    private final static Logger LOGGER = Logger.getLogger(NQuadsLoader.class.getCanonicalName());

    public NQuadsLoader(RepositoryConnection cn) {
        this.cn = cn;
    }

    public List<String> load(String url, String graph) throws RepositoryException {
        List<String> rejectedQuads = new ArrayList<>();

        cn.begin();
        try {
            Path path = FileSystems.getDefault().getPath(url);
            Files.lines(path)
                    .forEach(line -> {
                        StringReader reader = new StringReader(line);
                        try {
                            Model model = Rio.parse(reader, graph, RDFFormat.NQUADS);
                            cn.add(model);
                        } catch (RDFParseException e) {
                            rejectedQuads.add(line);
                            LOGGER.warning(e.getMessage());
                        } catch (RepositoryException | IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            cn.rollback();
            e.printStackTrace();
        } finally {
            cn.commit();
        }
        return rejectedQuads;
    }
}
