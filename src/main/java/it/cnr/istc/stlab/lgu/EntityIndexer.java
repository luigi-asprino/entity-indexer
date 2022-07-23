package it.cnr.istc.stlab.lgu;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;


public class EntityIndexer {

    private static final Logger log = LogManager.getLogger(EntityIndexer.class);
    private final String laundromatFolder, processedFile;
    private final AtomicLong processedFiles = new AtomicLong(0);
    private final RocksDB outDB;
    private final Set<CharSequence> processing;
    private final FileOutputStream fosProcessed;
    private final FileOutputStream fosErrors;
    private final RDFParserBuilder b = RDFParser.create().lang(Lang.NQUADS);

    public EntityIndexer(RocksDB outDB, String laundromatFolder, String processedFile) throws RocksDBException, FileNotFoundException {
        super();

        this.outDB = outDB;
        this.laundromatFolder = laundromatFolder;
        this.processing = new ConcurrentSkipListSet<>();
        this.processedFile = processedFile;

        this.fosProcessed = new FileOutputStream("processed_" + System.currentTimeMillis() + ".txt");
        this.fosErrors = new FileOutputStream("errors" + System.currentTimeMillis() + ".txt");

    }

    public void close() throws IOException {
        this.fosErrors.flush();
        this.fosErrors.close();
        this.fosProcessed.flush();
        this.fosProcessed.close();
    }

    public void build() {
        log.info("Start");
        System.out.println("Start");
        log.trace("Trace test");

        List<String> l = Utils.readFileToListString("entities.txt");
        ImmutableSet<String> entities = ImmutableSet.<String>builder().addAll(l).build();

        log.info("Entities loaded " + entities.size());
        try {

            ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
            ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    log.info("Files processed " + processedFiles.get());
                    log.info("Processing ");
                    for (CharSequence s : processing) {
                        log.info("\t" + s);
                    }
                }
            }, 1, 1, TimeUnit.MINUTES);

            Files.walk(Paths.get(laundromatFolder)).filter(f -> FilenameUtils.isExtension(f.getFileName().toAbsolutePath().toString(), "hdt") || FilenameUtils.getName(f.getFileName().toString()).endsWith("data.nq.gz")).parallel().forEach(new DatasetConsumer(entities));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void collectTriple(Triple q, ImmutableSet<String> esgEntities, Set<CharSequence> entities) {
        try {
            if (q.getSubject().isURI() && esgEntities.contains(q.getSubject().getURI())) {
                entities.add(q.getSubject().getURI());
            }

            if (q.getPredicate().isURI() && esgEntities.contains(q.getPredicate().getURI())) {
                entities.add(q.getPredicate().getURI());
            }

            if (q.getObject().isURI() && esgEntities.contains(q.getObject().getURI())) {
                entities.add(q.getObject().getURI());
            }

        } catch (Exception e) {
            log.error("Error " + e.getMessage());
        }
    }

    private void collectURI(CharSequence e, ImmutableSet<String> esgEntities, Set<CharSequence> entities) {
        log.trace("e: " + e.toString());
        if (esgEntities.contains(e.toString())) {
            entities.add(e);
        }
    }

    class DatasetConsumer implements Consumer<Path> {
        private final ImmutableSet<String> esgEntities;

        public DatasetConsumer(ImmutableSet<String> entities) {
            this.esgEntities = entities;
        }

        @Override
        public void accept(Path t) {
            try {
                File processed = new File(t.getParent().toFile().getAbsolutePath() + "/" + processedFile);
                log.info("Processing " + t.getFileName().toString() + " " + t.getParent().getFileName().toString() + " " + processed.getName() + " " + processedFiles.get());
                if (processed.exists()) {
                    log.info("Skipping " + t.getFileName().toString() + " " + t.getParent().getFileName().toString());
                    return;
                }
                AtomicLong nOfTriples = new AtomicLong();
                processing.add(t.getParent().getFileName().toString());
                Set<CharSequence> entities = new HashSet<>();
                log.trace("Filename: " + t.getFileName() + " " + t.getFileName().endsWith("hdt"));
                if (FilenameUtils.isExtension(t.toFile().getAbsolutePath(), "hdt")) {
                    log.trace("Processing hdt");
                    HDT hdt;
                    hdt = HDTManager.mapIndexedHDT(t.toFile().getAbsolutePath(), null);
                    log.trace("Size HDT " + hdt.size());

                    nOfTriples.addAndGet(hdt.size());

                    hdt.getDictionary().getSubjects().getSortedEntries().forEachRemaining(e -> collectURI(e, esgEntities, entities));
                    hdt.getDictionary().getPredicates().getSortedEntries().forEachRemaining(e -> collectURI(e, esgEntities, entities));
                    hdt.getDictionary().getObjects().getSortedEntries().forEachRemaining(e -> collectURI(e, esgEntities, entities));

                    hdt.close();
                }

                if (t.getFileName().endsWith("data.nq.gz")) {
                    log.trace("Processing data.nq.gz");
                    StreamRDF s = new StreamRDFBase() {
                        public void triple(Triple q) {
                            nOfTriples.incrementAndGet();
                            collectTriple(q, esgEntities, entities);
                        }

                        public void quad(Quad q) {
                            nOfTriples.incrementAndGet();
                            collectTriple(q.asTriple(), esgEntities, entities);
                        }
                    };

                    try {

                        InputStream is = new GZIPInputStream(new FileInputStream(t.toFile()), 20 * 1024);
                        BufferedReader br = new BufferedReader(new InputStreamReader(is), 20 * 1024);
                        AtomicLong al = new AtomicLong(0L);
                        String l;
                        StringBuilder sb = new StringBuilder();
                        while ((l = br.readLine()) != null) {
                            nOfTriples.incrementAndGet();
                            sb.append(l);
                            sb.append('\n');
                            al.incrementAndGet();

                            if (al.get() % 10000 == 0) {
                                String toParse = sb.toString();
                                try {
                                    b.fromString(toParse).parse(s);
                                    log.info("Progress " + t.getParent().toFile().getAbsolutePath() + " " + al.get() + " " + entities.size());
                                } catch (Exception e) {
                                    log.error("Error line " + t.getParent().toFile().getAbsolutePath());
                                    for (String ll : toParse.split("\n")) {
                                        b.fromString(ll).parse(s);
                                    }
                                }
                                sb = new StringBuilder();
                            }

                        }

                    } catch (Exception e) {
                        log.error("Parse exception");
                        e.printStackTrace();
                        fosErrors.write(("Error parsing with" + t.toFile().getAbsolutePath() + "\n").getBytes());
                    }

                }

                log.trace("Processed " + t.getFileName().toString() + " size entities: " + entities.size() + " triples " + nOfTriples.get());

                for (CharSequence e : entities) {
                    outDB.merge(t.getParent().getFileName().toString().getBytes(), e.toString().getBytes());
                }

                FileOutputStream fos = new FileOutputStream(processed);
                fos.write(new Date(System.currentTimeMillis()).toString().getBytes());
                fos.flush();
                fos.close();

                processing.remove(t.getParent().getFileName().toString());
                processedFiles.incrementAndGet();
                fosProcessed.write(t.getParent().getFileName().toString().getBytes());
                fosProcessed.write((" " + nOfTriples.get()).getBytes());
                fosProcessed.write('\n');

                log.info("Processed " + t.getFileName().toString() + " " + t.getParent().getFileName().toString() + " num of entities " + entities.size());

            } catch (Exception e) {
                log.trace("Error while processing " + t.getParent().getFileName().toString());
                log.error(e.getMessage());
                e.printStackTrace();
                try {
                    fosErrors.write(("Error processing with" + t.toFile().getAbsolutePath() + "\n").getBytes());
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

    }

}
