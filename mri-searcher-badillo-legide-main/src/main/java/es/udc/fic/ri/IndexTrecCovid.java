package es.udc.fic.ri;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

//@JsonIgnoreProperties(ignoreUnknown = true) para ignorar excepcion si fala algun campo
//@JsonProperty("imdb_id") cambiar el nombre de la variable de como aparece en el json por ejemplo para seguir convencion de nombres

public class IndexTrecCovid{

    public record TrecCovidRecord(
        String _id,
        String title,
        String text,
        Metadata metadata
    ){
        record Metadata(String url, String pubmed_id){}
    }

    private static void indexCorpus(TrecCovidRecord trecCovidRecord, IndexWriter indexWriter) throws IOException {
        org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

        doc.add(new KeywordField("_id", trecCovidRecord._id, Field.Store.YES));

        doc.add(new TextField("title", trecCovidRecord.title, Field.Store.YES));

        doc.add(new TextField("text", trecCovidRecord.text, Field.Store.YES));

        doc.add(new KeywordField("url", trecCovidRecord.metadata.url, Field.Store.YES));

        doc.add(new KeywordField("pubmed_id", trecCovidRecord.metadata.pubmed_id, Field.Store.YES));

        if(indexWriter.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE){
            System.out.println("Indexando corpus.jsonl  (CREATE)");
            indexWriter.addDocument(doc);
        }else{
            System.out.println("Indexando corpus.jsonl  (APPEND OR CREATE_OR_APPEND)");
            indexWriter.updateDocument(new Term("_id", trecCovidRecord._id), doc);
        }


    }



    private static void validateIndexPath(String indexPath){
        File indexDirectory = new File(indexPath);
        if (!indexDirectory.exists()) {
            if (indexDirectory.mkdirs()) {
                System.out.println("Directorio creado correctamente.");
            } else {
                throw new IllegalArgumentException("El path del índice debe ser válido");
            }
        }
    }

    private static void validateDocsPath(String docsPath){
        File indexDirectory = new File(docsPath);
        if (!indexDirectory.exists()) {
            if (indexDirectory.mkdirs()) {
                System.out.println("Directorio creado correctamente.");
            } else {
                throw new IllegalArgumentException("El path de los documentos corpus, queries y juicios de relevancia debe ser válido");
            }
        }
    }

    private static void validateOpenMode(String openMode){
        String openModeFormat = openMode.toLowerCase();
        if(!openModeFormat.equals("append") && !openModeFormat.equals("create") && !openModeFormat.equals("create_or_append")){
            throw new IllegalArgumentException("El modo de apertura debe ser append, create o create_or_append");
        }
    }

    private static void validateIndexingModel(String indexingModel, float indexingValue){
        if(indexingModel.equals("jm")){
            if(indexingValue < 0 || indexingValue > 1)
                throw new IllegalArgumentException("El valor para jm comprende el rango de [0-1]");
        }else if(indexingModel.equals("bm25")){
            if(indexingValue < 0.1 )
                throw new IllegalArgumentException("El valor para k1 debe ser mayor a 0.1");
        }else{
            throw new IllegalArgumentException("Debe utilizarse los modelos de similaridad Jelinek-Mercer o BM25");
        }

    }

    public static void main( String[] args ) throws IOException {

        String usage = "-openmode OPENMODE -index INDEXPATH -docs DOCSPATH -indexingmodel [jm LAMBDA | bm25 K1]";
        String indexPath = null;
        String docsPath = null;
        String openMode = null;
        String indexingModel = null;
        float indexingValue = 0;
        IndexWriter indexWriter = null;
        Analyzer analyzer = null;


        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    validateIndexPath(indexPath);
                    break;
                case "-docs":
                    docsPath = args[++i];
                    //validateDocsPath(docsPath);
                    break;
                case "-openmode":
                    openMode = args[++i];
                    validateOpenMode(openMode);
                    break;
                case "-indexingmodel":
                    indexingModel = args[++i];
                    indexingValue = Float.parseFloat(args[++i]);
                    validateIndexingModel(indexingModel, indexingValue);
                    break;
                default:
                    throw new IllegalArgumentException("Parámetro desconocido " + args[i]);
            }
        }

        if (indexPath == null || docsPath == null || indexingModel == null || openMode == null) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        //Creo el indexWriter
        try{
            //pillar el analyzer
            Directory dir = FSDirectory.open(Paths.get(indexPath)); //lugar en el que se va a crear o sobreescribir el indice
            analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer); //configuracion del writer
            if(openMode.equals("create")){
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            }else if(openMode.equals("create_or_append")){
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            }else{
                iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            }

            if(indexingModel.equals("jm")){
                iwc.setSimilarity(new LMJelinekMercerSimilarity(indexingValue));
            }else{
                iwc.setSimilarity(new BM25Similarity(indexingValue,0.75f));
            }

            indexWriter = new IndexWriter(dir,iwc);

        }catch (IOException e){
            e.printStackTrace();
        }

        var is = new FileInputStream(docsPath);
        //var is = IndexTrecCovid.class.getResourceAsStream("/trec-covid/corpus.jsonl");
        ObjectReader reader = JsonMapper.builder().findAndAddModules().build()
                .readerFor(TrecCovidRecord.class);
        List<TrecCovidRecord> trecCovidRecordList = null;

        try{
            trecCovidRecordList = reader.<TrecCovidRecord>readValues(is).readAll();
        }catch (IOException e){
            e.printStackTrace();
        }

        //indexar corpus.jsonl
        for(TrecCovidRecord record : trecCovidRecordList ){
            indexCorpus(record, indexWriter);
        }

        //indexar queries.jsonl

        if(indexWriter != null)
            indexWriter.close();

        if(is != null)
            is.close();
    }

}
