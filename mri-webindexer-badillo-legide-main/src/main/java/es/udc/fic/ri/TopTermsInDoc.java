package es.udc.fic.ri;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class TopTermsInDoc{

    private static void validatePath(String somePath){
        Path path = Paths.get(somePath);
        if (!Files.isDirectory(path))
            throw new IllegalArgumentException("El parametro -index debe ser un directorio válido");
    }

    private static void validateOutfilePath(String somePath) {
        Path path = Paths.get(somePath);
        Path parent = path.getParent();
        if (parent != null && !Files.isDirectory(parent)) {
            throw new IllegalArgumentException("El parametro -outfile debe ser un directorio válido");
        }
    }

    private static void validateDocID(int docID){
        if (docID < 0)
            throw new IllegalArgumentException("El docID debe ser válido ( > 0 )");
    }

    private static void validateTopN(int top){
        if (top <= 0)
            throw new IllegalArgumentException("El top N debe ser válido ( > 0 )");

    }

    private static void validateUrl(String url){

        String urlPattern = "^(http://|https://).*\\..*$";
        if (!Pattern.matches(urlPattern, url)){
            throw new IllegalArgumentException("La url debe tener un formato válido");
        }
    }

    private static void validateField(String field){
        if(!field.equals("body") && !field.equals("title")){
            throw new IllegalArgumentException("El field especificado debe tener term vectors");
        }
    }

    private static int findDocIDByUrl(IndexReader reader, String url) throws IOException {
        for (int docID = 0; docID < reader.maxDoc(); docID++) {
            Document doc = reader.document(docID);
            String path = doc.get("path");
            path = path.substring( path.lastIndexOf("\\")+ 1);
            if (path.equals(url)) {
                return docID;
            }
        }
        System.out.println("No existe un documento referente a la url " + url);
        return -1;
    }

    public static void main(String[] args){

        String usage = " -index INDEX_PATH -field FIELD (-docID int | -url URL) -top n -outfile PATH";
        String indexPath = null;
        String field = null;
        int docID = -1;
        int top = -1;
        String outfilePath = null;
        String url = null;

        for (int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i]; //Cojo el argumento que va despues de -index -> INDEX_PATH
                    validatePath(indexPath);
                    break;
                case "-field":
                    field = args[++i];
                    validateField(field);
                    break;
                case "-docID":
                    docID = Integer.parseInt(args[++i]);
                    validateDocID(docID);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    validateTopN(top);
                    break;
                case "-outfile":
                    outfilePath = args[++i];
                    validateOutfilePath(outfilePath);
                    break;
                case "-url":
                    url = args[++i];
                    validateUrl(url);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        //Si no le paso argumentos, indico al usuario como hacerlo correctamente
        if (indexPath == null || field == null || top == -1 || outfilePath == null){
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        //Paso la url al formato con el que se guardan los .loc.notags
        if (url != null){
            url = url.replaceAll("^(https?://)", "").replaceAll("/", "_") + ".loc";
        }

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))){


            if (docID == -1 && url != null) {
                docID = findDocIDByUrl(reader, url);
                if(docID == -1){
                    System.exit(-1);
                }
            }

            TermVectors termvectors = reader.termVectors();
            Terms terms = termvectors.get(docID, field);

            Map<String, Integer> termFreqMap = new HashMap<>(); //Mapa para el tf
            Map<String, Integer> docFreqMap = new HashMap<>(); //Mapa para el df
            Map<String, Double> tfidfMap = new HashMap<>(); //Mapa para el tf*idflog10


            if (terms != null){
                TermsEnum termsEnum = terms.iterator();
                BytesRef text;
                while ((text = termsEnum.next()) != null){
                    String termText = text.utf8ToString();

                    int freq = (int) termsEnum.totalTermFreq(); //tf
                    termFreqMap.put(termText, freq);

                    int docFreq = reader.docFreq(new Term(field, termText)); //df
                    docFreqMap.put(termText, docFreq);
                }

                for (Map.Entry<String, Integer> entry : termFreqMap.entrySet()){ //Calcular tf*idflog10
                    String termText = entry.getKey();
                    int tf = entry.getValue();
                    int df = docFreqMap.get(termText);
                    int totalDocs = reader.numDocs();
                    double idf = Math.log10((double) totalDocs / (double) df);
                    double tfidf = tf * idf;
                    tfidfMap.put(termText, tfidf);
                }
            }

            // Crear una lista ordenada de los términos según su TF * IDF (tfidf)
            List<Map.Entry<String, Double>> sortedTerms = new ArrayList<>(tfidfMap.entrySet());
            sortedTerms.sort(Map.Entry.<String, Double>comparingByValue().reversed());

            // Abrir el archivo de salida para escribir los resultados
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outfilePath))){ //newFileWriter con opcion false por defecto para sobreescribir

                if (!Files.exists(Paths.get(outfilePath))){
                    Files.createFile(Paths.get(outfilePath)); // Crear el archivo si no existe
                }

                System.out.println("Los " + top + " términos ordenados por (raw tf) x idflog10 del documento " + docID + ":\n");
                writer.write("Los " + top + " términos ordenados por (raw tf) x idflog10 del documento " + docID + ":\n");

                int count = 0;
                for(Map.Entry<String, Double> entry : sortedTerms){
                    if (count >= top) break;
                    String termText = entry.getKey();
                    double tfidf = entry.getValue();
                    int tf = termFreqMap.get(termText);
                    int df = docFreqMap.get(termText);
                    writer.write(termText + ": TF= " + tf + ", DF= " + df + ", TF x IDFLOG10= " + tfidf + "\n");

                    System.out.println(termText + ": TF= " + tf + ", DF= " + df + ", TF x IDFLOG10= " + tfidf);
                    count++;
                }

                System.out.println("Los resultados se han guardado en: " + outfilePath);

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

