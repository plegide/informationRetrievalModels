package es.udc.fic.ri;

import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;


public class TopTermsInField {



    private static void validatePath(String somePath){
        Path path = Paths.get(somePath);
        if(!Files.isDirectory(path))
            throw new IllegalArgumentException("El parametro -index debe ser un directorio válido");
    }

    private static void validateTopN(int top){
        if(top <= 0)
            throw new IllegalArgumentException("El top N debe ser válido ( > 0 )");

    }

    private static void validateField(String field){
        if(!field.equals("body") && !field.equals("title")){
            throw new IllegalArgumentException("El field especificado debe tener term vectors");
        }
    }

    public static void main( String[] args ) {

        String usage = " -index INDEX_PATH -field FIELD -top n -outfile PATH";
        String indexPath = null;
        String field = null;
        int top = -1;
        String outfilePath = null;



        //Bucle para guardar cada argumento indicado
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i]; //Cojo el argumento que va despues de -index -> INDEX_PATH
                    validatePath(indexPath);
                    break;
                case "-field":
                    field = args[++i];
                    validateField(field);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    validateTopN(top);
                    break;
                case "-outfile":
                    outfilePath = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }
        //Si no le paso argumentos, indico al usuario como hacerlo correctamente
        if(indexPath == null || field == null || top == -1 || outfilePath == null){
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {

            Map<String, Integer> termDocFreqMap = new HashMap<>();

            //iterar sobre todos los términos y calcular su frecuencia de documento
            for (int docID = 0; docID < reader.maxDoc(); docID++) {
                TermVectors termvectors = reader.termVectors();
                Terms terms = termvectors.get(docID, field);
                if (terms != null) {
                    TermsEnum termsEnum = terms.iterator();
                    BytesRef term;
                    while ((term = termsEnum.next()) != null) {
                        String termText = term.utf8ToString();
                        //Guardo en el mapa el termino que encuentro y si no estaba consigo 0 y le añado 1, en otro caso añado uno mas al valor que tenia
                        //es decir, añado uno más a la frecuencia de documentos que tenia
                        termDocFreqMap.put(termText, termDocFreqMap.getOrDefault(termText, 0) + 1);
                    }
                }
            }

            //ordenar los términos por su frecuencia de documento en orden descendente
            List<Map.Entry<String, Integer>> sortedTerms = new ArrayList<>(termDocFreqMap.entrySet());
            sortedTerms.sort(Map.Entry.<String, Integer>comparingByValue().reversed()); //Ordena el set por valor y se usa reversed para que sea en orden descendente



            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outfilePath))) { //newFileWriter con opcion false por defecto para sobreescribir

                if (!Files.exists(Paths.get(outfilePath))) {
                    Files.createFile(Paths.get(outfilePath)); // Crear el archivo si no existe
                }

                System.out.println("Los " + top + " términos con mayor frecuencia de documento:\n");
                writer.write("Los " + top + " términos con mayor frecuencia de documento:\n");
                int count = 0;
                for (Map.Entry<String, Integer> entry : sortedTerms) {
                    if (count >= top) break;
                    String term = entry.getKey();
                    int docFreq = entry.getValue();
                    writer.write(term + ": DF= " + docFreq + "\n");

                    System.out.println(term + ": DF= " + docFreq);

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
