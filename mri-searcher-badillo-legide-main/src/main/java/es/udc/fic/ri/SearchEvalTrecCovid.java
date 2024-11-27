package es.udc.fic.ri;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchEvalTrecCovid {


    private static void validateIndexPath(String indexPath){
        Path path = Paths.get(indexPath);

        if(!Files.exists(path)){
            throw new IllegalArgumentException("El índice indicado en los parámetros no existe");
        }

    }

    private static void validateSearchModel(String indexingModel, float indexingValue){
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

    private static void validateCut(int cut){
        if (cut <= 0)
            throw new IllegalArgumentException("El cut N debe ser válido ( cut > 0 )");
    }

    private static void validateTop(int top){
        if (top <= 0 || top > 171332)
            throw new IllegalArgumentException("El top M debe ser válido ( top > 0 && top < 171332)");
    }

    private static List<JsonNode> readQueriesFile() throws IOException {
        var queriesInputStream = IndexTrecCovid.class.getResourceAsStream("/trec-covid/queries.jsonl");
        ObjectReader reader = JsonMapper.builder().findAndAddModules().build()
                .readerFor(JsonNode.class);
        List<JsonNode> queriesList = null;
        try{
            queriesList = reader.<JsonNode>readValues(queriesInputStream).readAll();
        }catch(IOException e){
            e.printStackTrace();
        }
        if(queriesInputStream != null)
            queriesInputStream.close();

        return queriesList;
    }

    private static Map<String, Map<String, Integer>> readRelevanceFile() {
        InputStream judgmentsInputStream = IndexTrecCovid.class.getResourceAsStream("/trec-covid/qrels/test.tsv");
        Map<String, Map<String, Integer>> judgments = new HashMap<>();
        if (judgmentsInputStream != null) {
            try (BufferedReader buffer = new BufferedReader(new InputStreamReader(judgmentsInputStream))) {
                String line = buffer.readLine();  //Para saltarse la primera linea
                while ((line = buffer.readLine()) != null) {
                    String[] parts = line.split("\t"); // Dividir la línea utilizando el carácter de tabulación como delimitador
                    String queryId = parts[0]; // El queryID está en la primera columna
                    String docId = parts[1]; // El docID está en la segunda columna
                    int relevance = Integer.parseInt(parts[2]); // Suponiendo que la relevancia está en la tercera columna
                    // Verificar si ya existe una entrada para este queryID, si no, crear una nueva
                    if (!judgments.containsKey(queryId)) {
                        judgments.put(queryId, new HashMap<>());
                    }
                    judgments.get(queryId).put(docId, relevance); // Almacenar la relevancia para el documento y el query correspondientes
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return judgments;
    }


    private static int [] validateRange(String range){
        if(range.equals("all")){
            return new int[]{1,50};
        }

        String[] partes = range.split("-");
        int[] numeros = new int[2];


        numeros[0] = Integer.parseInt(partes[0]);
        if(numeros[0] < 1){
            throw new IllegalArgumentException("El valor mínimo para el rango de queries empieza en 1");
        }

        if(partes.length != 2){
            numeros[1] = numeros[0];
            return numeros;
        }
        numeros[1] = Integer.parseInt(partes[1]);

        if(numeros[1] > 50){
            throw new IllegalArgumentException("El valor máximo para el rango de queries es 50");
        }

        if(numeros[0] > numeros[1]){
            throw new IllegalArgumentException("El rango de evaluacion debe ser válido [min-max] (max > min)");
        }

        return numeros;
    }

    private static boolean isRelevant(String queryId, String docId, Map<String, Map<String, Integer>> relevanceMap) {
        return relevanceMap.get(queryId).getOrDefault(docId,0) != 0;
    }

    private static double getPrecision(ScoreDoc[] hits, int cut, Map<String, Map<String, Integer>> relevanceMap, IndexSearcher indexSearcher, int queryId) throws IOException {
        int relevantRetrieved = 0;

        for (int i = 0; i < Math.min(cut, hits.length); i++) {

            Document doc = indexSearcher.storedFields().document(hits[i].doc);
            String docId = doc.get("_id");

            if (isRelevant(String.valueOf(queryId), docId, relevanceMap)) {
                relevantRetrieved++;
            }
        }

        return (double) relevantRetrieved / Math.min(cut, hits.length);

    }

    private static double getAP(ScoreDoc[] hits, int cut, Map<String, Map<String, Integer>> relevanceMap, IndexSearcher indexSearcher, int queryId) throws IOException {
        int relevantRetrieved = 0;
        double precisionSum = 0;

        for (int i = 0; i < Math.min(cut, hits.length); i++) {
            Document doc = indexSearcher.storedFields().document(hits[i].doc);
            String docId = doc.get("_id");

            if (isRelevant(String.valueOf(queryId), docId, relevanceMap)) {
                relevantRetrieved++;
                precisionSum += (double) relevantRetrieved / (i + 1);
            }
        }

        if (relevantRetrieved > 0) {
            return precisionSum / relevantRetrieved;
        } else {
            return 0.0;
        }
    }

    private static double getRecall(ScoreDoc[] hits, int cut, Map<String, Map<String, Integer>> relevanceMap, IndexSearcher indexSearcher, int queryId) throws IOException {

        int relevantRetrieved = 0;
        int totalRelevant = 0;

        for (int i = 0; i < Math.min(cut, hits.length); i++) {
            Document doc = indexSearcher.storedFields().document(hits[i].doc);
            String docId = doc.get("_id");

            if (isRelevant(String.valueOf(queryId), docId, relevanceMap)) {
                relevantRetrieved++;
            }
        }

        Map<String, Integer> queryRelevanceMap = relevanceMap.get(String.valueOf(queryId));

        if (queryRelevanceMap != null) {
            for(Integer m : queryRelevanceMap.values()){
                if(m != 0){
                    totalRelevant++;
                }

            }
        }

        if (totalRelevant != 0) {
            return (double) relevantRetrieved / totalRelevant;
        } else {
            return 0.0;
        }

    }

    private static double getRR(ScoreDoc[] hits, int cut, Map<String, Map<String, Integer>> relevanceMap, IndexSearcher indexSearcher, int queryId) throws IOException {

        for (int i = 0; i < Math.min(cut, hits.length); i++) {
            Document doc = indexSearcher.storedFields().document(hits[i].doc);
            String docId = doc.get("_id");

            if (isRelevant(String.valueOf(queryId), docId, relevanceMap)) {
                return 1.0 / (i + 1);
            }
        }

        return 0.0;
    }

    private static Query parseQueryForId(int id, List<JsonNode> queryList) throws ParseException {

        String queryString;
        String queryId;

        for(JsonNode queryNode : queryList){
            queryString = queryNode.get("metadata").get("query").asText();
            queryString = queryString.toLowerCase(); //Para asegurar problemas con requerimientos booleanos
            queryId = queryNode.get("_id").asText();

            if(id == Integer.parseInt(queryId)){
                return new QueryParser("text", new StandardAnalyzer()).parse(queryString);
            }
        }

        return null;

    }

    private static void printAndWriteTxt(List<JsonNode> queryList, int top, ScoreDoc[] hits, int id, String fileName, IndexSearcher indexSearcher, Map<String, Map<String, Integer>> relevanceMap,
                                         double valueP, double valueR, double valueRR, double valueAP) throws ParseException{

        String queryString = "";
        String queryId;
        Query parsedQuery=null;
        //Obtener query original y parseada
        for(JsonNode queryNode : queryList){
            queryString = queryNode.get("metadata").get("query").asText();
            queryString = queryString.toLowerCase(); //Para asegurar problemas con requerimientos booleanos
            queryId = queryNode.get("_id").asText();

            if(id == Integer.parseInt(queryId)){
                QueryParser parser = new QueryParser("text", new StandardAnalyzer());
                parsedQuery = parser.parse(queryString);
                break;
            }
        }

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/searchEval/" + fileName, true));
            System.out.println("---------------------------------------------------------------------------------------------------------------------------");
            writer.write("---------------------------------------------------------------------------------------------------------------------------\n");
            System.out.println("Query original: " + queryString);
            writer.write("Query original: " + queryString + "\n");
            System.out.println("Query parseada: " + parsedQuery.toString());
            writer.write("Query parseada: " + parsedQuery.toString() + "\n");


            if (hits.length > 0) {
                for (int i = 0; i < Math.min(hits.length, top); i++) {

                    String relevancia;
                    Document doc = indexSearcher.storedFields().document(hits[i].doc);

                    if(isRelevant(String.valueOf(id),String.valueOf(doc.get("_id")),relevanceMap )){
                        relevancia = "Sí";
                    }else{
                        relevancia = "No";
                    }



                    System.out.println("\tDocumento: " + i);
                    writer.write("\tDocumento: " + i + "\n");
                    System.out.println("\t\tDocID: " + doc.get("_id"));
                    writer.write("\t\tDocID: " + doc.get("_id") + "\n");
                    System.out.println("\t\tScore: " + hits[i].score);
                    writer.write("\t\tScore: " + hits[i].score + "\n");
                    System.out.println("\t\tCampos del índice:");
                    writer.write("\t\tCampos del índice\n");

                    for(IndexableField field : doc.getFields()){
                        System.out.println("\t\t\t" + field.name() + ": " + field.stringValue());
                        writer.write("\t\t\t" + field.name() + ": " + field.stringValue() + "\n");
                    }
                    System.out.println("\t\tRelevante: " + relevancia);
                    writer.write("\t\tRelevante: " + relevancia + "\n");
                }

                System.out.println("===========================================================================================================================");
                writer.write("===========================================================================================================================\n");
                System.out.println("Métricas para la query " + id + ":");
                writer.write("Métricas para la query " + id + ":\n");
                System.out.println("  P@n : " + valueP);
                writer.write("  P@n : " + valueP + "\n");
                System.out.println("  Recall@n : " + valueR);
                writer.write("  Recall@n : " + valueR + "\n");
                System.out.println("  AP@n : " + valueAP);
                writer.write("  AP@n : " + valueAP + "\n");
                System.out.println("  RR : " + valueRR);
                writer.write("  RR : " + valueRR + "\n");


            }else{
                System.out.println("No hay documentos recuperados para la query");
            }
            writer.close();

        }catch (IOException e){
            e.printStackTrace();
        }



    }

    private static void writeMeanValuesTxt(double avgP, double avgR, double avgRR, double avgAP, String fileName){
        try(BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/searchEval/" + fileName, true))){
            System.out.println(".................................................................................................................");
            writer.write(".................................................................................................................\n");
            System.out.println("Promedio de P@n (MP): " + avgP);
            writer.write("Promedio de P@n (MP): " + avgP + "\n");
            System.out.println("Promedio de Recall@n (MR): " + avgR);
            writer.write("Promedio de Recall@n (MR): " + avgR + "\n");
            System.out.println("Promedio de RR (MRR): " + avgRR);
            writer.write("Promedio de RR (MRR): " + avgRR + "\n");
            System.out.println("Promedio de AP@n (MAP@n): " + avgAP);
            writer.write("Promedio de AP@n (MAP@n): " + avgAP + "\n");


        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static Map<String, String> searchAndEval(IndexSearcher indexSearcher, int minQueryId, int maxQueryId, int cut, int top, List<JsonNode> queriesList , Map<String, Map<String, Integer>> relevanceMap, String fileNameText) throws IOException, ParseException {

        double cP = 0, cR = 0, cAP = 0, cRR = 0;
        double valueP, valueR, valueAP, valueRR, totalP = 0.0, totalR = 0.0, totalAP = 0.0, totalRR = 0.0, avgP, avgR, avgAP, avgRR;
        String newLine;
        Map<String, String> linesCSV = new HashMap<>();

        linesCSV.put("cut"+cut, "cut"+cut + ",P@n,Recall,RR,AP@n");

        // Si -all -> 0-50, si int1 int1-int1, int1-int2
        for (int queryId = minQueryId; queryId <= maxQueryId; queryId++) {
            Query query = parseQueryForId(queryId, queriesList);
            TopDocs topDocs = indexSearcher.search(query, cut);
            ScoreDoc[] hits = topDocs.scoreDocs;

            valueP = getPrecision(hits, cut, relevanceMap, indexSearcher, queryId);
            valueR = getRecall(hits, cut, relevanceMap, indexSearcher, queryId);
            valueRR = getRR(hits, cut, relevanceMap, indexSearcher, queryId);
            valueAP = getAP(hits, cut, relevanceMap, indexSearcher, queryId);

            newLine = queryId + "," + valueP + "," + valueR + "," + valueRR + "," + valueAP;

            // Actualizar el valor en el mapa
            linesCSV.put(String.valueOf(queryId), newLine);

            if (valueP > 0) { totalP += valueP; cP++; }
            if (valueR > 0) { totalR += valueR; cR++; }
            if (valueRR > 0) { totalRR += valueRR; cRR++; }
            if (valueAP > 0) { totalAP += valueAP; cAP++; }

            //imprimir documentos
            printAndWriteTxt(queriesList, top, hits, queryId, fileNameText, indexSearcher, relevanceMap, valueP, valueR, valueRR, valueAP);

        }

        if(cP > 0){ avgP = totalP/cP; }else{ avgP = 0.0; }
        if(cR > 0){ avgR = totalR/cR; }else{ avgR = 0.0; }
        if(cAP > 0){ avgAP = totalAP/cAP;}else{ avgAP = 0.0; }
        if(cRR > 0){ avgRR = totalRR/cRR;}else{ avgRR = 0.0; }

        writeMeanValuesTxt(avgP, avgR, avgRR, avgAP, fileNameText);

        //guardar metricas promediadas
        linesCSV.put("promedios","promedios," + avgP + "," + avgR + "," + avgRR + "," + avgAP);

        return linesCSV;

    }

    private static void writeCSV(Map<String, String> linesCSV , int min1, int max1, String fileName, int cut) throws IOException {

       try(BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/searchEval/" + fileName))){
           //Primera linea
           writer.write(linesCSV.get("cut"+cut) + "\r\n");
           //Cuerpo
           for(int i = min1; i <= max1; i++){
               writer.write(linesCSV.get(String.valueOf(i)) + "\r\n" );
           }
           //Ultima linea
           writer.write(linesCSV.get("promedios") + "\r\n");
       }catch (IOException e){
           e.printStackTrace();
       }


    }

    public static void main( String[] args ) throws IOException, ParseException {

        String usage = "-search [jm LAMBDA | bm25 K1] -index INDEXPATH -cut N -top M -queries [all | int1 | int1-int2] ";
        String searchModel = null, indexPath = null, queries = null;
        float searchValue = 0;
        int cut = -1, top = -1;
        int [] range;
        int minQueryId, maxQueryId;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-search":
                    searchModel = args[++i];
                    searchValue = Float.parseFloat(args[++i]);
                    validateSearchModel(searchModel, searchValue);
                    break;
                case "-index":
                    indexPath = args[++i];
                    validateIndexPath(indexPath);
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    validateCut(cut);
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    validateTop(top);
                    break;
                case "-queries":
                    queries = args[++i];
                    break;

                default:
                    throw new IllegalArgumentException("Parámetro desconocido " + args[i]);
            }
        }

        if (searchModel == null ||indexPath == null || cut == -1 || top == -1 || queries == null) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }
        range = validateRange(queries);
        minQueryId = range[0];
        maxQueryId = range[1];
        Map<String, String> linesCSV;

        IndexReader indexReader = null;

        try(Directory dir = FSDirectory.open(Paths.get(indexPath))){
            indexReader = DirectoryReader.open(dir);
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            indexSearcher.setSimilarity(
                    searchModel.equals("jm") ? new LMJelinekMercerSimilarity(searchValue) : new BM25Similarity(searchValue, 0.75f)
            );

            List<JsonNode> queriesList = readQueriesFile();
            Map<String,Map<String , Integer>> judgments = readRelevanceFile();

            String fileNameTxt = searchModel.equals("jm") ?
                    "TREC-COVID." + searchModel + "." + top + ".hits.lambda." + searchValue + ".q" + queries + ".txt" :
                    "TREC-COVID." + searchModel + "." + top + ".hits.k1." + searchValue + ".q" + queries + ".txt";


            linesCSV = searchAndEval(indexSearcher, minQueryId,maxQueryId,cut,top,queriesList, judgments, fileNameTxt);

            String fileNameCSV = searchModel.equals("jm") ?
                    "TREC-COVID." + searchModel + "." + cut + ".cut" + ".lambda." + searchValue + ".q" + queries + ".csv" :
                    "TREC-COVID." + searchModel + "." + cut + ".cut" + ".k1." + searchValue + ".q" + queries + ".csv";

            writeCSV(linesCSV, minQueryId, maxQueryId,fileNameCSV , cut);


        }catch(IOException e){
            e.printStackTrace();
        }

        if(indexReader != null)
            indexReader.close();
    }

}
