package es.udc.fic.ri;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.queryparser.classic.QueryParser;


import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrainingTestTrecCovid {

    /*
    @JsonIgnoreProperties(ignoreUnknown = true)

    public record TrecQueryRecord(

            int _id,
            TrainingTestTrecCovid.TrecQueryRecord.Metadata metadata
    ){
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Metadata(String query){}
    }

     */


    private static void validateIndexPath(String indexPath){
        Path path = Paths.get(indexPath);

        if(!Files.exists(path)){
            throw new IllegalArgumentException("El índice indicado en los parámetros no existe");
        }

    }

    private static void validateMetrica(String metrica) {
        if(!metrica.equals("P") && !metrica.equals("R") && !metrica.equals("MRR") && !metrica.equals("MAP")){
            throw new IllegalArgumentException("La métrica debe ser P | R | MRR | MAP");
        }
    }

    private static void validateCut(int cut){
        if(cut <= 0){
            throw new IllegalArgumentException("El corte en el ranking debe ser mayor que 0");
        }
    }

    private static int [] validateRange(String range){
        String[] partes = range.split("-");
        int[] numeros = new int[2];

        numeros[0] = Integer.parseInt(partes[0]);
        numeros[1] = Integer.parseInt(partes[1]);
        if(numeros[0] < 1 || numeros[1] >50){
            throw new IllegalArgumentException("El rango de queries no está dentro del rango de queries válidas [1-50]");
        }

        if(numeros[0] > numeros[1]){
            throw new IllegalArgumentException("El rango de evaluacion debe ser válido [min-max] (max > min)");
        }

        return numeros;
    }

    private static Map<String, Map<String, Integer>> readRelevanceFile() throws IOException {
        InputStream judgmentsInputStream = IndexTrecCovid.class.getResourceAsStream("/trec-covid/qrels/test.tsv");
        Map<String, Map<String, Integer>> judgments = new HashMap<>();
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
        return judgments;
    }

    private static boolean isRelevant(String queryId, String docId, Map<String, Map<String, Integer>> relevanceMap) {
        return relevanceMap.get(queryId).getOrDefault(docId,0) != 0;
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


    private static double getMeanMetricValue(int cut, Map<String, Map<String, Integer>> relevanceMap, IndexSearcher indexSearcher, int minQueryId, int maxQueryId, String metrica, Map<String, String> linesCSV, List<JsonNode> queriesList) throws IOException, ParseException {
        double sum = 0;
        int countQueries = 0;
        double metricValue;


        // Itera sobre el rango de IDs de consulta especificado
        for (int queryId = minQueryId; queryId <= maxQueryId; queryId++) {
            Query query = parseQueryForId(queryId,queriesList); // Obtiene la consulta correspondiente al ID
            TopDocs topDocs = indexSearcher.search(query, cut); // Realiza la búsqueda de la consulta
            ScoreDoc[] hits = topDocs.scoreDocs; // Obtiene los documentos recuperados

            if(metrica.equals("P")){
                metricValue = getPrecision(hits, cut, relevanceMap, indexSearcher, queryId);
            }else if(metrica.equals("R")){
                metricValue = getRecall(hits, cut, relevanceMap, indexSearcher, queryId);
            }else if(metrica.equals("MRR")){
                metricValue = getRR(hits, cut, relevanceMap, indexSearcher, queryId);
            }else{ //MAP
                metricValue = getAP(hits, cut, relevanceMap, indexSearcher, queryId);
            }


            // Verificar si ya existe una entrada para la queryId en el mapa
            String existingValue = linesCSV.getOrDefault(String.valueOf(queryId), "");

            // Concatenar el nuevo valor al valor existente, separado por ","
            String newValue;
            if (existingValue.isEmpty()) {
                newValue = queryId + "," + metricValue;
            } else {
                newValue = existingValue + "," + metricValue;
            }

            // Actualizar el valor en el mapa
            linesCSV.put(String.valueOf(queryId), newValue);


            if (metricValue > 0) {
                sum += metricValue;
                countQueries++;
            }
        }

        if (countQueries > 0) {
            return sum / countQueries;
        } else {
            return 0.0;
        }
    }

    private static String getMetricaFile(String metrica){
        if(metrica.equals("P")){
            return "Prec";
        }else if(metrica.equals("R")){
            return "Recall";
        }else if(metrica.equals("MRR")){
            return "RecRank";
        }else{ // MAP
            return "AvgPrec";
        }
    }

    private static String getPromedioFile(String metrica){
        if(metrica.equals("P")){
            return "MP";
        }else if(metrica.equals("R")){
            return "MR";
        }else if(metrica.equals("MRR")){
            return "MRR";
        }else{ // MAP
            return "MAP";
        }
    }

    private static void writeCSV(BufferedWriter writer,Map<String, String> linesCSV , int min1, int max1, String promedioFile) throws IOException {
        String line;

        for(int i = min1; i <= max1; i++){
            line = linesCSV.get(String.valueOf(i));
            writer.write(line + "\r\n");
        }
        String lastLine = linesCSV.get(promedioFile);
        writer.write(lastLine);

    }


    private static void printResultsTraining(String fileName, String trainingOrTest){

        int lengthLine;

        if(trainingOrTest.equals("TRAINING")){
            lengthLine = 150;
        }else{
            lengthLine = 30;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/trainingTest/" + fileName))) {

            System.out.println("RESULTADOS DE " + trainingOrTest + " EN: " + fileName);
            String line = reader.readLine();
            System.out.println(line.replaceAll(",","\t\t\t"));
            for (int i = 0; i < lengthLine; i++) {
                System.out.print("=");
            }
            System.out.println();
            while ((line = reader.readLine()) != null) {
                System.out.println(line.replaceAll(",","\t\t\t"));
                for (int i = 0; i < lengthLine; i++) {
                    System.out.print("-");
                }
                System.out.println();
            }
            System.out.println("\n\n\n");



        } catch (IOException e) {
            System.err.println("Error al leer el archivo de training: " + e.getMessage());
        }


    }


    private static void testEval(float similarityValue, int min2, int max2, String metrica, int cut, Map<String, Map<String, Integer>> relevanceMap , IndexSearcher indexSearcher, String fileName, boolean isJM, List<JsonNode> queriesList) throws IOException {


        String metricaFile = getMetricaFile(metrica);
        String promedioFile = getPromedioFile(metrica);
        double meanMetricValue;
        Map<String, String> linesCSV = new HashMap<>();
        if(isJM){
            LMJelinekMercerSimilarity lmJelinekMercerSimilarity = new LMJelinekMercerSimilarity(similarityValue);
            indexSearcher.setSimilarity(lmJelinekMercerSimilarity);
        }else{
            BM25Similarity bm25Similarity = new BM25Similarity(similarityValue,0.75f);
            indexSearcher.setSimilarity(bm25Similarity);
        }


        linesCSV.put(promedioFile, promedioFile);

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/trainingTest/" + fileName));

            writer.write(similarityValue + "," + metricaFile + "\r\n");

            meanMetricValue = getMeanMetricValue(cut, relevanceMap, indexSearcher, min2, max2, metrica, linesCSV,queriesList );

            String promediosLine = linesCSV.get(promedioFile) +  "," + meanMetricValue;
            linesCSV.put(promedioFile, promediosLine);


            writeCSV(writer, linesCSV, min2, max2, promedioFile);
            writer.close();
            printResultsTraining(fileName, "TEST");
        }catch (IOException e){
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }


    }


    private static void evalJM(IndexSearcher indexSearcher, String metrica, int cut, int min1, int max1, int min2, int max2, Map<String, Map<String, Integer>> relevanceMap, List<JsonNode> queriesList) throws IOException {


        double meanMetricValue;
        double maxMetricValue = 0;
        float bestLambda = 0.001f;
        String fileName = "TREC-COVID.jm.training." + min1 + "-" + max1 + ".test." + min2 + "-" + max2 + "." + metrica.toLowerCase() + cut + ".training.csv";
        String testFileName = "TREC-COVID.jm.training." + min1 + "-" + max1 + ".test." + min2 + "-" + max2 + "." + metrica.toLowerCase() + cut + ".test.csv";
        String metricaFile = getMetricaFile(metrica);
        String promedioFile = getPromedioFile(metrica);
        Map<String, String> linesCSV = new HashMap<>();
        float tolerance = 0.0001f;

        linesCSV.put(promedioFile, promedioFile);

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/trainingTest/" + fileName));

            writer.write(metricaFile + "_" + cut + ",0.001,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0\r\n");

            //Primera iteracion con 0.001 y luego 0.1, 0.2, 0.3, ...
            for (float lambda = 0.001f; lambda <= 1.0f + tolerance; lambda += (lambda == 0.001f ? 0.099f : 0.1f)) {

                if(lambda > 1.0f){
                    LMJelinekMercerSimilarity lmJelinekMercerSimilarity = new LMJelinekMercerSimilarity(1.0f);
                    indexSearcher.setSimilarity(lmJelinekMercerSimilarity);
                }else{
                    LMJelinekMercerSimilarity lmJelinekMercerSimilarity = new LMJelinekMercerSimilarity(lambda);
                    indexSearcher.setSimilarity(lmJelinekMercerSimilarity);
                }

                meanMetricValue = getMeanMetricValue(cut, relevanceMap,indexSearcher, min1, max1, metrica, linesCSV, queriesList);

                if(meanMetricValue > maxMetricValue){
                    maxMetricValue = meanMetricValue;
                    bestLambda = lambda;

                }

                String promediosLine = linesCSV.get(promedioFile) +  "," + meanMetricValue;
                linesCSV.put(promedioFile, promediosLine);

            }
            writeCSV(writer, linesCSV, min1, max1, promedioFile);
            writer.close();
            printResultsTraining(fileName, "TRAINING");
            testEval(bestLambda, min2, max2, metrica, cut , relevanceMap, indexSearcher, testFileName, true, queriesList);

        }catch (IOException e){
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }




    private static void evalBM25(IndexSearcher indexSearcher, String metrica, int cut, int min1, int max1, int min2, int max2, Map<String, Map<String, Integer>> relevanceMap, List<JsonNode> queriesList) throws IOException{

        double meanMetricValue;
        double maxMetricValue = 0;
        float bestK1 = 0.4f;
        String fileName = "TREC-COVID.bm25.training." + min1 + "-" + max1 + ".test." + min2 + "-" + max2 + "." + metrica.toLowerCase() + cut + ".training.csv";
        String testFileName = "TREC-COVID.bm25.training." + min1 + "-" + max1 + ".test." + min2 + "-" + max2 + "." + metrica.toLowerCase() + cut + ".test.csv";
        String metricaFile = getMetricaFile(metrica);
        String promedioFile = getPromedioFile(metrica);
        Map<String, String> linesCSV = new HashMap<>();
        float tolerance = 0.0001f;

        linesCSV.put(promedioFile, promedioFile);

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter( "src/main/resources/trainingTest/" + fileName));

            writer.write(metricaFile + "_" + cut + ",0.4,0.6,0.8,1.0,1.2,1.4,1.6,1.8,2.0\r\n");

            //Primera iteracion con 0.4 y luego 0.6, 0.8, 1.0, ...
            for (float k1 = 0.4f; k1 <= 2.0f + tolerance; k1 += 0.2f) {

                if(k1 > 2.0f){
                    BM25Similarity bm25Similarity = new BM25Similarity(2.0f, 0.75f);
                    indexSearcher.setSimilarity(bm25Similarity);
                }else {
                    BM25Similarity bm25Similarity = new BM25Similarity(k1, 0.75f);
                    indexSearcher.setSimilarity(bm25Similarity);
                }


                meanMetricValue = getMeanMetricValue(cut, relevanceMap,indexSearcher, min1, max1, metrica, linesCSV, queriesList);

                if(meanMetricValue > maxMetricValue){
                    maxMetricValue = meanMetricValue;
                    bestK1 = k1;

                }

                String promediosLine = linesCSV.get(promedioFile) +  "," + meanMetricValue;
                linesCSV.put(promedioFile, promediosLine);

            }
            writeCSV(writer, linesCSV, min1, max1, promedioFile);
            writer.close();
            printResultsTraining(fileName, "TRAINING");
            testEval(bestK1, min2, max2, metrica, cut , relevanceMap, indexSearcher, testFileName, false, queriesList);

        }catch (IOException | ParseException e){
            e.printStackTrace();
        }
    }


    public static void main( String[] args ) throws IOException {

        String usage= "-index INDEX_PATH -metrica [P | R | MRR | MAP] -cut N ( -evaljm INT1-INT2 INT3-INT4 | -evalbm25 INT1-INT2 INT3-INT4 )";
        String eval = null, rangeStr1 = null, rangeStr2 = null, indexPath = null, metrica = null;
        int cut = -1;
        int [] range1;
        int [] range2;
        int min1, max1, min2, max2;


        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    validateIndexPath(indexPath);
                    break;
                case "-metrica":
                    metrica = args[++i];
                    validateMetrica(metrica);
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    validateCut(cut);
                    break;
                case "-evaljm":
                    if(eval != null && eval.equals("evalbm25"))
                        throw new IllegalArgumentException("Las opciones -evaljm y -evalbm25 son mutuamente exclusivas");
                    eval = "evaljm";
                    rangeStr1 = args[++i];
                    rangeStr2 = args[++i];
                    break;
                case "-evalbm25":
                    if(eval != null && eval.equals("evaljm"))
                        throw new IllegalArgumentException("Las opciones -evaljm y -evalbm25 son mutuamente exclusivas");
                    eval = "evalbm25";
                    rangeStr1 = args[++i];
                    rangeStr2 = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Parámetro desconocido " + args[i]);
            }
        }

        if(indexPath == null || cut == -1 || eval == null || rangeStr1 == null || rangeStr2 == null || metrica == null){
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        range1 = validateRange(rangeStr1);
        min1 = range1[0];
        max1 = range1[1];
        range2 = validateRange(rangeStr2);
        min2 = range2[0];
        max2 = range2[1];

        List<JsonNode> queriesList = readQueriesFile();
        IndexReader indexReader = null;
        Map<String,Map<String , Integer>> relevanceMap = readRelevanceFile();

        try(Directory directory = FSDirectory.open(Paths.get(indexPath))){
            indexReader = DirectoryReader.open(directory);
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);

            if(eval.equals("evaljm")){
                System.out.println("Evaluando con Jelinek-Mercer\n\n");
                evalJM(indexSearcher, metrica, cut, min1, max1, min2, max2, relevanceMap, queriesList);
            }else{
                System.out.println("Evaluando con BM25\n\n");
                evalBM25(indexSearcher, metrica, cut, min1, max1, min2, max2, relevanceMap, queriesList);
            }

        }catch (IOException e){
            e.printStackTrace();
        }

        if(indexReader != null)
            indexReader.close();


    }
}
