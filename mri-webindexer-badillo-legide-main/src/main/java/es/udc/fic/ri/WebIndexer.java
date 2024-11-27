package es.udc.fic.ri;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;



public class WebIndexer {

    public static class WorkerThread implements Runnable {

        private final Path file;
        private final String docsPath;
        private final boolean h;
        private final boolean titleTermVectors;
        private final boolean bodyTermVectors;
        private final String onlyDoms;
        private final String timeout;
        private final String maxRedirects;
        private int numRedirects = 0;
        private final IndexWriter indexWriter;
        private void setNumRedirects(int i){
            this.numRedirects = i;
        }



        public WorkerThread(final Path file, final String docsPath, final boolean h, final boolean titleTermVectors, final boolean bodyTermVectors, final Properties properties, final IndexWriter indexWriter) {

            this.file = file;
            this.docsPath = docsPath;
            this.h = h;
            this.titleTermVectors = titleTermVectors;
            this.bodyTermVectors = bodyTermVectors;
            this.onlyDoms =  properties.getProperty("onlyDoms", "allDomains");
            this.timeout = properties.getProperty("timeout", "10");
            this.maxRedirects = properties.getProperty("maxRedirects","5");
            this.indexWriter= indexWriter;
        }

        private void processURL(String url){



            if(numRedirects > Integer.parseInt(maxRedirects)){
                System.out.println("\nSuperado el máximo de redirecciones para: " + url + "\n");
                return;
            }
            try {
                //Configuración del cliente HTTP
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(Long.parseLong(timeout))) //esto es el timeout de conexion
                        .build();

                //Realizar la solicitud HTTP y obtener la respuesta
                try{
                    //Envia la solicitud http y la guarda como String
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString()); //el timeout de respuesta es el heredado por el cliente

                    //Verifica que el codigo de respuesta sea 200
                    if(response.statusCode() == 200) {
                        //Extraer el nombre del archivo de la URL
                        //replaceAll se usa para quitar caracteres no validos para nombres de archivo
                        //finalmente se agrega la extension .loc
                        String fileName = url.replaceAll("^(https?://)", "").replaceAll("/", "_") + ".loc";

                        //Creo un objeto Path con nombre "fileName" y se guarda en docsPath
                        Path localFilePath = Paths.get(docsPath, fileName);
                        //Utilizo Files.write para escritura thread-safe
                        //response.body().getBytes() obtiene el cuerpo de la respuesta http como un arreglo de bytes
                        //StandardOpenOption especifica que se debe crear el archivo si no existe
                        Files.write(localFilePath, response.body().getBytes(), StandardOpenOption.CREATE);


                        //Parte de Jsoup para guardar los archivos que serán indexados

                        org.jsoup.nodes.Document document = Jsoup.parse(response.body());
                        String title = document.title();
                        String body = document.body().text();

                        //Esta vez guardo como antes acabado en .loc y concateno al final .notags
                        Path noTagsFile = Paths.get(docsPath, fileName + ".notags");

                        //(title + "\n" + body) para hacer que la primera linea sea el título y despues el resto del cuerpo
                        Files.write(noTagsFile, (title + "\n" + body).getBytes(), StandardOpenOption.CREATE);

                        //Indexacion
                        if (Files.exists(noTagsFile)) {
                            index(indexWriter, noTagsFile, localFilePath);
                        }


                    }else if (response.statusCode() >= 300 && response.statusCode() < 400){
                        //Guardo la redireccion que va en el campo Location de la respuesta. Si no hay guardo el valor null
                        String redirectUrl = response.headers().firstValue("Location").orElse(null);



                        if(redirectUrl != null){

                            if(!validateOnlyDoms(redirectUrl)){ //ignoro las urls que no terminen con la url requerida
                                System.out.println("\nRedireccion con dominio no permitido: " + redirectUrl + "\n");
                                return;
                            }

                            //Proceso la nueva url
                            setNumRedirects(numRedirects+1);
                            processURL(redirectUrl);
                        }else{
                            System.out.println("\nRedirección recibida sin campo Location\n");
                        }


                    }else{
                        System.out.println("Error (status code " + response.statusCode() + ") al descargar la página: " + url);
                    }

                }catch (Exception e){
                    System.out.println("Hubo un error al realizar la solicitud HTTP: " + e.getMessage());
                }
            }catch (UnsupportedOperationException | IllegalArgumentException e){
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
        }




        private void index(IndexWriter writer, Path file, Path loc)  throws IOException { //metodo para guardar lo requerido en el indice

            try (InputStream stream = Files.newInputStream(loc)) {
                org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

                //Añadir el path del archivo sin tokenizar y se guarda
                doc.add(new KeywordField("path", loc.toString(), Field.Store.YES));

                // Agregar el contenido del archivo a un campo llamado "contents". Especificar un Reader,
                // para que el texto del archivo sea tokenizado e indexado, pero no almacenado.
                // Ten en cuenta que FileReader espera que el archivo esté codificado en UTF-8.
                // Si no es el caso, la búsqueda de caracteres especiales podría fallar.
                doc.add(
                        new TextField(
                                "contents",
                                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

                //hostname
                doc.add(new KeywordField("hostname", InetAddress.getLocalHost().getHostName() , Field.Store.YES));
                //thread que lo hizo
                doc.add(new KeywordField("thread", Thread.currentThread().getName() , Field.Store.YES));

                long locKb = Files.size(loc) /1024; //Tamaño en kilobytes del archivo .loc
                long notagsKb = Files.size(file) /1024; //Tamaño en kilobytes del archivo .loc.notags

                doc.add(new LongField("locKb", locKb, Field.Store.YES));
                doc.add(new LongField("notagsKb", notagsKb, Field.Store.YES));

                //fecha de creacion, ultimo acceso y ultima fecha de modificacion

                BasicFileAttributes attributes = Files.readAttributes(loc, BasicFileAttributes.class);

                //Convertir FileTime a Date
                //Se hace toMillis() para que sea mas precisa la fecha y luego convertir a Date
                Date creationTimeDate = new Date(attributes.creationTime().toMillis());
                Date lastAccessTimeDate = new Date(attributes.lastAccessTime().toMillis());
                Date lastModifiedTimeDate = new Date(attributes.lastModifiedTime().toMillis());

                //fecha de creacion, ultimo acceso y ultima fecha de modificacion sin formato Lucene
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmXXX"); //año,mes,dia,hora,minuto,zona horaria
                String creationTimeStr = dateFormat.format(creationTimeDate);
                String lastAccessTimeStr = dateFormat.format(lastAccessTimeDate);
                String lastModifiedTimeStr = dateFormat.format(lastModifiedTimeDate);

                doc.add(new StringField("creationTime", creationTimeStr, Field.Store.YES));
                doc.add(new StringField("lastAccessTime", lastAccessTimeStr, Field.Store.YES));
                doc.add(new StringField("lastModifiedTime", lastModifiedTimeStr, Field.Store.YES));


                //fecha de creacion, ultimo acceso y ultima fecha de modificacion en formato Lucene
                String creationTimeLucene = DateTools.dateToString(creationTimeDate, DateTools.Resolution.MILLISECOND);
                String lastAccessTimeLucene = DateTools.dateToString(lastAccessTimeDate, DateTools.Resolution.MILLISECOND);
                String lastModifiedTimeLucene = DateTools.dateToString(lastModifiedTimeDate, DateTools.Resolution.MILLISECOND);

                doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));


                //title de .loc.notags
                String content = Files.readString(file); //leer contenido del .loc.notags
                String[] lines = content.split("\\r?\\n", 2);

                String title = lines[0];
                String body = lines.length > 1 ? lines[1] : ""; //si tiene mas de una linea guardo el body o sino string vacio


                //opcion para guardar termVectors para el titulo
                if(titleTermVectors){

                    FieldType titleFieldType = new FieldType();
                    titleFieldType.setStored(true);
                    titleFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS); // Configuración de opciones de indexación
                    titleFieldType.setStoreTermVectors(true);
                    titleFieldType.setStoreTermVectorPositions(true); //posicion
                    titleFieldType.setStoreTermVectorOffsets(true); //offset

                    Field titleField = new Field("title", title, titleFieldType);

                    doc.add(titleField);
                }else{
                    doc.add(new TextField("title", title, Field.Store.YES));
                }

                if(bodyTermVectors){

                    FieldType bodyFieldType = new FieldType();
                    bodyFieldType.setStored(true);
                    bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS); // Configuración de opciones de indexación
                    bodyFieldType.setStoreTermVectors(true);
                    bodyFieldType.setStoreTermVectorPositions(true);
                    bodyFieldType.setStoreTermVectorOffsets(true);

                    Field bodyField = new Field("body", body, bodyFieldType);

                    doc.add(bodyField);
                }else{
                    doc.add(new TextField("body", body, Field.Store.YES));
                }




                //Crear o actualizar
                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    //en este caso el indice es nuevo asi que lo creo añadiendo simplemente
                    System.out.println("Añadiendo " + file);
                    writer.addDocument(doc);
                } else {
                    // Índice existente (puede haber una copia antigua de este documento ya indexada) por lo que
                    // utilizamos updateDocument en lugar de añadirlo para reemplazar la antigua si coincide
                    // con la ruta exacta, si está presente:
                    System.out.println("Actualizando " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            }
        }

        private boolean validateOnlyDoms(String url) {
            if (onlyDoms.equals("allDomains") || onlyDoms.isEmpty()) {
                return true; // Si no hay restricciones, todas las URLs son válidas
            }

            String[] dominiosPermitidos = onlyDoms.split("\\s+"); // Guarda cada uno de los posibles valores de onlyDoms

            // Encuentra el TLD de la URL
            String tld = obtainTLD(url);

            // Verifica si el TLD está en la lista de dominios permitidos
            for (String dominio : dominiosPermitidos) {
                if (tld.equals(dominio.trim())) {
                    return true;
                }
            }

            return false;
        }

        private String obtainTLD(String url) {
            // Elimina el protocolo (http:// o https://) de la URL
            String urlSinProtocolo = url.replaceAll("https?://", "");

            // Separa la URL por barras
            String[] partesUrl = urlSinProtocolo.split("/");

            // Separa el último segmento por puntos
            String[] partesTLD = partesUrl[0].split("\\.");

            // Devuelve el último segmento del TLD
            String tld = partesTLD[partesTLD.length - 1];

            return ".".concat(tld);
        }

        @Override
        public void run() { //Cada thread hace esto

            try(BufferedReader reader = Files.newBufferedReader(file)){ //BufferedReader para leer archivo. Los guarda antes de procesar y es efectivo para leer desde disco como en este caso
                String eachUrl;
                while( (eachUrl = reader.readLine()) != null){ //Bucle que lee cada linea hasta que el archivo esté vacío

                    if(!validateOnlyDoms(eachUrl)){ //ignoro las urls que no terminen con la url requerida
                        continue;
                    }

                    if(h){
                        System.out.println("Hilo " + Thread.currentThread().getName() + " comienzo url " + eachUrl);
                        processURL(eachUrl);
                        System.out.println("Hilo " + Thread.currentThread().getName() + " fin url " + eachUrl + "\n");

                    }else{
                        processURL(eachUrl);

                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }

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
                throw new IllegalArgumentException("El path de los documentos .loc y .loc.notags debe ser válido");
            }
        }
    }

    private static void validateNumThreads(int numThreads, int numCores){
        if(numThreads < 0 || numThreads > numCores)
            throw new IllegalArgumentException("El número de threads debe ser 0 para usar el número de núcleos de la máquina o positvo y como máximo " + numCores);
    }

    private static Properties loadConfiguration(){
        Properties properties = new Properties();
        String pathProperties = "src/test/resources/config.properties"; //ubicacion de properties.config

        try(FileInputStream input = new FileInputStream(pathProperties)){
            properties.load(input);
        }catch (IOException e){
            e.printStackTrace();
        }
        return properties;
    }
    private static Analyzer getAnalyzer(String analyzerChosen){ //metodo para asignar analyzer
        if(analyzerChosen == null){ //si no se utilizó la opcion -analyzer
            return new StandardAnalyzer();
        }else{
            switch (analyzerChosen.toLowerCase()){ //paso a minusculas el analyzer para simplificar
                case "standardanalyzer":
                    return new StandardAnalyzer();
                case "englishanalyzer":
                    return new EnglishAnalyzer();
                case "spanishanalyzer":
                    return new SpanishAnalyzer();
                default:
                    return new StandardAnalyzer(); //hecho para que si se especifica un analyzer no reconocido use StandardAnalyzer
            }
        }
    }
    public static void main( String[] args ) throws IOException {


        String usage = " -index INDEX_PATH -docs DOCS_PATH [-create] [-numThreads NUM_THREADS] "
                        + "[-h] [-p] [-titleTermVectors] [-bodyTermVectors] "
                        + "[-analyzer Analyzer]";
        String urls = "src/test/resources/urls"; //path en el que se encuentran los archivos .url con las urls a descargar, parsear e indexar
        String indexPath = null;
        String docsPath = null;
        boolean create = false;
        boolean p = false;
        boolean h = false;
        boolean titleTermVectors = false;
        boolean bodyTermVectors = false;
        int numThreads = 0;
        final ExecutorService executor;
        String analyzerChosen = null; //Analyzer (por defecto es StandardAnalyzer)
        //Lo guardo como string para despues ver cual es e inicializarlo
        Analyzer analyzer = null;
        IndexWriter indexWriter = null;
        final int numCores = Runtime.getRuntime().availableProcessors();  //número de núcleos de mi ordenador


        //Bucle para guardar cada argumento indicado
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i]; //Cojo el argumento que va despues de -index -> INDEX_PATH
                    validateIndexPath(indexPath);
                    break;
                case "-docs":
                    docsPath = args[++i];
                    validateDocsPath(docsPath);
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    validateNumThreads(numThreads, numCores);
                    break;
                case "-h":
                    h = true;
                    break;
                case "-p":
                    p = true;
                    break;
                case "-titleTermVectors":
                    titleTermVectors = true;
                    break;
                case "-bodyTermVectors":
                    bodyTermVectors = true;
                    break;
                case "-analyzer":
                    analyzerChosen = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Parámetro desconocido " + args[i]);
            }
        }

        //Si no le paso argumentos, indico al usuario como hacerlo correctamente
        if (indexPath == null || docsPath == null) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }


        //Creo el pool de threads con el numero por defecto o con el indicado con -numThreads
        if(numThreads == 0){
            executor = Executors.newFixedThreadPool(numCores);
        }else{
            executor = Executors.newFixedThreadPool(numThreads);
        }

        //propiedades de config.properties
        Properties properties = loadConfiguration();

        //Comienzo del trabajo de la aplicacion
        Date start = new Date();

        //Creación del writer
        try{
            //pillar el analyzer
            analyzer=getAnalyzer(analyzerChosen);
            Directory dir = FSDirectory.open(Paths.get(indexPath)); //lugar en el que se va a crear o sobreescribir el indice
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer); //configuracion del writer
            if(create){
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            }else{
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            }
            indexWriter = new IndexWriter(dir,iwc);

        }catch (IOException e){
            e.printStackTrace();
        }



        //Creo un directory stream para mirar cada contenido dentro de un directorio
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(urls))) {

            //Coge cada objeto Path del directorio
            for (final Path path : directoryStream) {

                //Verifico que el documento es legible y que no es un directorio
                if(!Files.isReadable(path)){
                    System.out.println("El archivo o directorio no es legible");
                }else if (Files.isRegularFile(path)) {
                    //final Runnable worker = new WorkerThread(path, docsPath, create, h, titleTermVectors, bodyTermVectors, analyzerChosen, indexPath, properties);
                    final Runnable worker = new WorkerThread(path, docsPath, h, titleTermVectors, bodyTermVectors, properties, indexWriter);

                    //Creo el hilo y lo mando al pool para que se ejecute eventualmente
                    executor.execute(worker);
                }else{ //Recorrer subdirectorios recursivamente
                    try {
                        String finalDocsPath = docsPath;
                        boolean finalH = h;
                        boolean finalTitleTermVectors = titleTermVectors;
                        boolean finalBodyTermVectors = bodyTermVectors;
                        IndexWriter finalIndexWriter = indexWriter;
                        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (!Files.isDirectory(file) && Files.isRegularFile(file)) {
                                    final Runnable worker = new WorkerThread(file, finalDocsPath, finalH, finalTitleTermVectors, finalBodyTermVectors, properties, finalIndexWriter);
                                    executor.execute(worker);
                                }
                                return FileVisitResult.CONTINUE; //Continua la busqueda
                            }

                            @Override
                            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException { //No se puede leer el archivo
                                System.out.println("No es posible leer el archivo" + file);
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        //Cierro el pool para no aceptar mas hilos y los que quedan se ejecutaran eventualmente
        executor.shutdown();

        //Espero hasta 1 hora para que se terminen los hilos que estan currando
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }

        //LLAMAR A QUE SE CIERRE EL WRITER
        if(indexWriter != null)
            indexWriter.close(); //esto mete lo de IOException en el main

        //Fin del trabajo de la aplicación (ya han hecho todos los hilos su trabajo)
        Date end = new Date();

        if(p){ //opcion para informar de cuando se ha tardado en crear el índice
            System.out.println("\nCreado índice " + indexPath + " en " + (end.getTime()-start.getTime()) + " msecs" );
        }else{
            System.out.println("\n¡¡¡Índice creado!!!");
        }

    }
}
