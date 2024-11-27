package es.udc.fic.ri;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

public class Compare {

    private static double[] linesToDoubleArray(List<String> lines) { // Método para convertir la columna del CSV a un array de doubles
        double[] array = new double[lines.size() - 2]; //Todas las líneas menos la primera y la ultima
        for (int i = 1; i < lines.size() - 1; i++) {
            array[i - 1] = Double.parseDouble(lines.get(i).split(",")[1]);
        }
        return array;
    }

    public static void validateAlpha(double alpha) {
        if (!(alpha >= 0) && (alpha <= 1))
            throw new IllegalArgumentException("El valor de alpha debe estar entre 0 y 1");
    }
    public static void main(String[] args) throws IOException {

        String usage = "-test t | wilcoxon ALPHA -results RESULTS1.CSV RESULTS2.CSV";
        String model = null;
        float alpha = -1;
        String results1 = null;
        String results2 = null;
        TTest tTest= new TTest();
        WilcoxonSignedRankTest wilcoxonSignedRankTest= new WilcoxonSignedRankTest();


        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-test":
                    model = args[++i];
                    alpha = Float.parseFloat(args[++i]);
                    validateAlpha(alpha);
                    break;
                case "-results":
                    results1 = args[++i];
                    results2 = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Parámetro desconocido " + args[i]);
            }
        }

        if (model == null|| alpha == -1 || results1 == null || results2 == null) {
            System.out.println("Usage: " + usage);
            System.exit(1);
        }

        // Leer la metrica de los CSV
        List<String> lines1 = Files.readAllLines(Paths.get("src/main/resources/trainingTest/" + results1));
        List<String> lines2 = Files.readAllLines(Paths.get( "src/main/resources/trainingTest/" + results2));
        double[] metricaCSV1 = linesToDoubleArray(lines1);
        double[] metricaCSV2 = linesToDoubleArray(lines2);

        double pValue;
        if (model.equals("t"))
            pValue = tTest.tTest(metricaCSV1, metricaCSV2);
        else { //-wilcoxon
            pValue = wilcoxonSignedRankTest.wilcoxonSignedRankTest(metricaCSV1, metricaCSV2,true);
        }

        System.out.println("Resultado del test de significancia estadística:");
        System.out.println("p-valor: " + pValue);
        if (pValue < alpha) {
            System.out.println("Se rechaza la hipótesis nula. Hay diferencia significativa entre los modelos.");
        } else {
            System.out.println("No se puede rechazar la hipótesis nula. No hay diferencia significativa entre los modelos.");
        }
    }

}