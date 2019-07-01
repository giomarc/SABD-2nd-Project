package erreesse;

public class StartSimulation {

    public static void main(String[] args) {

        final String csvFilePath = args[0];
        final float speedFactor = Float.parseFloat(args[1]);
        DataSourceSimulator dss = new DataSourceSimulator(csvFilePath,speedFactor);
        dss.startSimulation();
    }
}
