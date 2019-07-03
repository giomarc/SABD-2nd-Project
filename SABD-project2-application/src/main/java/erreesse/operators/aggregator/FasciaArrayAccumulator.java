package erreesse.operators.aggregator;

public class FasciaArrayAccumulator {

    private int[] fasciaCounter;

    private void initArrayCounter() {
        fasciaCounter = new int[12];
    }
    public FasciaArrayAccumulator() {
        initArrayCounter();
    }

    private void checIndex(int index) {
        if (index<0 || index > 12) {
            throw new IllegalArgumentException("index out of range");
        }
    }

    public void hit(int index) {
        checIndex(index);
        fasciaCounter[index]++;
    }

    public int getCountByIndex(int index) {
        checIndex(index);
        return fasciaCounter[index];
    }

    public FasciaArrayAccumulator mergeWith(FasciaArrayAccumulator otherAcc) {

        for (int i =0; i<12; i++) {
            fasciaCounter[i] += otherAcc.fasciaCounter[i];
        }
        return this;
    }



}
