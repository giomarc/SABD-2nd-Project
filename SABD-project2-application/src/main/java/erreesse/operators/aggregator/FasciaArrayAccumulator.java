package erreesse.operators.aggregator;

public class FasciaArrayAccumulator {

    private int[] fasciaCounter;

    private void initArrayCounter() {
        fasciaCounter = new int[12];
    }
    public FasciaArrayAccumulator() {
        initArrayCounter();
    }

    private void checkIndex(int index) {
        if (index<0 || index > 12) {
            throw new IllegalArgumentException("index out of range");
        }
    }

    // increment the relative counter
    public void hit(int index) {
        checkIndex(index);
        fasciaCounter[index]++;
    }

    public int getCountByIndex(int index) {
        checkIndex(index);
        return fasciaCounter[index];
    }

    // merge two array: sum value index by index
    public FasciaArrayAccumulator mergeWith(FasciaArrayAccumulator otherAcc) {

        for (int i =0; i<12; i++) {
            fasciaCounter[i] += otherAcc.fasciaCounter[i];
        }
        return this;
    }



}
