package erreesse.operators.cogroup;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.NoSuchElementException;

public class ComputePopularUserCGF implements CoGroupFunction<CommentInfoPOJO, CommentInfoPOJO, Tuple2<Long, Double>> {

    @Override
    public void coGroup(Iterable<CommentInfoPOJO> iterableA,
                        Iterable<CommentInfoPOJO> iterableB,
                        Collector<Tuple2<Long, Double>> out) throws Exception {

        long totalLike = 0L;
        for (CommentInfoPOJO singleCip : iterableA) {
            totalLike += singleCip.getRecommendations();
        }

        long b = 0L;
        for (CommentInfoPOJO singleCip : iterableB) {
            b++;
        }

        double finalResult = 0.3 * totalLike + 0.7 * b;

        CommentInfoPOJO nextA = null;
        CommentInfoPOJO nextB = null;
        Long key = null;
        try {
            nextA = iterableA.iterator().next();
            nextB = iterableB.iterator().next();
        } catch (NoSuchElementException e) {

        }
        if (nextA != null) key = nextA.getUserID();
        if (nextB != null) key = nextB.getUserID();

        out.collect(new Tuple2<>(key, finalResult));

    }
}
