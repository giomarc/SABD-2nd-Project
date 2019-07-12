package erreesse.operators.cogroup;

import erreesse.metrics.LatencyTuple2;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.pojo.LatencyCommentInfoPojo;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class ComputePopularUserCGF extends RichCoGroupFunction<CommentInfoPOJO, CommentInfoPOJO, LatencyTuple2<Long, Double>> {

    // use flink state variable shared between different instances of same operator
    // mapstate with entry of <CommentId, UserId>
    protected transient MapState<Long,Long> mappaCommentiUtenti;
    protected transient long maxTimeStamp = 0L;

    private void updateMaxTimeStamp(Iterable<CommentInfoPOJO> iterable) {

        Iterator<CommentInfoPOJO> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            CommentInfoPOJO next = iterator.next();
            if (next instanceof LatencyCommentInfoPojo) {
                maxTimeStamp = Math.max(maxTimeStamp, ((LatencyCommentInfoPojo) next).getStartTime());
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long,Long> descriptor =
                new MapStateDescriptor<>(
                        "mappaCommentiUtenti",
                        Long.class,
                        Long.class
                        ); // default value of the state, if nothing was set

        // use a cache validator policy for mapstate
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();

        descriptor.enableTimeToLive(ttlConfig);

        mappaCommentiUtenti = getRuntimeContext().getMapState(descriptor);

    }

    private void insertCommentIDMapping(CommentInfoPOJO cip) {
        try {
            mappaCommentiUtenti.put(cip.getCommentID(), cip.getUserID());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // search for authorid in both direct and indirect stream
    // since we can have empty iterable of either type
    private Long getAuthorUserID(Iterable<CommentInfoPOJO> direct, Iterable<CommentInfoPOJO> indirect) {
        Long key = null;
        CommentInfoPOJO nextA;
        CommentInfoPOJO nextB;

        if (direct.iterator().hasNext()) {
            nextA = direct.iterator().next();
            key = nextA.getUserID();
        }
        else if (indirect.iterator().hasNext()){
            nextB = indirect.iterator().next();
            try {
                key = mappaCommentiUtenti.get(nextB.getInReplyTo());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return key;
    }

    @Override
    public void coGroup(Iterable<CommentInfoPOJO> iterableDirect,
                        Iterable<CommentInfoPOJO> iterableIndirect,
                        Collector<LatencyTuple2<Long, Double>> out) throws Exception {

        // iterableA -> directComment
        // iterableB -> indirectComment

        double totalLike = 0.0;
        for (CommentInfoPOJO singleCip : iterableDirect) {
            totalLike += singleCip.getRecommendations();
            // popolo la mappa per ritrovate l'userid passato il commentid
            insertCommentIDMapping(singleCip);
        }
        // update maxTimestamp
        updateMaxTimeStamp(iterableDirect);

        double b = 0.0;
        for (CommentInfoPOJO singleCip : iterableIndirect) {
            b += 1.0;
        }

        // update maxTimestamp
        updateMaxTimeStamp(iterableIndirect);

        double finalResult = 0.3 * totalLike + 0.7 * b;

        // estraggo lo userId o dai commenti diretti o dalla mappa di stato in caso di commento indiretto
        Long key = getAuthorUserID(iterableDirect,iterableIndirect);

        LatencyTuple2<Long, Double> sendToOut = new LatencyTuple2<>(key,finalResult);
        sendToOut.setStartTime(maxTimeStamp);

        if (key!=null) {
            // emit final result tuple
            out.collect(sendToOut);
        }

    }
}
