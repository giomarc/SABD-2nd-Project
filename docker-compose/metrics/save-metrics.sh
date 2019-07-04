 #!/bin/sh

JOB_ID="f79b0d113c2ab478fb48c7ec7d300257"
SOURCE_VTX_ID="cbc357ccb763df2852fee8c4fc7d55f2" # SOURCE OPERATOR

SINK_VTX_ID_1="3f6da82815af3a69d5051d55da462b26" # SOURCE OPERATOR
SINK_VTX_ID_2="2db423c13d9ac2386eade7faec8cc15a" # SOURCE OPERATOR
SINK_VTX_ID_3="46814e79612081fb6e1c6bbc7d845c20" # SOURCE OPERATOR

FLINK_THR_METRIC_URL="http://localhost:8081/jobs/$JOB_ID/vertices/$SOURCE_VTX_ID/metrics?get=0.numRecordsOutPerSecond"
FLINK_LATENCY_METRIC_URL_1="http://localhost:8081/jobs/$JOB_ID/vertices/$SINK_VTX_ID_1/metrics?get=0.Window(TumblingEventTimeWindows(3600000)__EventTimeTrigger__RankingWF).ErreEsseLatencyQuery1"
FLINK_LATENCY_METRIC_URL_2="http://localhost:8081/jobs/$JOB_ID/vertices/$SINK_VTX_ID_2/metrics?get=0.Window(TumblingEventTimeWindows(86400000)__EventTimeTrigger__RankingWF).ErreEsseLatencyQuery1"
FLINK_LATENCY_METRIC_URL_3="http://localhost:8081/jobs/$JOB_ID/vertices/$SINK_VTX_ID_3/metrics?get=0.Window(TumblingEventTimeWindows(604800000)__EventTimeTrigger__RankingWF).ErreEsseLatencyQuery1"
FILE_OUTPUT="metrics_$(date +'%s').txt";

echo $FLINK_THR_METRIC_URL;
echo $FLINK_LATENCY_METRIC_URL_1;
echo $FLINK_LATENCY_METRIC_URL_2;
echo $FLINK_LATENCY_METRIC_URL_3;
while true
do 
    THR=$(curl $FLINK_THR_METRIC_URL);
    LATENCY1=$(curl $FLINK_LATENCY_METRIC_URL_1);
    LATENCY2=$(curl $FLINK_LATENCY_METRIC_URL_2);
    LATENCY3=$(curl $FLINK_LATENCY_METRIC_URL_3);
    DATETIME=$(date);
    echo $THR"record/s;"$LATENCY1"ms;"$LATENCY2"ms;"$LATENCY3"ms;"$DATETIME>>$FILE_OUTPUT;
    sleep 10
done
