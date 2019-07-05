 #!/bin/bash

function cleanstring() {

    toFind='[{"id":"0.numRecordsOutPerSecond","value":"'
    replaceWith=""

    firstStep="${1/$toFind/$replaceWith}"

    toFind='"}]'
    secondStep="${firstStep/$toFind/$replaceWith}"
    echo $secondStep;
}


JOB_ID="126a3b53cd7f38bbdb06d42aead2652d" #REMEMBER THAT IT CHANGES AT EACH CREATION OF THE PLAN
SOURCE_THR_VTX_ID="cbc357ccb763df2852fee8c4fc7d55f2" # SOURCE OPERATOR
SINK_THR_VTX_ID="f1c4789f15c75d0d32f107a9595d229d" # SINK OPERATOR



FLINK_THR_IN_METRIC_URL="http://localhost:8081/jobs/$JOB_ID/vertices/$SOURCE_THR_VTX_ID/metrics?get=1.numRecordsOutPerSecond"
FLINK_THR_OUT_METRIC_URL="http://localhost:8081/jobs/$JOB_ID/vertices/$SINK_THR_VTX_ID/metrics?get=0.numRecordsInPerSecond"

FILE_OUTPUT="metrics_$(date +'%s').txt";

echo $FLINK_THR_IN_METRIC_URL;
echo $FLINK_THR_OUT_METRIC_URL;


while true
do 
    THR_IN=$(curl $FLINK_THR_IN_METRIC_URL);
    THR_OUT=$(curl $FLINK_THR_OUT_METRIC_URL);

    #THR_IN=cleanstring $THR_IN;
    #THR_OUT=cleanstring $THR_OUT;
    DATETIME=$(date);

    echo $THR_IN";"$THR_OUT";"$DATETIME>>$FILE_OUTPUT;
    sleep 10
done



