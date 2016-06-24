# CascadingJoinTwoFiles

Create jar-file: sudo gradle clean jar

Clean existing output directory: rm -rf output

Run hadoop jod: hadoop jar ./build/libs/impatient.jar data/rain.txt output/rain

View result: more output/rain/part-00000