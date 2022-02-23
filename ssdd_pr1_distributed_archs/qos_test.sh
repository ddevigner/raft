#!/bin/bash
# Argumentos: qos_test.sh ip_servidor puerto_servidor peticiones tts comentario
total=0
i=0
output=tests/test_$3_$4_$5.txt
echo > $output
while read line
do
        echo $line >> $output
        partial=$(echo $line | sed 's/.$//' | cut -d' ' -f2)
        total=$(echo "$total + $partial" | bc)
        i=$(($i + 1))
done < <(./client $1 $2 $3 $4)

sort -n $output > tmp && cat tmp > $output && rm tmp
echo "total time: ${total}s" >> $output
echo -n "media: " >> $output && echo "$total / $i" | bc -l >> $output
