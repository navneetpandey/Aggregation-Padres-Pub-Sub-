#!/bin/bash
#cat  ../temp.tmp | awk -F, '{print $6}' | awk -F] '{sum+=$1}END{print sum;print NR}'
cat  $1 | awk -F, '{print $6}' | awk -F] '{sum+=$1}END{print sum;print NR}'
