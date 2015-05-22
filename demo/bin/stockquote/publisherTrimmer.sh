#!/bin/bash
for i in `ls *`
do 
	head -n 1000 $i> temp ; 
	cat temp > $i  ; 
done
rm temp
