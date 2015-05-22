#!/bin/bash
for i in `ls * | grep -v "\."` 
do
   echo $i; 
   git checkout $i;
   touch temp.tmp;
  line=`cat $i | wc -l `;
    echo $line;
   while [ `cat temp.tmp | wc -l` -le 3000 ]
  do
	echo `cat $i | wc -l`;	
      cat $i >> temp.tmp;
   done
	mv temp.tmp $i;
done
