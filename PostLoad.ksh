#!/bin/ksh 
#Files = *.txt 
for file in *.txt 
do echo "$file\n"
head -1 $file | tr 'a-z' 'A-Z' >QVFiles/$file 
tail +2 $file | sed -e "s/N\/A/Not Specified/g;" >> QVFiles/$file
done 
echo "Done!"
exit 0
