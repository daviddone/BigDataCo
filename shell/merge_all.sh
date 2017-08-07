var=$(find -name "*.sh*")
echo "all files:"$var
for item in $var
do
    echo "each file:$item"
    echo "$item" >> allText
    echo "########################" >> allText
    cat $item >> allText
done