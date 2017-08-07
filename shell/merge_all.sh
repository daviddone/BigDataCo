var=$(find -name "*.sh*")
echo "all files:"$var
for item in $var
do
    echo "each file:$item"
done