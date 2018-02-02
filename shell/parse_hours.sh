day=20180130
reducenum=200
for((i=0;i<=23;i++))
do
k=$(printf "%02d" ${i})
echo ${day}${k}
echo "hadoop jar gx.jar base ${day}${k} ${reducenum}"
done
