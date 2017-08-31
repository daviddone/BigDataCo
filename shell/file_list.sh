list=$(find /home1/oozie/oozie_wy/david/mro_hubei/ -name "*.q")
for file in $list
do
    arr=(${arr[*]} $file)
done
list2=$(find /home1/oozie/oozie_wy/david/mro_hubei/ -name "*.q")
for file in $list2
do
   arr=(${arr[*]} $file)
done
echo ${arr[@]}
echo ${#arr[@]}

