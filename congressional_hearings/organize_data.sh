find ./data -type f | (
    while read FNAME; do
        MD5=$( echo $FNAME | md5sum )
        first=${MD5:0:1}
        second=${MD5:1:1}
        mkdir -p ./data/${first}/${second}
        mv $FNAME ./data/${first}/${second} || exit
    done;
)
