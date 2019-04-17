#!/bin/bash
source /etc/profile
PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/bin

remote_path=/ftp_samba/112/spider/python/pubg
if [ ! -d $remote_path ]
	then
	mkdir -p $remote_path
fi

#time=`date -d "-1 hour" +"%Y%m%d%H"`
time=`date +"%Y%m%d"`
redis_key='spider:python:pubg_game:keyword:dest'
file_name=$remote_path'/pubg_game_'$time

size=$(/usr/local/bin/redis-cli -c -h 172.31.10.132 -p 6379 llen spider:python:pubg_game:keyword:dest)
if [ $size -gt 10000 ]; then
	#echo $size
	echo "lrange $redis_key 0 9999" | redis-cli -h 172.31.10.132 -p 6379 -c | awk 'NR==1{if($0!~/Redirected/)print $0}NR>1{print $0}' >> $file_name
	echo "ltrim $redis_key 10000 -1" | redis-cli -h 172.31.10.132 -p 6379 -c
elif [ $size -gt 1000 ]; then
	echo "lrange $redis_key 0 999" | redis-cli -h 172.31.10.132 -p 6379 -c | awk 'NR==1{if($0!~/Redirected/)print $0}NR>1{print $0}' >> $file_name
	echo "ltrim $redis_key 1000 -1" | redis-cli -h 172.31.10.132 -p 6379 -c
elif [ $size -gt 100 ]; then
	echo "lrange $redis_key 0 99" | redis-cli -h 172.31.10.132 -p 6379 -c | awk 'NR==1{if($0!~/Redirected/)print $0}NR>1{print $0}' >> $file_name
	echo "ltrim $redis_key 100 -1" | redis-cli -h 172.31.10.132 -p 6379 -c
elif [ $size -gt 10 ]; then
	echo "lrange $redis_key 0 9" | redis-cli -h 172.31.10.132 -p 6379 -c | awk 'NR==1{if($0!~/Redirected/)print $0}NR>1{print $0}' >> $file_name
	echo "ltrim $redis_key 10 -1" | redis-cli -h 172.31.10.132 -p 6379 -c
fi
