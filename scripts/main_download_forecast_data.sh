#!/bin/bash
###################################################################################
#         Auto download ROMS forecast data from GFS and RTOFS website             #
#                            write by Zhiguo                                      #
#                    Last modify:20250405                                         #
###################################################################################
###################################################################################
#                Download job start at 00:30 everday automaticlly                 #
###################################################################################

# 设置环境变量
export LANG=en_US.UTF-8
export TZ=Asia/Shanghai

# 初始化时间变量
dt0=$(date -d -1days '+%Y%m%d')
dt1=$(date -d 0days '+%Y%m%d')
dt=$(date -d 0days '+%Y-%m-%d')
jobdir="/data/nwpoms-forecast/scrips/"
cd ${jobdir} || exit 1

###################################################################################
#                        立即执行 RTOFS 下载                                      #
###################################################################################

rtofs_output_dir="/data/nwpoms-forecast/RTOFS/${dt}/"
mkdir -p ${rtofs_output_dir}
rm -rf rtofs_download_log.txt

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始下载RTOFS数据" >> rtofs_download_log.txt
./download_rtofs_forecast.sh $dt0 ${rtofs_output_dir} >> rtofs_download_log.txt 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') RTOFS下载完成" >> rtofs_download_log.txt

###################################################################################
#                        安排 MERCATOR 下载任务到14:00执行                          #
###################################################################################

# 创建临时脚本
tmp_script="${jobdir}mercator_script_$(date '+%Y-%m-%d').sh"
# 删除昨天的临时脚本
rm -rf "${jobdir}mercator_script_$(date -d -1days '+%Y-%m-%d').sh"
# 写入临时脚本
cat > ${tmp_script} <<'EOF'
#!/bin/bash
export LANG=en_US.UTF-8
export TZ=Asia/Shanghai

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始下载MERCATOR数据" >> mercator_download_log.txt
python download_mercator_global_forecast.py >> mercator_download_log.txt 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') MERCATOR数据下载完成" >> mercator_download_log.txt
EOF

# 设置执行权限
chmod +x ${tmp_script}

# 计算延迟执行时间（当天14:00）
current_hour=$(date +%H)
if [ ${current_hour#0} -lt 15 ]; then
    target_time="14:00"
else
    target_time="14:00 tomorrow"
fi

# 提交at任务
echo "${tmp_script}" | at -M  ${target_time} 2>> at_job.log

echo "$(date '+%Y-%m-%d %H:%M:%S') 已提交MERCATOR下载任务到 ${target_time}" >> at_job.log

###################################################################################
#                       安排GFS和ECMWF下载任务到16:00执行                         #
###################################################################################

# 创建临时脚本
tmp_script="${jobdir}tmp_script_$(date '+%Y-%m-%d').sh"
# 删除昨天的临时脚本
rm -rf "${jobdir}tmp_script_$(date -d -1days '+%Y-%m-%d').sh"
# 写入临时脚本
cat > ${tmp_script} <<'EOF'
#!/bin/bash
export LANG=en_US.UTF-8
export TZ=Asia/Shanghai

dt0=$(date -d -1days '+%Y%m%d')
dt1=$(date -d 0days '+%Y%m%d')
dt=$(date -d 0days '+%Y-%m-%d')
jobdir="/data/nwpoms-forecast/scrips/"

cd ${jobdir} || exit 1

# GFS大气数据下载
gfs_atoms_output_dir="/data/nwpoms-forecast/GFS/atoms/${dt}/"
mkdir -p ${gfs_atoms_output_dir}
rm -rf gfs_atoms_download_log.txt

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始下载GFS大气数据" >> gfs_atoms_download_log.txt
./download_gfs_atoms.sh $dt1 $gfs_atoms_output_dir >> gfs_atoms_download_log.txt 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') GFS大气数据下载完成" >> gfs_atoms_download_log.txt

# GFS波浪数据下载
gfs_wave_output_dir="/data/nwpoms-forecast/GFS/wave/${dt}/"
mkdir -p ${gfs_wave_output_dir}
rm -rf gfs_wave_download_log.txt

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始下载GFS波浪数据" >> gfs_wave_download_log.txt
./download_gfs_wave.sh $dt1 $gfs_wave_output_dir >> gfs_wave_download_log.txt 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') GFS波浪数据下载完成" >> gfs_wave_download_log.txt

# ECMWF大气数据下载
ecmwf_atoms_output_dir="/data/nwpoms-forecast/ecmwf/atoms/${dt}/"
mkdir -p ${ecmwf_atoms_output_dir}
rm -rf ecmwf_atoms_download_log.txt

echo "$(date '+%Y-%m-%d %H:%M:%S') 开始下载ECMWF数据" >> ecmwf_atoms_download_log.txt
python download_ecmwf_forecast.py >> ecmwf_atoms_download_log.txt 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') ECMWF数据下载完成" >> ecmwf_atoms_download_log.txt
EOF

# 设置执行权限
chmod +x ${tmp_script}

# 计算延迟执行时间（当天17:00）
current_hour=$(date +%H)
if [ ${current_hour#0} -lt 17 ]; then
    target_time="17:00"
else
    target_time="17:00 tomorrow"
fi

# 提交at任务
echo "${tmp_script}" | at -M  ${target_time} 2>> at_job.log

echo "$(date '+%Y-%m-%d %H:%M:%S') 已提交GFS和ECMWF下载任务到 ${target_time}" >> at_job.log

###################################################################################
#                            清理临时文件                                         #
###################################################################################
# 保留临时脚本以供调试，实际运行时可取消注释下一行
#rm -f ${tmp_script}

exit 0
