#!/bin/sh

# download globle 0.25*0.25 GFS atoms data 
 
dt1=$1
dir=$2

perl get_gfs_atoms.pl data ${dt1}00 0 192 3 all 'PRMSL.mean_sea_level|SPFH.2_m_above_ground|TMP.2_m_above_ground|RH.2_m_above_ground|PRATE.surface|UGRD.10_m_above_ground|VGRD.10_m_above_ground|DLWRF.surface|DSWRF.surface' ${dir}

# convert GFS dataform  from grib2 to netcdf 
max_retries=5
timeout_duration=60  # 60s超时
rm -rf "${dir}*.nc"

# 查找所有gfs.t开头的文件，处理特殊字符
 a=$(find ${dir} -name gfs.t\*)
 if [ -n "$a" ]; then
    for file in $a; do
    ncfile="${file}.nc"
    attempt=1
    success=0

     while [ $attempt -le $max_retries ]; do
         echo "尝试转换文件: ${file} (第${attempt}次)"
        
         # 使用临时文件，避免部分写入
         tmp_ncfile="${ncfile}.tmp"
         rm -f "${tmp_ncfile}"  # 清理旧的临时文件

         # 执行转换并检查超时
         if timeout ${timeout_duration} wgrib2 "${file}" -netcdf "${tmp_ncfile}"; then
             # 检查生成的临时文件是否有效
             if [ -f "${tmp_ncfile}" ] && [ -s "${tmp_ncfile}" ]; then
                 mv "${tmp_ncfile}" "${ncfile}"  # 原子操作重命名
                 echo "转换成功: ${file} -> ${ncfile}"
                 rm -f "${file}"  # 删除原文件
                 success=1
                 break
             else
                 echo "错误: 生成的NetCDF文件无效或为空"
             fi
         else
             # 记录超时或转换失败
             echo "错误: 转换超时或失败，退出码: $?"
         fi

         attempt=$((attempt + 1))
         sleep $((attempt * 5))  # 指数退避等待
     done

     if [ $success -eq 0 ]; then
         echo "错误: 文件 ${file} 转换失败，已达最大重试次数"
         # 可选的错误处理，如移动失败文件到其他目录
     fi
   done
 fi

#a=$(find ${dir} -name gfs.t\*)
#
#if [ -n "$a" ]; then
#    for file in $a; do
#        ncfile="${file}.nc"
#        
#        # 循环直到转换成功并且目标 NetCDF 文件存在
#        while true; do
#            wgrib2 $file -netcdf $ncfile
#            # 判断转换后的 NetCDF 文件是否存在
#            if [ -f "$ncfile" ]; then
#                rm -rf $file
#                break
#            fi
#        done
#    done
#fi

