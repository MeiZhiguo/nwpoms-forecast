#!/bin/bash
date="$1"
dir="$2"

cd "${dir}" || exit 1

# 设置并发控制参数
MAX_PARALLEL=8  # 最大并行下载数（根据带宽调整）
current_jobs=0

# 创建下载函数
download_file() {
    local url=$1
    wget -q -nc -c "$url" &
    ((current_jobs++))
    # 控制并行数量
    if [[ $current_jobs -ge $MAX_PARALLEL ]]; then
        wait -n
        ((current_jobs--))
    fi
}

# 生成文件列表
declare -a urls=()
for hour in 024 048 072 096 120 144 168 192; do
    base_url="ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/rtofs/prod/rtofs.${date}"
    urls+=(
        "${base_url}/rtofs_glo_2ds_f${hour}_diag.nc"
        "${base_url}/rtofs_glo_3dz_f${hour}_daily_3zsio.nc"
        "${base_url}/rtofs_glo_3dz_f${hour}_daily_3ztio.nc"
        "${base_url}/rtofs_glo_3dz_f${hour}_daily_3zuio.nc"
        "${base_url}/rtofs_glo_3dz_f${hour}_daily_3zvio.nc"
    )
done

# 执行并行下载
for url in "${urls[@]}"; do
    download_file "$url"
done

# 等待所有后台任务完成
wait
