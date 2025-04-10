#!/bin/bash
date="$1"
dir="$2"

cd "${dir}" || exit 1

# 设置超时参数（单位：秒）
DOWNLOAD_TIMEOUT=2400  # 40分钟超时时间
MAX_PARALLEL=5
current_jobs=0

# 预生成文件列表
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

# 清理残留临时文件（关键新增部分）
echo "[INFO] Cleaning up residual temp files..."
for url in "${urls[@]}"; do
    tmp_file="$(basename "$url").tmp"
    if [[ -f "$tmp_file" ]]; then
        echo "[WARN] Removing residual temp file: $tmp_file"
        rm -f "$tmp_file"
    fi
done

# 创建下载函数
download_file() {
    local url=$1
    local max_retries=5
    local retry_count=0
    local local_file=$(basename "$url")
    local remote_size=""
    local local_size=""
    local timeout_flag=0

    # 获取远程文件大小
    remote_size=$(wget --spider --server-response "$url" 2>&1 | awk '/^213/ {print $2; exit}')
    if [[ -z "$remote_size" ]]; then
        echo "[ERROR] Failed to get size: $url" >&2
        return 1
    fi

    while ((retry_count < max_retries)); do
        # 检查现有文件
        if [[ -f "$local_file" ]]; then
            local_size=$(stat -c "%s" "$local_file" 2>/dev/null || echo 0)
            if ((local_size == remote_size)); then
                echo "[INFO] File exists: $local_file"
                return 0
            else
                echo "[WARN] Size mismatch: $local_file (local:$local_size vs remote:$remote_size)"
                rm -f "$local_file"
            fi
        fi

        # 开始下载（使用临时文件）
        echo "[INFO] Downloading ($((retry_count+1))/$max_retries): $url"
        
        # 使用 timeout 命令控制下载时间
        if timeout $DOWNLOAD_TIMEOUT wget -q -c "$url" -O "${local_file}.tmp"; then
            mv "${local_file}.tmp" "$local_file"
            # 最终校验
            local_size=$(stat -c "%s" "$local_file" 2>/dev/null || echo 0)
            if ((local_size == remote_size)); then
                echo "[INFO] Download verified: $local_file"
                return 0
            else
                echo "[WARN] Post-download size mismatch: $local_file"
                rm -f "$local_file"
                timeout_flag=0  # 重置超时标志
            fi
        else
            local exit_status=$?
            # 处理超时情况
            if [[ $exit_status -eq 124 ]]; then
                echo "[WARN] Download timeout: $url (retry $((retry_count+1)))"
                timeout_flag=1
            else
                echo "[WARN] Download failed (code $exit_status): $url"
                rm -f "${local_file}.tmp"
                timeout_flag=0
            fi
        fi

        # 如果因超时失败，立即重试（不等待）
        if ((timeout_flag == 1)); then
            ((retry_count++))
            continue
        fi

        # 指数退避重试（仅限非超时错误）
        local backoff_time=$(( (2 ** retry_count) ))
        echo "[INFO] Waiting ${backoff_time}s before retry..."
        sleep $backoff_time
        
        ((retry_count++))
    done

    # 最终清理超时残留文件
    if [[ -f "${local_file}.tmp" ]]; then
        echo "[WARN] Cleaning up timeout residue: ${local_file}.tmp"
        rm -f "${local_file}.tmp"
    fi

    echo "[ERROR] Failed after $max_retries attempts: $url" >&2
    return 1
}

# 执行并行下载
for url in "${urls[@]}"; do
    download_file "$url" &
    ((current_jobs++))

    # 控制并行数量
    if ((current_jobs >= MAX_PARALLEL)); then
        wait -n
        ((current_jobs--))
    fi
done

# 等待所有剩余任务完成
wait
