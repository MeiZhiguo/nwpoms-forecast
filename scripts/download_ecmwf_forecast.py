#######################################################################################################
#                      This Pyhton script is developed for downlaoding ECMWF                          #
#                   atomsphere  and wave forecast data through ecmwf.opendata api.                    #
#                                  Write By Zhiguo 2025/04/01                                         #
#                                   Last modify : 2025/04/09                                          #
#-----------------------------------------------------------------------------------------------------#
#                       |-----------------------------------------------------------------------------#
#                       | URL:https://data.ecmwf.int/forecasts/20250409/00z/ifs/0p25/oper/            #
#     Variables : Atoms | t2      2 metre temperature                                                 #
#                       | t2d     2 metre dewpoint temperature                                        #
#                       | msl     Mean sea level pressure                                             #
#                       | tprate  Total precipitation rate                                            #
#                       | 10u     10 metre U wind component                                           #
#                       | 10v     10 metre V wind component                                           #
#                       | rh      Relative humidity                                                   #
#                       | qh      Specific humidity                                                   #
#                       | ssrd	  Surface net short-wave (solar) radiation downwards                  #
#                       | strd	  Surface net long-wave (thermal) radiation downwards                 #
#                       |-----------------------------------------------------------------------------#
#                       | URL:https://data.ecmwf.int/ecpds/home/opendata/20250409/00z/ifs/0p25/wave/  #
#                 Wave  | mwd	  Mean wave direction                                                 #
#                       | mp2	  Mean zero-crossing wave period                                      #
#                       | mwp	  Mean wave period                                                    #
#                       | pp1d	  Peak wave period                                                    #
#                       | swh	  Significant height of combined wind waves and swell                 #
#                       |-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------------------------------#
#      This script is designed to automate the daily operational workflow of retrieving global        #
#      meteorological forecast data initialized at 00:00 UTC from ECMWF, ensuring timely and          #
#                   reliable data acquisition for downstream applications.                            #
#######################################################################################################
from ecmwf.opendata import Client
import numpy as np
import netCDF4 as nc
import time
import datetime
import requests
from glob import glob
import threading
from netCDF4 import Variable
from tqdm import tqdm
import os

client = Client(source='ecmwf')
latest_start_time = client.latest(type='fc')
start_time = latest_start_time.replace(hour=00, minute=0)

# 创建输出目录
atoms_output_dir = os.path.join('/data/nwpoms-forecast/ECMWF/atoms/', start_time.strftime('%Y-%m-%d'))
wave_output_dir = os.path.join('/data/nwpoms-forecast/ECMWF/wave/', start_time.strftime('%Y-%m-%d'))
os.makedirs(atoms_output_dir, exist_ok=True)
os.makedirs(wave_output_dir, exist_ok=True)

# 时间步长配置
step1 = np.arange(0, 145, 3)
step2 = np.arange(144, 192, 6)
steps = np.concatenate((step1, step2))

class DownloadTimeout(Exception):
    """自定义超时异常"""
    pass

def safe_download(request_params, target_path, max_retry=5, timeout=300):
    """支持主动超时中断的下载函数（兼容所有操作系统）"""
    retry_count = 0
    temp_path = f"{target_path}.tmp"

    while retry_count < max_retry:
        download_event = threading.Event()
        error = None
        result = None

        # 定义下载线程
        def download_task():
            nonlocal error, result
            try:
                # 清理历史文件
                if os.path.exists(temp_path):
                    os.remove(temp_path)

                # 执行下载
                client = Client(source='ecmwf')
                client.retrieve(
                    **request_params,
                    target=temp_path
                )

                # 验证文件完整性
                if os.path.getsize(temp_path) < 1024:
                    raise ValueError("文件不完整")
                os.rename(temp_path, target_path)
                result = True
            except Exception as e:
                error = e
            finally:
                download_event.set()

        # 启动下载线程
        thread = threading.Thread(target=download_task)
        thread.start()

        # 等待线程完成或超时
        thread.join(timeout)

        if thread.is_alive():
            # 标记超时并清理
            error = DownloadTimeout(f"下载超过 {timeout} 秒未完成")
            if os.path.exists(temp_path):
                os.remove(temp_path)
        else:
            if result:
                return True

        # 错误处理
        print(f"尝试 {retry_count+1}/{max_retry}: {target_path} 失败 - {str(error)}")
        retry_count += 1
        time.sleep(min(2 ** retry_count, 30))  # 指数退避等待

    return False

# 主下载循环
for step in tqdm(steps, desc='处理预报步长'):
    # 生成文件名（保持原有逻辑）
    atoms_filename = f"ecmwf.atoms.{start_time.strftime('%Y%m%d%H')}.f{step:03}h.0p25.grib2"
    atoms_target = os.path.join(atoms_output_dir, atoms_filename)
    wave_filename = f"ecmwf.wave.{start_time.strftime('%Y%m%d%H')}.f{step:03}h.0p25.grib2"
    wave_target = os.path.join(wave_output_dir, wave_filename)
    # 大气数据下载参数
    atoms_params = {
        'date': start_time.strftime('%Y-%m-%d'),
        'time': start_time.strftime('%H'),
        'type': 'fc',
        'model': "ifs",
        'param': ['2t', '2d', '10v', '10u', 'msl', 'tprate', 'ssrd', 'strd', 'sp'],
        'step': str(step),
        'resol': '0p25'
    }
    # 海浪数据下载参数
    wave_params = {
        'date': start_time.strftime('%Y-%m-%d'),
        'time': start_time.strftime('%H'),
        'type': 'fc',
        'stream': 'wave',
        'model': "ifs",
        'param': ['mp2', 'swh', 'mwd', 'mwp', 'pp1d'],
        'step': str(step),
        'resol': '0p25'
    }
    # 执行下载
    safe_download(
        request_params=atoms_params,
        target_path=atoms_target,
        timeout=300  # 300秒超时
    )
    safe_download(
        request_params=wave_params,
        target_path=wave_target,
        timeout=300  # 300秒超时
    )


def grib2nc(filename):
    """使用wgrib2将GRIB文件转换为NetCDF并修改变量属性"""
    import subprocess
    # 生成对应的nc文件名
    nc_file = filename.replace('.grib2', '.nc')
    # 调用wgrib2进行格式转换
    cmd = f"wgrib2 {filename} -netcdf {nc_file}"
    result = subprocess.run(cmd, shell=True, capture_output=True)
    if result.returncode != 0:
        print(f"Error converting {file}: {result.stderr.decode()}")
        return
    # 仅当文件名包含"atoms"时执行属性修改
    if 'atoms' in os.path.basename(filename):
    # 修改变量属性
        with nc.Dataset(nc_file, 'a') as ds:
            ds.renameVariable('PRES_meansealevel', 'PRMSL_meansealevel')
            ds.renameVariable('TPRATE_surface', 'PRATE_surface')
            # 同步元数据到磁盘
            ds.sync()
    print(f"Converted {filename} to {nc_file}")


def copy_var_metadata(src_var, new_name, new_attrs):
    """复制源变量的元数据并更新特定属性"""
    var_attrs = {k: src_var.getncattr(k) for k in src_var.ncattrs()}
    # 删除需要覆盖的属性
    for attr in ['long_name', 'standard_name', 'units']:
        if attr in var_attrs:
            del var_attrs[attr]
    # 合并新属性
    var_attrs.update(new_attrs)
    return var_attrs

def add_humidity_vars(nc_file):
    with nc.Dataset(nc_file, 'a') as ds:
        # 读取基础变量
        t2 = ds.variables['TMP_2maboveground'][:]
        t2d = ds.variables['DPT_2maboveground'][:]
        sp = ds.variables['PRES_surface'][:]
        sp = sp / 100
        # 单位转换（原始单位为K）
        t2_c = t2[:] - 273.15
        t2d_c = t2d[:] - 273.15
        # 计算水汽压
        e = 6.11 * 10 ** (7.5 * t2d_c / (237.3 + t2d_c))  # 实际水汽压 (hPa)
        es = 6.11 * 10 ** (7.5 * t2_c / (237.3 + t2_c))  # 饱和水汽压 (hPa)
        # 计算相对湿度 (%)
        rh = np.clip((e / es) * 100.0, 0, 100)
        # 计算比湿 (kg/kg)
        q = (0.622 * e) / (sp - 0.378 * e)
        # 获取模板变量的属性
        template_var = ds.variables['TMP_2maboveground']
        # 创建相对湿度变量
        if 'RH_2maboveground' not in ds.variables:
            rh_attrs = copy_var_metadata(template_var, 'RH_2maboveground', {
                'short_name': 'RH_2maboveground',
                'long_name': 'Relative Humidity',
                'units': '%'
            })
            rh_var = ds.createVariable(
                'RH_2maboveground',
                template_var.dtype,  # 继承数据类型
                dimensions=template_var.dimensions,
                fill_value=getattr(template_var, '_FillValue', None),
                zlib=True  # 启用压缩（可选）
            )
            rh_var.setncatts(rh_attrs)
            rh_var[:] = rh
        # 创建比湿度变量
        if 'SPFH_2maboveground' not in ds.variables:
            ah_attrs = copy_var_metadata(template_var, 'SPFH_2maboveground', {
                'short_name': 'SPFH_2maboveground',
                'long_name': 'Specific Humidity',
                'units': 'kg/kg'
            })
            ah_var = ds.createVariable(
                'SPFH_2maboveground',
                template_var.dtype,
                dimensions=template_var.dimensions,
                fill_value=getattr(template_var, '_FillValue', None),
                zlib=True
            )
            ah_var.setncatts(ah_attrs)
            ah_var[:] = q


def process_ecmwf_file(filepath):
    #"""静默处理单个ECMWF文件，将累积量转换为瞬时通量"""
    try:
        filename = os.path.basename(filepath)
        parts = filename.split('.')
        if len(parts) < 4 or not parts[2].startswith('f') or not parts[2].endswith('h'):
            return
        fcst_hour = int(parts[2][1:-1])
        if fcst_hour == 0:
            return
        prev_step = 6 if fcst_hour > 144 else 3
        prev_hour = fcst_hour - prev_step
        prev_filename = f"ecmwf.{parts[1]}.f{prev_hour:03d}h.0p25.nc"
        prev_filepath = os.path.join(os.path.dirname(filepath), prev_filename)
        if not os.path.exists(prev_filepath):
            return
        with nc.Dataset(prev_filepath, 'r') as ds_prev, \
             nc.Dataset(filepath, 'r+') as ds_current:
            delta_t = (fcst_hour - prev_hour) * 3600
            for var_name in ['DLWRF_surface', 'DSWRF_surface']:
                if var_name in ds_prev.variables and var_name in ds_current.variables:
                    prev_data = ds_prev[var_name][:]
                    current_data = ds_current[var_name][:]
                    flux = (current_data - prev_data) / delta_t
                    ds_current[var_name][:] = flux.astype(ds_current[var_name].dtype)
    except Exception:
        pass


# 转换grib2格式为netcdf
def process_files(file_list, desc):
    """带重试机制的文件处理函数"""
    for file in tqdm(file_list, desc=desc):
        nc_file = file.replace('.grib2', '.nc')
        # 持续尝试直到生成目标文件
        while True:
            # 执行格式转换
            grib2nc(file)
            # 验证转换结果
            if validate_nc_file(nc_file):
                # 删除原始文件（安全删除）
                safe_remove(file)
                break

def validate_nc_file(nc_path):
    """验证NetCDF文件有效性"""
    try:
        with nc.Dataset(nc_path, 'r') as ds:
            # 基础验证：检查维度变量存在
            required_dims = ['latitude', 'longitude', 'time']
            return all(dim in ds.dimensions for dim in required_dims)
    except Exception:
        return False

def safe_remove(file_path):
    """安全删除文件"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False
    except Exception as e:
        print(f"删除文件失败 {file_path}: {str(e)}")
        return False

# 处理大气数据
atoms_files = glob(os.path.join(atoms_output_dir, 'ecmwf.*.grib2'))
process_files(atoms_files, "处理大气数据")

# 处理波浪数据
wave_files = glob(os.path.join(wave_output_dir, 'ecmwf.*.grib2'))
process_files(wave_files, "处理波浪数据")

# 计算并添加比湿和相对湿度
filelist = glob(os.path.join(atoms_output_dir, 'ecmwf.*.nc'))
for file in tqdm(filelist):
    add_humidity_vars(file)
    # 将辐射由积分转化时间平均
    process_ecmwf_file(file)
