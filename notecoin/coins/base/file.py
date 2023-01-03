import copy
import logging
import os
from datetime import datetime, timedelta

from notecoin.base.drive.lanzou import LanzouDirectory
from notecoin.coins.base.load import LoadDataKline, LoadTradeKline
from notefile.compress import tarfile
from tqdm import tqdm

logger = logging.getLogger()


def merge_unique(input_csv_list, put_csv):
    with open(put_csv, 'w') as outfile:
        for i, csv_file in tqdm(enumerate(input_csv_list), desc='merge files', total=len(input_csv_list)):
            with open(csv_file, 'r') as infile:
                if i > 0:
                    next(infile)
                for line in infile:
                    outfile.write(line)


class FileProperty:
    def __init__(self,
                 exchange_name='okex',
                 data_type='kline',
                 path='~/workspace/tmp',
                 start_date=datetime.today(),
                 end_date=datetime.today(),
                 freq='daily',
                 timeframe='1m',
                 file_format='%Y%m%d'):
        self.path = path
        self.freq = freq
        self.data_type = data_type
        self.timeframe = timeframe
        self.start_date = start_date
        self.end_date = end_date
        self.file_format = file_format
        self.exchange_name = exchange_name

    def file_path_dir(self, absolute=True):
        path = f"notecoin/{self.exchange_name}/{self.data_type}-{self.freq}-{self.timeframe}"
        if absolute:
            path = f"{self.path}/{path}"
            if not os.path.exists(path):
                os.makedirs(path)
        return path

    @property
    def filename_prefix(self):
        return f"{self.exchange_name}-{self.data_type}-{self.freq}-{self.timeframe}-{self.start_date.strftime(self.file_format)}"

    def file_path_csv(self, absolute=True):
        return f"{self.file_path_dir(absolute)}/{self.filename_prefix}.csv"

    def file_path_tar(self, absolute=True):
        return f"{self.file_path_dir(absolute)}/{self.filename_prefix}.tar"

    def arcname(self, file):
        return os.path.join(self.file_path_dir(False), os.path.basename(file))


class DataFileProperty:
    def __init__(self, exchange, data_type='kline', path='~/workspace/tmp', start_date=datetime.today(),
                 end_date=datetime.today(), freq='daily', timeframe='1m', file_format='%Y%m%d'):
        self.exchange = exchange
        self.data_type = data_type
        self.file_pro = FileProperty(exchange_name=exchange.name.lower(), path=path, freq=freq,
                                     data_type=data_type, timeframe=timeframe, start_date=start_date, end_date=end_date,
                                     file_format=file_format)
        self.drive = LanzouDirectory(fid=6073401)

    def change_freq(self, freq):
        self.file_pro.freq = freq

    def change_data_type(self, data_type):
        self.data_type = data_type
        self.file_pro.data_type = data_type

    def change_timeframe(self, timeframe):
        self.file_pro.timeframe = timeframe

    def sync(self, path):
        logger.info('sync file')
        self.drive.scan_all_file()
        self.drive.sync(f'{path}/notecoin')

    def tar_exists(self):
        if self.drive.file_exist(self.file_pro.file_path_tar(False)):
            return True
        if os.path.exists(self.file_pro.file_path_tar()):
            return False

    def _daily_load_and_save(self, file_pro: FileProperty) -> bool:
        if self.tar_exists():
            return False
        self.sync(self.file_pro.path)

        logger.info(f'download for {file_pro.file_path_tar(absolute=False)}')
        unix_start, unix_end = int(file_pro.start_date.timestamp() * 1000), int(file_pro.end_date.timestamp() * 1000)

        if self.data_type == 'kline':
            exchan = LoadDataKline(self.exchange, unix_start=unix_start,
                                   unix_end=unix_end, csv_path=file_pro.file_path_csv(), timeframe=file_pro.timeframe)
        else:
            exchan = LoadTradeKline(self.exchange, unix_start=unix_start,
                                    unix_end=unix_end, csv_path=file_pro.file_path_csv())
        # 下载
        exchan.load_all()
        # 压缩
        with tarfile.open(file_pro.file_path_tar(), "w|xz") as tar:
            tar.add(file_pro.file_path_csv(), arcname=file_pro.arcname(file_pro.file_path_csv()))
        # 删除
        os.remove(file_pro.file_path_csv())
        return True

    def _merge_and_save(self, file_pro: FileProperty) -> bool:
        if self.tar_exists():
            return False
        self.sync(self.file_pro.path)

        logger.info(f'merge for {file_pro.file_path_tar(absolute=False)}')

        result = []
        for i in range(1000):
            day = copy.deepcopy(file_pro)
            day.freq = 'daily'
            day.start_date, day.end_date = file_pro.start_date + timedelta(days=i), file_pro.start_date + timedelta(
                days=i + 1)
            if day.start_date < file_pro.start_date:
                continue
            if day.start_date > file_pro.end_date:
                break
            result.append(day.file_path_csv())
            if os.path.exists(day.file_path_csv()):
                continue
            # download
            fid = self.drive.file_fid(day.file_path_tar(False))
            if fid == -1:
                self._daily_load_and_save(day)
                fid = self.drive.file_fid(day.file_path_tar(False))

            self.drive.drive.down_file_by_id(fid, save_path=os.path.dirname(day.file_path_tar()), overwrite=True)
            # unzip
            with tarfile.open(day.file_path_tar(), "r|xz") as tar:
                tar.extractall(path=day.path)
            os.remove(day.file_path_tar())
            # remove
        merge_unique(result, file_pro.file_path_csv())

        # 压缩
        with tarfile.open(file_pro.file_path_tar(), "w|xz") as tar:
            tar.add(file_pro.file_path_csv(), arcname=file_pro.arcname(file_pro.file_path_csv()))
        # 删除
        os.remove(file_pro.file_path_csv())
        [os.remove(csv) for csv in result]
        return True

    def load_daily(self, start_time, end_time):
        self.file_pro.start_date, self.file_pro.end_date = start_time, end_time
        self._daily_load_and_save(self.file_pro)

    def load_merge(self, start_time, end_time):
        self.file_pro.start_date, self.file_pro.end_date = start_time, end_time
        self._merge_and_save(self.file_pro)
