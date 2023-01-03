from notecoin.base.db import BaseTable
from notedrive.lanzou import LanZouDrive
from sqlalchemy import BIGINT, TIMESTAMP, Column, String, Table, func, select


class LanzouDirectory(BaseTable):
    def __init__(self, table_name="notecoin_lanzou", fid=5679873, *args, **kwargs):
        self.fid = fid
        super(LanzouDirectory, self).__init__(table_name=table_name, *args, **kwargs)
        self.table = Table(self.table_name, self.meta,
                           Column('fid', BIGINT, comment='fid', primary_key=True),
                           Column('gmt_create', TIMESTAMP(True), server_default=func.now()),
                           Column('gmt_modified', TIMESTAMP(True), server_default=func.now()),
                           Column('isfile', BIGINT, comment='是否文件', default='1'),
                           Column('name', String(100), comment='名称', default=''),
                           Column('desc', String(500), comment='描述', default='0'),
                           Column('path', String(500), comment='路径', default=''),
                           Column('downs', BIGINT, comment='下载次数', default='0'),
                           Column('url', String(100), comment='分享链接', default='0'),
                           Column('pwd', String(50), comment='提取码', default='0'),
                           )
        self.create()
        self.drive = LanZouDrive()

        self.drive.login_by_cookie()
        self.drive.ignore_limits()

    def scan_all_file(self, clear=False):
        if clear:
            self.delete_all()
        self._scan_all_file(self.fid, 'notecoin')

    def _scan_all_file(self, fid, path):
        for _dir in self.drive.get_dir_list(fid):
            _path = f'{path}/{_dir.name}'
            if self.file_exist(_path):
                continue
            share = self.drive.get_share_info(fid=_dir.id, is_file=False)
            data = {
                "fid": _dir.id,
                "name": _dir.name,
                "path": _path,
                "isfile": 0,
                "desc": share.desc,
                "url": share.url,
                "pwd": share.pwd
            }
            self.upsert(values=data)
            if _dir.name.endswith(".tar"):
                continue
            self._scan_all_file(fid=data['fid'], path=data['path'])

        for _file in self.drive.get_file_list(fid):
            _path = f'{path}/{_file.name}'
            if self.file_exist(_path):
                continue
            share = self.drive.get_share_info(fid=_file.id, is_file=True)
            data = {
                "fid": _file.id,
                "name": _file.name,
                "path": _path,
                "isfile": 1,
                "downs": _file.downs,
                "desc": share.desc,
                "url": share.url,
                "pwd": share.pwd
            }
            self.upsert(values=data)

    def file_exist(self, path):
        s = select(self.table.columns).where(self.table.columns.path == path)
        data = [line for line in self.engine.execute(s)]
        return len(data) == 1

    def file_fid(self, path):
        s = select(self.table.columns).where(self.table.columns.path == path)
        data = [line for line in self.engine.execute(s)]

        if len(data) == 1:
            return data[0]['fid']
        else:
            return -1

    def sync(self, path):
        def filter_fun(x): return x.endswith('.csv') or x.startswith("_")

        self.drive.sync_files(path, self.fid, remove_local=True, filter_fun=filter_fun)
