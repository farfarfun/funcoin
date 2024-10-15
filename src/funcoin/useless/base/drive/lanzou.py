import os

from funcoin.base.db import BaseTable, Base
from fundrive import LanZouDrive
from sqlalchemy import BIGINT, String, select, UniqueConstraint, DateTime, func
from sqlalchemy.orm import mapped_column
from tqdm import tqdm


class FunCoinLanZou(Base):
    __tablename__ = "funcoin_lanzou"
    __table_args__ = (UniqueConstraint("fid"),)

    fid = mapped_column(BIGINT, comment="fid", primary_key=True)
    gmt_create = mapped_column(DateTime(timezone=True), server_default=func.now())
    gmt_modified = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    isfile = mapped_column(BIGINT, comment="是否文件", default="1")
    name = mapped_column(String(100), comment="名称", default="")
    desc = mapped_column(String(500), comment="描述", default="0")
    path = mapped_column(String(500), comment="路径", default="")
    downs = mapped_column(BIGINT, comment="下载次数", default="0")
    url = mapped_column(String(100), comment="分享链接", default="0")
    pwd = mapped_column(String(50), comment="提取码", default="0")


class LanzouDirectory(BaseTable):
    def __init__(self, fid=5679873, *args, **kwargs):
        self.fid = fid
        super(LanzouDirectory, self).__init__(table=FunCoinLanZou, *args, **kwargs)
        self.drive = LanZouDrive()
        self.drive.login()
        self.drive.ignore_limit()

    def scan_all_file(self, clear=False):
        if clear:
            self.delete_all()
        self._scan_all_file(self.fid, "funcoin")

    def _scan_all_file(self, fid, path):
        for _dir in self.drive.get_dir_list(fid):
            _path = f"{path}/{_dir['name']}"
            if not self.file_exist(_path):
                share = self.drive.get_file_info(fid=_dir["fid"], is_file=False)
                data = {
                    "fid": _dir["fid"],
                    "name": _dir["name"],
                    "path": _path,
                    "isfile": 0,
                    "desc": share["desc"],
                    "url": share["url"],
                    "pwd": share["pwd"],
                }
                self.upsert(values=data)

            if _dir["name"].endswith(".tar"):
                continue
            self._scan_all_file(fid=_dir["fid"], path=_path)

        for _file in tqdm(
            self.drive.get_file_list(fid), desc=f"fid={fid}-{os.path.basename(path)}"
        ):
            _path = f"{path}/{_file['name']}"

            if not self.file_exist(_path):
                share = self.drive.get_file_info(fid=_file["fid"], is_file=True)
                data = {
                    "fid": _file["fid"],
                    "name": _file["name"],
                    "path": _path,
                    "isfile": 1,
                    "downs": _file["downs"],
                    "desc": share["desc"],
                    "url": share["url"],
                    "pwd": share["pwd"],
                }
                self.upsert(values=data)

    def file_exist(self, path):
        s = select(self.table).where(FunCoinLanZou.path == path)
        with self.engine.connect() as conn:
            data = [line for line in conn.execute(s)]
            return len(data) == 1

    def file_fid(self, path):
        s = select(self.table).where(FunCoinLanZou.path == path)
        with self.engine.connect() as conn:
            data = [line for line in conn.execute(s)]
            if len(data) == 1:
                return data[0]["fid"]
            else:
                return -1

    def sync(self, path, remove_local=True, *args, **kwargs):
        def filter_fun(x):
            return x.endswith(".csv") or x.startswith("_")

        self.drive.upload_dir(
            path, self.fid, remove_local=remove_local, filter_fun=filter_fun
        )
