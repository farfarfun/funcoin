import json

import pandas as pd
from funcoin.huobi.utils.etf_result import etf_result_check
from funcoin.huobi.utils.huobi_api_exception import HuobiApiException
from funcoin.huobi.utils.print_mix_object import TypeCheck


def check_response(dict_data):
    status = dict_data.get("status", None)
    code = dict_data.get("code", None)
    success = dict_data.get("success", None)
    if status and len(status):
        if TypeCheck.is_basic(status):  # for normal case
            if status == "error":
                err_code = dict_data.get("err-code", 0)
                err_msg = dict_data.get("err-msg", "")
                raise HuobiApiException(HuobiApiException.EXEC_ERROR,
                                        "[Executing] " + str(err_code) + ": " + err_msg)
            elif status != "ok":
                raise HuobiApiException(HuobiApiException.RUNTIME_ERROR,
                                        "[Invoking] Response is not expected: " + status)
        # for https://status.huobigroup.com/api/v2/summary.json in example example/generic/get_system_status.py
        elif TypeCheck.is_dict(status):
            if dict_data.get("page") and dict_data.get("components"):
                pass
            else:
                raise HuobiApiException(HuobiApiException.EXEC_ERROR, "[Executing] System is in maintenances")
    elif code:
        code_int = int(code)
        if code_int != 200:
            err_code = dict_data.get("code", 0)
            err_msg = dict_data.get("message", "")
            raise HuobiApiException(HuobiApiException.EXEC_ERROR,
                                    "[Executing] " + str(err_code) + ": " + err_msg)
    elif success is not None:
        if bool(success) is False:
            err_code = etf_result_check(dict_data.get("code"))
            err_msg = dict_data.get("message", "")
            if err_code == "":
                raise HuobiApiException(HuobiApiException.EXEC_ERROR, "[Executing] " + err_msg)
            else:
                raise HuobiApiException(HuobiApiException.EXEC_ERROR, "[Executing] " + str(err_code) + ": " + err_msg)
    else:
        raise HuobiApiException(HuobiApiException.RUNTIME_ERROR, "[Invoking] Status cannot be found in response.")


class Response:
    def __init__(self, dict_data=None, response=None, code=None, data=None):
        if response is not None:
            try:
                dict_data = json.loads(response, encoding="utf-8")
                check_response(dict_data)
            except Exception as e:
                print(f"{e}:{response}")
        

        dict_data = dict_data or {}
        self.dict_data = dict_data

        self.code = dict_data.get('code', -1)
        self.data = dict_data.get('data', [])
        if self.data is not None and len(self.data) > 0:
            try:
                self.data_df = pd.DataFrame(self.data)
            except Exception as e:
                print(f"{e}:{response}")

    def __str__(self):
        return self.code
