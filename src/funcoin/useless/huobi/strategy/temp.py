import json
import os
import time

import requests
from funtool.tool.secret import SecretManage

secret = SecretManage()
print(secret.select_all())
