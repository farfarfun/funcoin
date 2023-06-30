import json
import os
import time

import requests
from darktool.tool.secret import SecretManage

secret = SecretManage()
print(secret.select_all())
