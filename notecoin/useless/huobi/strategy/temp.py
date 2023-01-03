import json
import os
import time

import requests
from notetool.tool.secret import SecretManage

secret = SecretManage()
print(secret.select_all())
