"""Environment Variables
"""
import os
from dotenv import load_dotenv

load_dotenv()


HIC_DB_USERNAME = os.environ["HIC_DB_USERNAME"]
HIC_DB_PASSWORD = os.environ["HIC_DB_PASSWORD"]
HIC_DB_HOST = os.environ["HIC_DB_HOST"]
HIC_DB_DATABASE = os.environ["HIC_DB_DATABASE"]

MS_SQL_ODBC_DRIVER = os.environ["MS_SQL_ODBC_DRIVER"]
MS_SQL_UHL_DWH_HOST = os.environ["MS_SQL_UHL_DWH_HOST"]
MS_SQL_UHL_DWH_USER = os.environ["MS_SQL_UHL_DWH_USER"]
MS_SQL_UHL_DWH_PASSWORD = os.environ["MS_SQL_UHL_DWH_PASSWORD"]

IDENTITY_API_KEY = os.environ["IDENTITY_API_KEY"]
IDENTITY_HOST = os.environ["IDENTITY_HOST"]
