<<<<<<< HEAD
import os,sys
import json
import logging
import requests
from fastapi import FastAPI, APIRouter,Request, Query, HTTPException,status,BackgroundTasks
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from db.nosqlsrv import Azure_cosmos
from db.sqlsrv import Azure_mssql
from data.log import logger_main_msg
from data.request_data import WhatsappConfigRequest_data
from whatsapp_utilsFAPI.utils.utils import (
    webhook_check
)
# ===================== Load Environment =====================
load_dotenv()
ms_server=os.getenv('server')
ms_database=os.getenv('database')
ms_username=os.getenv('dbuser')
ms_password=os.getenv('password')
ms_driver=os.getenv('driver')
whatapp_hook_url=os.getenv('whatapp_hook_url')

print("##########  ms_username  ",ms_username)
whatsapp_config_g={}
nosql_config_g={}
sql_client=None
cosmos_con=None
class connection_error(Exception):
    pass
#============================ check DB =========================
def update_wapp_config(new_conf: dict):
    whatsapp_config_g.clear()
    whatsapp_config_g.update(new_conf)

def update_nosql_config(new_conf: dict):
    nosql_config_g.clear()
    nosql_config_g.update(new_conf)

def get_config():
    global whatsapp_config_g
    global nosql_config_g
    global sql_client
    global cosmos_con
    try:
        sql_client = Azure_mssql(ms_server, ms_database, ms_username, ms_password, ms_driver)
        result = sql_client.connect()
        print("--2--",result)
        if result["status"] ==False:
            logging.error(f"❌ SQL Connection failed: {result['comment']}")
            raise connection_error(f"❌ MSSQL Connection failed: {result['comment']}")
        else:
            try:
                config=sql_client.get_config("""SELECT 
                                                id,
                                                opdate,
                                                whatapp_config,
                                                nosql_config
                                            FROM [Globby-REV-DEV-DB].[AI_SRV].[whasapp_config]
                                            WHERE opdate = (
                                                SELECT MAX(opdate)
                                                FROM [Globby-REV-DEV-DB].[AI_SRV].[whasapp_config])
                                                                """)
                print("##2## config ",config)
                if config["status"] ==False:
                    raise connection_error(f"❌ nosql Connection failed: {config['comment']}")
                else:
                    config['data']['nosql_config'] = json.loads(config['data']['nosql_config'])
                    config['data']['whatapp_config'] = json.loads(config['data']['whatapp_config'])    
                    cosmos_conf=config['data']['nosql_config']
                    update_nosql_config(config['data']['nosql_config'])
                    whatsapp_config_g.clear()
                    whatsapp_config_g.update(config['data']['whatapp_config'])
                    cosmos_con=Azure_cosmos(url=cosmos_conf['COSMOS_URI'],database=cosmos_conf['DATABASE_NAME'],
                                            container=cosmos_conf['CONTAINER_NAME'])
                    logging.info(f" Get NOSQL config successfully")
                    return sql_client,cosmos_con
            except Exception as e:
                logging.error(f"❌Exception : get configuration from NOSQL -- {str(e)}")
                raise connection_error(f"❌Exception : get configuration from MSSQL -- {str(e)}")
    except Exception as e:
        logging.error(f"❌Exception : get configuration from MSSQL -- {str(e)}")
        raise connection_error(f"❌Exception : get configuration from MSSQL -- {str(e)}")

get_config()
logger_main_msg("finishing check DB and configuration")
                

# ---------------------- FastAPI App ----------------------
app = FastAPI(title="WhatsApp  API", version="1.0")
executor = ThreadPoolExecutor(max_workers=4)
@app.get("/health")
async def health():
    return {"status": "ok"}
@app.get("/wapp_config", response_class=JSONResponse)
async def get_whatsapp_config():
    return whatsapp_config_g

@app.post("/wapp_config")
async def update_whatsapp_config(req: WhatsappConfigRequest_data):
    try:
        new_data= req.dict()
        print("###req  ",new_data)
        new_data['whatapp_hook_url']=whatapp_hook_url
        x=sql_client.set_wapp_config(new_data,nosql_config_g)
        if x['status']==True:
            update_nosql_config(nosql_config_g)
            update_wapp_config(x['data'])
            return {"status": "updated", "data": new_data}
        else:
            raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Internal server error: {x['comment']}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/webhook")
async def verify_webhook(hub_mode: str = Query(None, alias="hub.mode"),
        hub_verify_token: str = Query(None, alias="hub.verify_token"),
        hub_challenge: str = Query(None, alias="hub.challenge")):
    """
    WhatsApp webhook verification endpoint.
    """
    try:
        VERIFY_TOKEN =whatsapp_config_g['whatsapp_verykey']
        print(f"Webhook verification: mode={hub_mode}, token={hub_verify_token},{hub_challenge}, {whatsapp_config_g['whatsapp_verykey']}")
        logging.info(f"Webhook verification: mode={hub_mode}, token={hub_verify_token}")
        return webhook_check(hub_challenge, hub_verify_token, whatsapp_config_g['whatsapp_verykey'])
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}")