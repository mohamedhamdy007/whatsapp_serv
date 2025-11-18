from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import logging
import pandas as pd
import json
from utils import random_code
class Azure_mssql():
    def __init__(self,server,database,username,password,driver):
        self.server=server
        self.database=database
        self.username=username
        self.password=password
        self.driver=driver
        self.connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=no")
        self.engine=create_engine(self.connection_string)

    def connect(self):
        try:
            engine = create_engine(self.connection_string)
            connect_ = self.engine.connect()
            result =  connect_.execute(text("SELECT 1 AS dummy"))
            if result.fetchone()[0]==1:
                self.connect_=connect_
                self.engine=engine
                logging.info("succssfully login")
                return  {"status":True,"comment":f"MSSQL succssfully login"}
            else:
                logging.info(f"MSSQL Fail in login")
                return  {"status":False,"comment":f"MSSQL Fail in login"}
        except Exception as e:
            logging.info(f"Exception - connect : {str(e)}")
            return  {"status":False,"comment":f"Exception - connect : {str(e)}"}
    def get_config(self,config_sql):
        try:
            df = pd.read_sql(config_sql, self.engine)
            
            if len(df)==1:
                df=df.to_dict(orient='records')[0]
                print("### df ==> ",df)           
                return  {"status":True,'data':df,"comment":"Succssefully connect to databse" }
            else:
                logging.info(f"Exception - get_config: mulriple configuration founds")
                return  {"status":False,"comment":f" get_config: mulriple configuration founds"}
        except Exception as e:
            logging.info(f"Exception - get_config: {str(e)}")
            return  {"status":False,"comment":f"Exception - get_config: {str(e)}"}
        
    def set_wapp_config(self,whatapp_json,nosql_json):
        try:
            # Convert dict to JSON string
            whatapp_json['whatsapp_verykey']=random_code()
            whatapp_json_ = json.dumps(whatapp_json)
            nosql_json = json.dumps(nosql_json)

            # Insert query (do NOT provide id or opdate)
            insert_sql = text(f"""
                INSERT INTO [Globby-REV-DEV-DB].AI_SRV.whasapp_config 
                (whatapp_config,nosql_config)
                VALUES (:whatapp_config,:nosql_config)
            """)

            with self.engine.begin() as conn:  # ensures commit/rollback automatically
                print("--1-in--")
                conn.execute(insert_sql, {"whatapp_config": whatapp_json_,"nosql_config":nosql_json})
                conn.commit()
                print("--2-in--")

            logging.info("Configuration inserted successfully.")
            return {"status": True,'data':whatapp_json, "comment": "Configuration inserted successfully."}

        except Exception as e:
            logging.error(f"Exception - set_config: {str(e)}")
            return {"status": False, "comment": f"Exception - set_config: {str(e)}"}



    