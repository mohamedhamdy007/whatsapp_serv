from pydantic import BaseModel
from typing import Optional, Dict

class WhatsappConfigRequest_data(BaseModel):
    whatsapp_phone: int
    whatsapp_phone_ID: int
    whatapp_token: str
