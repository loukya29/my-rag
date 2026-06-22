import logging
from decimal import Context

from fastapi import FastAPI
import inngest
from inngest import fast_api
from dotenv import load_dotenv
import os
import datetime
import uuid
from inngest.experimental import ai

load_dotenv()

inngest_client = inngest.Inngest(
    app_id="rag_01",
    logger=logging.getLogger("uvicorn"),
    is_production=False,
    serializer=inngest.PydanticSerializer()
)

@inngest_client.create_function(
    fn_id="RAG: Inngest PDF",
    trigger=inngest.TriggerEvent(event="rag/ingest_pdf")
)
async def rag_ingest_pdf(ctx:inngest.Context):
    return {"hello"}


app = FastAPI()

inngest.fast_api.serve(app,inngest_client,[rag_ingest_pdf])