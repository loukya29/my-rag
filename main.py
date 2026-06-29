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
from data_loader import *
from vecto_db import QdrantStorage
from custom_types import *

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
    def _load(ctx:inngest.Context) -> RAGChunkAndSrc:
        pdf_path = ctx.event.data["pdf_path"]
        source_id = ctx.event.data.get("source_id",pdf_path)
        chunks = load_and_chunk_pdf(pdf_path)
        return RAGChunkAndSrc(chunk=chunks, source_id=source_id)

    def _upsert(chunks_and_src:RAGChunkAndSrc) -> RAGUpsertResult:
        chunks = chunks_and_src.chunk
        source_id = chunks_and_src.source_id
        vecs = embed_texts(chunks)
        ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}:{i}")) for i in range(len(chunks))]
        payload = [{"source":source_id,"text": chunks[i]} for i in range(len(chunks))]
        QdrantStorage().upsert(ids, vecs, payload )
        return RAGUpsertResult(ingested=len(chunks))

    chunks_and_src = await ctx.step.run("load-and-chunk", lambda :_load(ctx), output_type= RAGChunkAndSrc)
    ingested = await ctx.step.run("embed-and-upsert", lambda :_upsert(chunks_and_src), output_type= RAGUpsertResult)
    # this is how we call our steps and make use of it
    return ingested.model_dump()

#this is to query for the content of pdf

@inngest_client.create_function(
    fn_id="RAG: query PDF",
    trigger=inngest.TriggerEvent(event="rag/query_pdf_ai")
)
async def rag_query_pdf_ai(ctx: inngest.Context):
    def _search(question: str,top_k: int = 5) -> RAGSearchResult:
        query_vec = embed_texts([question])[0] #going to pass a list of questions and i am going to pull out the first on
        store = QdrantStorage()
        found = store.search(query_vec, top_k)
        return RAGSearchResult(contexts = found["contexts"], sources = found["sources"])

    question = ctx.event.data["question"]
    top_k = int(ctx.event.data.get("top_k",5))


    found = await ctx.step.run("embed-and-search", lambda :_search(question,top_k), output_type= RAGSearchResult)

# this is the query that i will be dynamically using to communicate with the mentioned model ie gpt-4o-mini in this case
    context_block = "\n\n".join(f"- {c}" for c in found.contexts)
    user_content = (
        "Use the following context to answer the question.\n\n"
        f"Context: \n{context_block}\n\n"
        f"Question: {question}\n"
        "Answer concisely using the context above."
    )

# this is the openai adapter that i will be using
    adapter = ai.openai.Adapter(
        auth_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini"
    )

    res = await ctx.step.ai.infer(
        "llm-answer",
        adapter=adapter,
        body= {
            "max_tokens": 1024,
            "temperature": 0.2,
            "messages": [
                {"role":"system","content":"You answer questions using only on the provided context."},
                {"role":"user","content":user_content}
            ]
        }
    )

    answer = res["choices"][0]["message"]["content"].strip()
    # return {"answer":answer,"source":found.sources,"num_contexts":len(found.contexts)}
    return {"answer": answer, "source": list(found.sources), "num_contexts": len(found.contexts)}

#we have 2 individual steps (ie. load and upsert) inside the rag inngest pdf function


app = FastAPI()

inngest.fast_api.serve(app,inngest_client,[rag_ingest_pdf,rag_query_pdf_ai])