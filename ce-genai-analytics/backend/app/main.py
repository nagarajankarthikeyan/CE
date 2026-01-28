from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.chat_stream import router as chat_stream_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_stream_router)
