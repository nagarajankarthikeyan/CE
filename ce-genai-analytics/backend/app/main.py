from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.routers.chat_stream import router as chat_stream_router
from app.auth import get_current_user

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/auth/check")
def auth_check(user: str = Depends(get_current_user)):
    return {"status": "ok", "user": user}

app.include_router(chat_stream_router)
