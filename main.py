from fastapi import FastAPI
from pydantic import BaseModel
from services.email_service import fetch_emails_to_df
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:8502"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class UserInput(BaseModel):
    email: str
    password: str



@app.post("/analyze-emails")
def analyze_emails(user: UserInput):
    result = fetch_emails_to_df(user.email, user.password)
    return {
        "status": "success",
        "data": result
    }
