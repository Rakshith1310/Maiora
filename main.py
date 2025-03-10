from fastapi import FastAPI, HTTPException
import httpx
import uvicorn
from sqlalchemy import create_engine, Column, Integer, String, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# Database setup
DATABASE_URL = "sqlite:///./jokes.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


# Joke Table Model
class Joke(Base):
    __tablename__ = "jokes"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    category = Column(String, index=True)
    type = Column(String)
    joke = Column(String, nullable=True)
    setup = Column(String, nullable=True)
    delivery = Column(String, nullable=True)
    nsfw = Column(Boolean)
    political = Column(Boolean)
    sexist = Column(Boolean)
    safe = Column(Boolean)
    lang = Column(String)


# Create the table
Base.metadata.create_all(bind=engine)

JOKE_API_URL = "https://sv443.net/jokeapi/v2/joke/Any?amount=100"


@app.get("/fetch_jokes")
def fetch_jokes():
    try:
        response = httpx.get(JOKE_API_URL, timeout=10)
        if response.status_code != 200:
            raise HTTPException(
                status_code=500, detail="Failed to fetch jokes from the API"
            )

        jokes_data = response.json()
        processed_jokes = []
        db = SessionLocal()

        while len(processed_jokes) < 100:
            for joke in jokes_data.get("jokes", []):
                joke_entry = Joke(
                    category=joke.get("category"),
                    type=joke.get("type"),
                    joke=joke.get("joke") if joke.get("type") == "single" else None,
                    setup=joke.get("setup") if joke.get("type") == "twopart" else None,
                    delivery=(
                        joke.get("delivery") if joke.get("type") == "twopart" else None
                    ),
                    nsfw=joke.get("flags", {}).get("nsfw"),
                    political=joke.get("flags", {}).get("political"),
                    sexist=joke.get("flags", {}).get("sexist"),
                    safe=joke.get("safe"),
                    lang=joke.get("lang"),
                )
                db.add(joke_entry)
                processed_jokes.append(joke_entry)
                if len(processed_jokes) >= 100:
                    break

        db.commit()
        db.close()

        return {"message": "100 jokes stored successfully in SQLite!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, reload=True, access_log=False)
