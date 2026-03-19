from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
import hashlib
import csv
import os
from dotenv import load_dotenv
load_dotenv()

# ─── Database Helpers ─────────────────────────────────────────
def get_db():
    """Return a PostgreSQL connection with dict-style row access."""
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def hash_password(plain: str) -> str:
    """SHA-256 hash a plain-text password."""
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


# ─── Database Initialization ─────────────────────────────────
def init_db():
    """Create tables based on the provided relational schema."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Users (
                    email       TEXT PRIMARY KEY,
                    password    TEXT NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS Helpdesk (
                    email       TEXT PRIMARY KEY,
                    position    TEXT,
                    FOREIGN KEY (email) REFERENCES Users(email)
                );
            """)

            # TODO: Add other parts of the schema here once necessary

        conn.commit()


# ─── CSV Data Population ─────────────────────────────────────
def populate_from_csv():
    """
    Load CSV files from the NittanyAuctionDataset_v1/ directory into the database.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            # Skip if already populated
            cur.execute("SELECT COUNT(*) AS count FROM Users")
            count = cur.fetchone()["count"]
            if count > 0:
                return

            data_dir = "NittanyAuctionDataset_v1"

            # --- Users (hash passwords!) ---
            users_file = os.path.join(data_dir, "Users.csv")
            if os.path.exists(users_file):
                with open(users_file, newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        cur.execute(
                            """
                            INSERT INTO Users (email, password)
                            VALUES (%s, %s)
                            ON CONFLICT (email) DO NOTHING
                            """,
                            (row["email"].strip(), hash_password(row["password"].strip())),
                        )

            # --- Helpdesk ---
            helpdesk_file = os.path.join(data_dir, "Helpdesk.csv")
            if os.path.exists(helpdesk_file):
                with open(helpdesk_file, newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        cur.execute(
                            """
                            INSERT INTO Helpdesk (email, position)
                            SELECT %s, %s
                            WHERE EXISTS (SELECT 1 FROM Users WHERE email = %s) 
                            ON CONFLICT (email) DO NOTHING
                            """, # There are some emails in Helpdesk.csv that don't exist in Users.csv
                            (row["email"].strip(), row.get("Position").strip(), row["email"].strip()),
                        )

            # TODO: Populate other tables with their respective CSV files once tables are created
            # TODO: Make a function to do this to avoid repeated code blocks

        conn.commit()
        print("✅ Database populated from CSV files.")


# ─── Startup Event ────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    init_db()
    populate_from_csv()

    yield

    # TODO: shutdown logic here


# ─── Helper: Determine User Role ─────────────────────────────
def get_user_role(email: str, conn) -> str:
    """Return 'helpdesk', 'seller', or 'bidder' based on which tables the user exists in."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM Helpdesk WHERE email = %s", (email,))
        if cur.fetchone():
            return "helpdesk"

        '''
        cur.execute("SELECT 1 FROM Sellers WHERE email = %s", (email,))
        if cur.fetchone():
            return "seller"

        cur.execute("SELECT 1 FROM Bidders WHERE email = %s", (email,))
        if cur.fetchone():
            return "bidder"
        '''

    return "unknown"


# ─── App Setup ────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=os.getenv('MASTER_KEY'))
DATABASE_URL = f"postgresql://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@localhost:5432/NittanyAuction"


# ─── Routes ───────────────────────────────────────────────────
@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, email: str = Form(...), password: str = Form(...)):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Users WHERE email = %s", (email,))
            user = cur.fetchone()

            if not user or user["password"] != hash_password(password):
                return HTMLResponse(content="Invalid credentials", status_code=401)

            role = get_user_role(email, conn)

    request.session["email"] = email
    request.session["role"] = role

    return RedirectResponse(url=f"/dashboard/{role}", status_code=303) # TODO: Add dashboard endpoint


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")
