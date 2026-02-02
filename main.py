import os
import csv
import io
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, List, Literal, Dict, Any
from uuid import UUID, uuid4

from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer
from fastapi import Query

from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel, Field

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB

from dotenv import load_dotenv
import json


load_dotenv()
# ---------------------------
# Config
# ---------------------------
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")
JWT_SECRET = os.getenv("JWT_SECRET", "CHANGE_ME")
JWT_ALG = "HS256"
TOKEN_MINUTES = 60 * 24 * 7

pwd = CryptContext(schemes=["argon2"], deprecated="auto")

ROLE = Literal["admin", "supervisor"]
STATUS = Literal["pending", "approved", "rejected"]
FREQ = Literal["weekly", "biweekly", "monthly"]

DAILY_TARGET_KG_EQUIV = Decimal("1.0")
METRES_PER_KG_EQUIV = Decimal("60.0")  # your rubric target basis

engine: Engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

    

def bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return auth.split(" ", 1)[1].strip()

def current_user(request: Request):
    token = bearer_token(request)
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        user_id = UUID(data["sub"])
        role = data["role"]
        return {"id": user_id, "role": role}
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
        

def get_current_user(token: str = Depends(oauth2_scheme)):
    """
    Returns a dict like:
      {"id": "<uuid>", "email": "...", "role": "admin|supervisor", "factory_id": "<uuid or None>"}
    """
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    # optional exp check (jose validates exp automatically if present, but keep safe)
    exp = payload.get("exp")
    if exp is not None:
        if datetime.fromtimestamp(exp, tz=timezone.utc) < datetime.now(tz=timezone.utc):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")

    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("role")
    factory_id = payload.get("factory_id")

    if not user_id or not email or role not in ("admin", "supervisor"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token claims")

    return {
        "id": user_id,
        "email": email,
        "role": role,
        "factory_id": factory_id,
    }

def require_admin(u=Depends(get_current_user)):
    if u["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return u

def require_supervisor_or_admin(u=Depends(get_current_user)):
    if u["role"] not in ("admin", "supervisor"):
        raise HTTPException(status_code=403, detail="Forbidden")
    return u

def require_admin(u=Depends(current_user)):
    if u["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return u


# ---------------------------
# App
# ---------------------------
app = FastAPI(title="Banana Fibre Production (Admin/Supervisor)")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")
    
@app.get("/api/healthz")
def healthz():
    return {"ok": True}


@app.get("/api/me")
def me_endpoint(u=Depends(current_user)):
    with engine.begin() as conn:
        row = conn.execute(
            text("select email, role from app_users where id=:id"),
            {"id": str(u["id"])},
        ).fetchone()
        if not row:
            raise HTTPException(401, "User not found")
    return {"email": row[0], "role": row[1]}

@app.get("/api/task-types", dependencies=[Depends(current_user)])
def task_types_endpoint():
    with engine.begin() as conn:
        rows = conn.execute(
            text("select id, code, name, unit, default_rate_ngn from task_types order by name asc")
        ).fetchall()
    return [
        {"id": r[0], "code": r[1], "name": r[2], "unit": r[3], "default_rate_ngn": float(r[4])}
        for r in rows
    ]



# ---------------------------
# Helpers
# ---------------------------
def db():
    if not SUPABASE_DB_URL:
        raise RuntimeError("SUPABASE_DB_URL not set")
    return engine

def create_token(user_id: UUID, role: ROLE) -> str:
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_MINUTES)
    payload = {"sub": str(user_id), "role": role, "exp": exp}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing bearer token")
    return auth.split(" ", 1)[1].strip()

def current_user(request: Request) -> Dict[str, Any]:
    token = bearer_token(request)
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return {"id": UUID(data["sub"]), "role": data["role"]}
    except (JWTError, KeyError, ValueError):
        raise HTTPException(401, "Invalid token")

def require_admin(u=Depends(current_user)):
    if u["role"] != "admin":
        raise HTTPException(403, "Admin only")
    return u

def rubric_from_logged(combed_kg: Decimal, woven_m: Decimal) -> Dict[str, Any]:
    progress = combed_kg + (woven_m / METRES_PER_KG_EQUIV)
    target_met = progress >= DAILY_TARGET_KG_EQUIV

    weaving_needed = Decimal("0")
    combing_needed = Decimal("0")

    if combed_kg < Decimal("1"):
        weaving_needed = (Decimal("1") - combed_kg) * METRES_PER_KG_EQUIV
        if weaving_needed < 0:
            weaving_needed = Decimal("0")

    if woven_m < METRES_PER_KG_EQUIV:
        combing_needed = (METRES_PER_KG_EQUIV - woven_m) / METRES_PER_KG_EQUIV
        if combing_needed < 0:
            combing_needed = Decimal("0")

    return {
        "progress_kg_equiv": float(progress),
        "target_met": bool(target_met),
        "weaving_needed_m": float(weaving_needed),
        "combing_needed_kg": float(combing_needed),
    }

def period_for_worker(freq: FREQ, anchor: date, as_of: date) -> tuple[date, date]:
    """
    Stable cycles:
    - weekly: 7-day blocks from anchor
    - biweekly: 14-day blocks from anchor
    - monthly: calendar month
    """
    if freq == "monthly":
        start = as_of.replace(day=1)
        return start, as_of

    block = 7 if freq == "weekly" else 14
    delta_days = (as_of - anchor).days
    if delta_days < 0:
        # if as_of before anchor, clamp to anchor period
        start = anchor
        end = min(as_of, anchor + timedelta(days=block - 1))
        return start, end

    block_index = delta_days // block
    start = anchor + timedelta(days=block_index * block)
    end = start + timedelta(days=block - 1)
    if end > as_of:
        end = as_of
    return start, end

def effective_rate(conn, worker_id: UUID, task_type_id: UUID) -> Decimal:
    # worker override first
    r = conn.execute(
        text("""
          select rate_ngn from worker_rates
          where worker_id = :wid and task_type_id = :tid
        """),
        {"wid": str(worker_id), "tid": str(task_type_id)}
    ).fetchone()
    if r:
        return Decimal(str(r[0]))

    d = conn.execute(
        text("select default_rate_ngn from task_types where id = :tid"),
        {"tid": str(task_type_id)}
    ).fetchone()
    return Decimal(str(d[0] if d else 0))


def audit(conn, actor_id, actor_role, action, entity_type, entity_id, metadata: Optional[Dict[str, Any]] = None):

    meta = metadata or {}

    stmt = text("""
        insert into audit_logs (actor_id, actor_role, action, entity_type, entity_id, metadata)
        values (:actor_id, :actor_role, :action, :entity_type, :entity_id, :meta)
    """).bindparams(
        bindparam("meta", type_=JSONB)
    )

    conn.execute(stmt, {
        "actor_id": str(actor_id),
        "actor_role": actor_role,
        "action": action,
        "entity_type": entity_type,
        "entity_id": str(entity_id) if entity_id else None,
        "meta": meta,
    })


def compute_period(payout: str, anchor: date, as_of: date):
    """
    Returns (period_start, period_end) inclusive dates for the period containing as_of.
    payout: weekly|biweekly|monthly
    anchor: anchor date (defines boundaries)
    """
    if payout in ("weekly", "biweekly"):
        days = 7 if payout == "weekly" else 14
        delta = (as_of - anchor).days
        if delta < 0:
            # move backwards in blocks
            blocks = (abs(delta) + days - 1) // days
            start = anchor - timedelta(days=blocks * days)
        else:
            blocks = delta // days
            start = anchor + timedelta(days=blocks * days)
        end = start + timedelta(days=days - 1)
        return start, end

    # monthly: anchor day-of-month defines boundary
    # we interpret "period start" as anchor day in a month, and period end day before next anchor.
    anchor_dom = anchor.day

    def add_month(d: date):
        y = d.year + (d.month // 12)
        m = (d.month % 12) + 1
        # clamp day
        import calendar
        last = calendar.monthrange(y, m)[1]
        return date(y, m, min(d.day, last))

    # find most recent anchor date <= as_of
    # start at same month as as_of, on anchor_dom (clamped)
    import calendar
    last = calendar.monthrange(as_of.year, as_of.month)[1]
    cand = date(as_of.year, as_of.month, min(anchor_dom, last))
    if cand > as_of:
        # move to previous month
        y = as_of.year if as_of.month > 1 else as_of.year - 1
        m = as_of.month - 1 if as_of.month > 1 else 12
        last2 = calendar.monthrange(y, m)[1]
        cand = date(y, m, min(anchor_dom, last2))

    start = cand
    next_anchor = add_month(start)
    end = next_anchor - timedelta(days=1)
    return start, end


# ---------------------------
# Schemas
# ---------------------------
class LoginIn(BaseModel):
    email: str
    password: str

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

class CreateAppUserIn(BaseModel):
    email: str
    password: str
    role: ROLE = "supervisor"
    factory_id: Optional[UUID] = None

class CreateWorkerIn(BaseModel):
    worker_code: Optional[str] = None
    full_name: str
    factory_id: Optional[UUID] = None
    team_id: Optional[UUID] = None
    payout: FREQ = "weekly"
    payout_anchor_date: date = Field(default_factory=date.today)

class WorkerOut(BaseModel):
    id: UUID
    worker_code: Optional[str]
    full_name: str
    payout: FREQ
    payout_anchor_date: date
    is_active: bool

class WorkDayCreateIn(BaseModel):
    worker_id: UUID
    work_date: date
    workstation_id: Optional[UUID] = None
    day_note: Optional[str] = None

class WorkTaskCreateIn(BaseModel):
    id: Optional[UUID] = None  # client-generated UUID for offline dedupe
    work_day_id: UUID
    task_type_id: UUID
    quantity: float
    note: Optional[str] = None


class WorkTaskDecisionIn(BaseModel):
    status: Literal["approved", "rejected"]
    decision_reason: Optional[str] = None

class PayrollOut(BaseModel):
    worker_id: UUID
    full_name: str
    payout: FREQ
    period_start: date
    period_end: date
    approved_total_pay_ngn: float
    approved_combed_kg: float
    approved_woven_m: float

class AppUserUpdateIn(BaseModel):
    role: Optional[str] = Field(default=None, pattern="^(admin|supervisor)$")
    factory_id: Optional[UUID] = None
    is_active: Optional[bool] = None

class WorkerUpdateIn(BaseModel):
    worker_code: Optional[str] = None
    full_name: Optional[str] = None
    payout: Optional[str] = Field(default=None, pattern="^(weekly|biweekly|monthly)$")
    payout_anchor_date: Optional[date] = None
    factory_id: Optional[UUID] = None
    team_id: Optional[UUID] = None
    is_active: Optional[bool] = None

# ---------------------------
# Auth endpoints
# ---------------------------
@app.post("/api/auth/login", response_model=TokenOut)
def login(body: LoginIn):
    with engine.begin() as conn:
        row = conn.execute(
            text("""select id, password_hash, role, is_active
                    from app_users where email = :e"""),
            {"e": body.email.lower().strip()},
        ).fetchone()

        if not row or not row[3]:
            raise HTTPException(400, "Invalid credentials")
        if not pwd.verify(body.password, row[1]):
            raise HTTPException(400, "Invalid credentials")

        #token = create_token(UUID(row[0]), row[2])
        token = create_token(row[0] if isinstance(row[0], UUID) else UUID(str(row[0])), row[2])

        return TokenOut(access_token=token)

@app.post("/api/admin/app-users", dependencies=[Depends(require_admin)])
def create_app_user(body: CreateAppUserIn):
    with engine.begin() as conn:
        conn.execute(
            text("""insert into app_users (email, password_hash, role, factory_id)
                    values (:e, :p, :r, :f)"""),
            {
                "e": body.email.lower().strip(),
                "p": pwd.hash(body.password),
                "r": body.role,
                "f": str(body.factory_id) if body.factory_id else None,
            },
        )
    return {"ok": True}
    
@app.get("/api/admin/app-users", dependencies=[Depends(require_admin)])
def list_app_users():
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select id, email, role, factory_id, is_active, created_at
          from app_users
          order by created_at desc
        """)).fetchall()
    return [{
        "id": r[0],
        "email": r[1],
        "role": r[2],
        "factory_id": r[3],
        "is_active": bool(r[4]),
        "created_at": r[5],
    } for r in rows]


@app.patch("/api/admin/app-users/{user_id}", dependencies=[Depends(require_admin)])
def update_app_user(user_id: UUID, body: AppUserUpdateIn, u=Depends(get_current_user)):
    payload = body.model_dump(exclude_unset=True)

    if not payload:
        return {"ok": True}

    sets = []
    params = {"id": str(user_id)}

    if "role" in payload and payload["role"] is not None:
        sets.append("role = :role")
        params["role"] = payload["role"]

    if "factory_id" in payload:
        sets.append("factory_id = :fid")
        params["fid"] = str(payload["factory_id"]) if payload["factory_id"] else None

    if "is_active" in payload and payload["is_active"] is not None:
        sets.append("is_active = :act")
        params["act"] = bool(payload["is_active"])

    with engine.begin() as conn:
        r = conn.execute(text("select id from app_users where id=:id"), {"id": str(user_id)}).fetchone()
        if not r:
            raise HTTPException(404, "User not found")

        conn.execute(text(f"update app_users set {', '.join(sets)} where id=:id"), params)
        audit(conn, u["id"], u["role"], "APPUSER_UPDATE", "app_user", user_id, payload)

    return {"ok": True}

    
def require_role(allowed: set[str]):
    def _dep(u=Depends(current_user)):
        if u["role"] not in allowed:
            raise HTTPException(403, "Forbidden")
        return u
    return _dep

require_admin_or_supervisor = require_role({"admin", "supervisor"})

def can_decide_task(conn, decider_id: UUID, decider_role: str, task_id: UUID) -> bool:
    """
    Rule:
    - Admin can decide any task
    - Supervisor can decide tasks they logged (i.e., work_day.logged_by == supervisor id)
    """
    if decider_role == "admin":
        return True
    row = conn.execute(
        text("""
          select wd.logged_by
          from work_tasks wt
          join work_days wd on wd.id = wt.work_day_id
          where wt.id = :tid
        """),
        {"tid": str(task_id)},
    ).fetchone()
    if not row:
        return False
    return str(row[0]) == str(decider_id)


# ---------------------------
# Worker management
# ---------------------------
@app.post("/api/workers", dependencies=[Depends(current_user)], response_model=WorkerOut)
def create_worker(body: CreateWorkerIn):
    wid = uuid4()
    with engine.begin() as conn:
        conn.execute(
            text("""insert into workers
              (id, worker_code, full_name, factory_id, team_id, payout, payout_anchor_date)
              values (:id, :code, :name, :factory, :team, :payout, :anchor)"""),
            {
                "id": str(wid),
                "code": body.worker_code,
                "name": body.full_name,
                "factory": str(body.factory_id) if body.factory_id else None,
                "team": str(body.team_id) if body.team_id else None,
                "payout": body.payout,
                "anchor": body.payout_anchor_date,
            },
        )
        row = conn.execute(
            text("""select id, worker_code, full_name, payout, payout_anchor_date, is_active
                    from workers where id = :id"""),
            {"id": str(wid)},
        ).fetchone()

    return WorkerOut(
        id=UUID(str(row[0])),
        worker_code=row[1],
        full_name=row[2],
        payout=row[3],
        payout_anchor_date=row[4],
        is_active=row[5],
    )

@app.get("/api/admin/workers", dependencies=[Depends(require_admin)])
def admin_list_workers(include_inactive: bool = Query(default=True)):
    where = "" if include_inactive else " where is_active = true "
    with engine.begin() as conn:
        rows = conn.execute(text(f"""
          select id, worker_code, full_name, payout, payout_anchor_date, factory_id, team_id, is_active, created_at
          from workers
          {where}
          order by full_name asc
        """)).fetchall()

    return [{
        "id": r[0],
        "worker_code": r[1],
        "full_name": r[2],
        "payout": r[3],
        "payout_anchor_date": r[4],
        "factory_id": r[5],
        "team_id": r[6],
        "is_active": bool(r[7]),
        "created_at": r[8],
    } for r in rows]

    
@app.patch("/api/workers/{worker_id}", dependencies=[Depends(require_admin)])
def update_worker(worker_id: UUID, body: WorkerUpdateIn, u=Depends(get_current_user)):
    payload = body.model_dump(exclude_unset=True)
    if not payload:
        return {"ok": True}

    sets = []
    params = {"id": str(worker_id)}

    if "worker_code" in payload:
        sets.append("worker_code = :code")
        params["code"] = payload["worker_code"]

    if "full_name" in payload and payload["full_name"] is not None:
        sets.append("full_name = :name")
        params["name"] = payload["full_name"]

    if "payout" in payload and payload["payout"] is not None:
        sets.append("payout = :payout")
        params["payout"] = payload["payout"]

    if "payout_anchor_date" in payload and payload["payout_anchor_date"] is not None:
        sets.append("payout_anchor_date = :anchor")
        params["anchor"] = payload["payout_anchor_date"]

    if "factory_id" in payload:
        sets.append("factory_id = :fid")
        params["fid"] = str(payload["factory_id"]) if payload["factory_id"] else None

    if "team_id" in payload:
        sets.append("team_id = :tid")
        params["tid"] = str(payload["team_id"]) if payload["team_id"] else None

    if "is_active" in payload and payload["is_active"] is not None:
        sets.append("is_active = :act")
        params["act"] = bool(payload["is_active"])

    with engine.begin() as conn:
        r = conn.execute(text("select id from workers where id=:id"), {"id": str(worker_id)}).fetchone()
        if not r:
            raise HTTPException(404, "Worker not found")

        conn.execute(text(f"update workers set {', '.join(sets)} where id=:id"), params)
        audit(conn, u["id"], u["role"], "WORKER_UPDATE", "worker", worker_id, payload)

    return {"ok": True}


@app.get("/api/workers", dependencies=[Depends(current_user)])
def list_workers():
    with engine.begin() as conn:
        rows = conn.execute(
            text("""select id, worker_code, full_name, payout, payout_anchor_date, is_active
                    from workers where is_active = true
                    order by full_name asc""")
        ).fetchall()
    return [
        {
            "id": r[0], "worker_code": r[1], "full_name": r[2],
            "payout": r[3], "payout_anchor_date": r[4], "is_active": r[5]
        }
        for r in rows
    ]

# ---------------------------
# Work logging
# ---------------------------
@app.post("/api/work-days", dependencies=[Depends(current_user)])
def create_work_day(body: WorkDayCreateIn, u=Depends(current_user)):
    with engine.begin() as conn:
        # upsert: one day per worker

        row = conn.execute(
            text("""insert into work_days (worker_id, work_date, logged_by, workstation_id, day_note)
                    values (:wid, :d, :by, :ws, :note)
                    on conflict (worker_id, work_date)
                    do update set workstation_id = excluded.workstation_id, day_note = excluded.day_note
                    returning id"""),
            {
                "wid": str(body.worker_id),
                "d": body.work_date,
                "by": str(u["id"]),
                "ws": str(body.workstation_id) if body.workstation_id else None,
                "note": body.day_note,
            },
        ).fetchone()
        
        audit(conn, u["id"], u["role"], "WORKDAY_UPSERT", "work_day", UUID(str(row[0])), {
	    "worker_id": str(body.worker_id),
	    "work_date": str(body.work_date),
	    "workstation_id": str(body.workstation_id) if body.workstation_id else None,
	    "day_note": body.day_note
})

    return {"work_day_id": str(row[0])}
    
@app.post("/api/work-tasks", dependencies=[Depends(current_user)])
def add_work_task(body: WorkTaskCreateIn, u=Depends(current_user)):
    if body.quantity < 0:
        raise HTTPException(400, "Quantity cannot be negative")

    tid = body.id or uuid4()

    with engine.begin() as conn:
        # Block changes if the day is closed
        wd = conn.execute(
            text("select is_closed from work_days where id=:wd"),
            {"wd": str(body.work_day_id)}
        ).fetchone()
        if not wd:
            raise HTTPException(404, "Work day not found")
        if wd[0]:
            raise HTTPException(400, "Work day is closed")

        # Insert task; ON CONFLICT makes offline replay safe
        conn.execute(
            text("""
              insert into work_tasks (id, work_day_id, task_type_id, quantity, note)
              values (:id, :wd, :tt, :q, :note)
              on conflict (id) do nothing
            """),
            {
                "id": str(tid),
                "wd": str(body.work_day_id),
                "tt": str(body.task_type_id),
                "q": body.quantity,
                "note": body.note,
            },
        )

        audit(conn, u["id"], u["role"], "TASK_CREATE", "work_task", tid, {
            "work_day_id": str(body.work_day_id),
            "task_type_id": str(body.task_type_id),
            "quantity": body.quantity,
            "note": body.note
        })

    return {"task_id": str(tid)}



@app.get("/api/work-days/{worker_id}")
def get_worker_days(worker_id: UUID, start: Optional[date] = None, end: Optional[date] = None, u=Depends(current_user)):
    q = """select wd.id, wd.work_date, wd.day_note, wd.is_closed, wd.closed_at
           from work_days wd
           where wd.worker_id = :wid"""
    params = {"wid": str(worker_id)}
    if start:
        q += " and wd.work_date >= :s"
        params["s"] = start
    if end:
        q += " and wd.work_date <= :e"
        params["e"] = end
    q += " order by wd.work_date desc"

    with engine.begin() as conn:
        days = conn.execute(text(q), params).fetchall()

        out = []
        for d in days:
            tasks = conn.execute(
                text("""
                  select wt.id, tt.code, tt.name, tt.unit, wt.quantity, wt.status, wt.approved_pay_ngn, wt.note,
                         wt.decided_at, wt.decision_reason
                  from work_tasks wt
                  join task_types tt on tt.id = wt.task_type_id
                  where wt.work_day_id = :wd
                  order by wt.created_at asc
                """),
                {"wd": str(d[0])},
            ).fetchall()

            # rubric based on LOGGED totals (show target guidance immediately)
            combed_logged = sum(Decimal(str(t[4])) for t in tasks if t[1] == "COMBING")
            woven_logged = sum(Decimal(str(t[4])) for t in tasks if t[1] == "WEAVING")
            rubric_logged = rubric_from_logged(combed_logged, woven_logged)

            # approved totals for payroll/verification
            combed_appr = sum(Decimal(str(t[4])) for t in tasks if t[1] == "COMBING" and t[5] == "approved")
            woven_appr = sum(Decimal(str(t[4])) for t in tasks if t[1] == "WEAVING" and t[5] == "approved")
            rubric_approved = rubric_from_logged(combed_appr, woven_appr)

            out.append({
                "work_day_id": d[0],
                "work_date": d[1],
                "day_note": d[2],
                "rubric_logged": rubric_logged,
                "rubric_approved": rubric_approved,
                "is_closed": bool(d[3]),
                "closed_at": d[4],
                "tasks": [
                    {
                        "id": t[0], "code": t[1], "name": t[2], "unit": t[3],
                        "quantity": float(t[4]),
                        "status": t[5],
                        "approved_pay_ngn": float(t[6]),
                        "note": t[7],
                        "decided_at": t[8],
                        "decision_reason": t[9],
                    }
                    for t in tasks
                ]
            })
    return out

@app.get("/api/approvals/pending", dependencies=[Depends(require_admin_or_supervisor)])
def pending_tasks(
    worker_id: Optional[UUID] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    u=Depends(current_user),
):
    q = """
      select wt.id, wd.work_date, wd.worker_id, w.full_name,
             tt.code, tt.name, tt.unit,
             wt.quantity, wt.note, wt.status, wt.created_at
      from work_tasks wt
      join work_days wd on wd.id = wt.work_day_id
      join workers w on w.id = wd.worker_id
      join task_types tt on tt.id = wt.task_type_id
      where wt.status = 'pending'
    """
    params = {}
    if worker_id:
        q += " and wd.worker_id = :wid"
        params["wid"] = str(worker_id)
    if start:
        q += " and wd.work_date >= :s"
        params["s"] = start
    if end:
        q += " and wd.work_date <= :e"
        params["e"] = end

    # supervisors only see tasks they logged
    if u["role"] == "supervisor":
        q += " and wd.logged_by = :by"
        params["by"] = str(u["id"])

    q += " order by wd.work_date desc, wt.created_at asc"

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()

    return [{
        "task_id": r[0],
        "work_date": r[1],
        "worker_id": r[2],
        "worker_name": r[3],
        "task_code": r[4],
        "task_name": r[5],
        "unit": r[6],
        "quantity": float(r[7]),
        "note": r[8],
        "status": r[9],
        "created_at": r[10],
    } for r in rows]


@app.post("/api/work-tasks/{task_id}/decide", dependencies=[Depends(require_admin_or_supervisor)])
def decide_task(task_id: UUID, body: WorkTaskDecisionIn, u=Depends(current_user)):
    with engine.begin() as conn:
        assert_workday_open_by_task(conn, task_id)

        # Fetch the task + rate
        row = conn.execute(text("""
            select
                wt.id,
                wt.status,
                wt.quantity,
                wt.paid_run_id,
                tt.rate_ngn_per_unit
            from work_tasks wt
            join task_types tt on tt.id = wt.task_type_id
            where wt.id = :id
        """), {"id": str(task_id)}).mappings().fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Task not found")

        # If task already paid, don't allow decisions/changes
        if row["paid_run_id"] is not None:
            raise HTTPException(status_code=409, detail="Task already paid; cannot change decision")

        # Optional: if already decided, you may want to block changes
        # (You can remove this if you want to allow re-decisions while unpaid.)
        if row["status"] in ("approved", "rejected") and row["status"] != body.status:
            # allow changing decision if you prefer: just delete this block
            pass

        approved_pay = Decimal("0.00")

        if body.status == "approved":
            qty = Decimal(str(row["quantity"] or 0))
            rate = Decimal(str(row["rate_ngn_per_unit"] or 0))
            approved_pay = (qty * rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        conn.execute(
            text("""
                update work_tasks
                set status = :st,
                    decided_by = :by,
                    decided_at = now(),
                    decision_reason = :reason,
                    approved_pay_ngn = :pay
                where id = :id
            """),
            {
                "st": body.status,
                "by": str(u["id"]),
                "reason": body.decision_reason,
                "pay": str(approved_pay),
                "id": str(task_id),
            },
        )

        audit(
            conn,
            u["id"], u["role"],
            "TASK_APPROVE" if body.status == "approved" else "TASK_REJECT",
            "work_task", task_id,
            {"reason": body.decision_reason, "approved_pay_ngn": float(approved_pay)}
        )

    return {"ok": True, "approved_pay_ngn": float(approved_pay)}



class BulkDecisionIn(BaseModel):
    task_ids: List[UUID]
    status: Literal["approved", "rejected"]
    decision_reason: Optional[str] = None

@app.post("/api/work-tasks/bulk-decide", dependencies=[Depends(require_admin_or_supervisor)])
def bulk_decide(body: BulkDecisionIn, u=Depends(current_user)):
    if not body.task_ids:
        return {"ok": True, "updated": 0}

    updated = 0
    with engine.begin() as conn:
        for tid in body.task_ids:
            try:
                assert_workday_open_by_task(conn, tid)
            except HTTPException:
                continue
                
            if not can_decide_task(conn, u["id"], u["role"], tid):
                continue  # skip tasks supervisor isn't allowed to decide

            t = conn.execute(
                text("""select wt.id, wt.task_type_id, wt.quantity, wd.worker_id
                        from work_tasks wt
                        join work_days wd on wd.id = wt.work_day_id
                        where wt.id = :id"""),
                {"id": str(tid)},
            ).fetchone()
            if not t:
                continue

            worker_id = UUID(t[3])
            rate = effective_rate(conn, worker_id, UUID(t[1]))
            qty = Decimal(str(t[2]))

            approved_pay = Decimal("0")
            if body.status == "approved":
                approved_pay = (qty * rate).quantize(Decimal("0.01"))

            conn.execute(
                text("""update work_tasks
                        set status = :st,
                            decided_by = :by,
                            decided_at = now(),
                            decision_reason = :reason,
                            approved_pay_ngn = :pay
                        where id = :id"""),
                {"st": body.status, "by": str(u["id"]), "reason": body.decision_reason,
                 "pay": str(approved_pay), "id": str(tid)},
            )
            updated += 1
            audit(conn, u["id"], u["role"],
		      "TASK_APPROVE" if body.status == "approved" else "TASK_REJECT",
		      "work_task", tid, {"reason": body.decision_reason})


    return {"ok": True, "updated": updated}


# ---------------------------
# Payroll (approved tasks only)
# ---------------------------
@app.get("/api/payroll/{worker_id}", response_model=PayrollOut)
def payroll(worker_id: UUID, as_of: Optional[date] = None, u=Depends(current_user)):
    as_of = as_of or date.today()

    with engine.begin() as conn:
        w = conn.execute(
            text("""select id, full_name, payout, payout_anchor_date
                    from workers where id = :id"""),
            {"id": str(worker_id)},
        ).fetchone()
        if not w:
            raise HTTPException(404, "Worker not found")

        freq = w[2]
        anchor = w[3]
        start, end = period_for_worker(freq, anchor, as_of)

        rows = conn.execute(
            text("""
              select tt.code, wt.quantity, wt.approved_pay_ngn
              from work_tasks wt
              join work_days wd on wd.id = wt.work_day_id
              join task_types tt on tt.id = wt.task_type_id
              where wd.worker_id = :wid
                and wd.work_date between :s and :e
                and wt.status = 'approved'
                and wt.paid_run_id is null
            """),
            {"wid": str(worker_id), "s": start, "e": end},
        ).fetchall()

        total_pay = sum((Decimal(str(r[2] or 0)) for r in rows), Decimal("0"))
        combed = sum((Decimal(str(r[1] or 0)) for r in rows if r[0] == "COMBING"), Decimal("0"))
        woven  = sum((Decimal(str(r[1] or 0)) for r in rows if r[0] == "WEAVING"), Decimal("0"))
        
        return PayrollOut(
	    worker_id=UUID(str(w[0])),
	    full_name=w[1],
	    payout=freq,
	    period_start=start,
	    period_end=end,
	    approved_total_pay_ngn=float(total_pay.quantize(Decimal("0.01"))),
	    approved_combed_kg=float(combed),
	    approved_woven_m=float(woven),
	)


@app.get("/api/payroll/{worker_id}/export.csv")
def payroll_csv(worker_id: UUID, as_of: Optional[date] = None, u=Depends(current_user)):
    as_of = as_of or date.today()

    with engine.begin() as conn:
        w = conn.execute(
            text("""select full_name, payout, payout_anchor_date
                    from workers where id = :id"""),
            {"id": str(worker_id)},
        ).fetchone()
        if not w:
            raise HTTPException(404, "Worker not found")

        start, end = period_for_worker(w[1], w[2], as_of)

        rows = conn.execute(
            text("""
              select wd.work_date,
                     tt.code, tt.name, tt.unit,
                     wt.quantity,
                     wt.status,
                     wt.approved_pay_ngn,
                     wt.note
              from work_tasks wt
              join work_days wd on wd.id = wt.work_day_id
              join task_types tt on tt.id = wt.task_type_id
              where wd.worker_id = :wid
                and wd.work_date between :s and :e
                and wt.paid_run_id is null
              order by wd.work_date asc, wt.created_at asc
            """),
            {"wid": str(worker_id), "s": start, "e": end},
        ).fetchall()

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["worker", w[0]])
    writer.writerow(["period_start", start, "period_end", end])
    writer.writerow([])
    writer.writerow(["date", "task_code", "task_name", "unit", "quantity", "status", "approved_pay_ngn", "note"])
    for r in rows:
        writer.writerow([r[0], r[1], r[2], r[3], float(r[4]), r[5], float(r[6]), r[7] or ""])

    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="payroll_{worker_id}_{start}_{end}.csv"'},
    )

@app.get("/api/payroll", dependencies=[Depends(require_admin_or_supervisor)])
def payroll_all(as_of: Optional[date] = None):
    as_of = as_of or date.today()
    with engine.begin() as conn:
        workers = conn.execute(text("""
          select id, full_name, payout, payout_anchor_date
          from workers
          where is_active = true
          order by full_name asc
        """)).fetchall()

        results = []
        for w in workers:
            wid = UUID(w[0])
            start, end = period_for_worker(w[2], w[3], as_of)

            rows = conn.execute(text("""
              select tt.code, wt.quantity, wt.approved_pay_ngn
              from work_tasks wt
              join work_days wd on wd.id = wt.work_day_id
              join task_types tt on tt.id = wt.task_type_id
              where wd.worker_id = :wid
                and wd.work_date between :s and :e
                and wt.status = 'approved'
            """), {"wid": str(wid), "s": start, "e": end}).fetchall()

            total_pay = sum(Decimal(str(r[2])) for r in rows).quantize(Decimal("0.01"))
            results.append({
                "worker_id": str(wid),
                "full_name": w[1],
                "payout": w[2],
                "period_start": start,
                "period_end": end,
                "approved_total_pay_ngn": float(total_pay),
            })
    return results


@app.get("/api/payroll/export.csv", dependencies=[Depends(require_admin_or_supervisor)])
def payroll_all_csv(as_of: Optional[date] = None):
    as_of = as_of or date.today()
    data = payroll_all(as_of=as_of)

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["as_of", as_of])
    writer.writerow(["worker_id", "full_name", "payout", "period_start", "period_end", "approved_total_pay_ngn"])
    for r in data:
        writer.writerow([r["worker_id"], r["full_name"], r["payout"], r["period_start"], r["period_end"], r["approved_total_pay_ngn"]])

    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="payroll_all_{as_of}.csv"'},
    )

# ---------------------------
# Settings: Factories / Teams / Workstations
# ---------------------------

class FactoryIn(BaseModel):
    name: str

class TeamIn(BaseModel):
    factory_id: UUID
    name: str

class WorkstationIn(BaseModel):
    factory_id: UUID
    name: str

@app.get("/api/factories", dependencies=[Depends(require_admin_or_supervisor)])
def list_factories():
    with engine.begin() as conn:
        rows = conn.execute(text("select id, name from factories order by name asc")).fetchall()
    return [{"id": r[0], "name": r[1]} for r in rows]

@app.post("/api/factories", dependencies=[Depends(require_admin)])
def create_factory(body: FactoryIn):
    with engine.begin() as conn:
        row = conn.execute(
            text("insert into factories (name) values (:n) returning id, name"),
            {"n": body.name.strip()},
        ).fetchone()
    return {"id": row[0], "name": row[1]}

@app.get("/api/teams", dependencies=[Depends(require_admin_or_supervisor)])
def list_teams(factory_id: Optional[UUID] = None):
    q = "select id, factory_id, name from teams"
    params = {}
    if factory_id:
        q += " where factory_id = :f"
        params["f"] = str(factory_id)
    q += " order by name asc"
    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()
    return [{"id": r[0], "factory_id": r[1], "name": r[2]} for r in rows]

@app.post("/api/teams", dependencies=[Depends(require_admin)])
def create_team(body: TeamIn):
    with engine.begin() as conn:
        row = conn.execute(
            text("""insert into teams (factory_id, name) values (:f, :n)
                    returning id, factory_id, name"""),
            {"f": str(body.factory_id), "n": body.name.strip()},
        ).fetchone()
    return {"id": row[0], "factory_id": row[1], "name": row[2]}

@app.get("/api/workstations", dependencies=[Depends(require_admin_or_supervisor)])
def list_workstations(factory_id: Optional[UUID] = None):
    q = "select id, factory_id, name from workstations"
    params = {}
    if factory_id:
        q += " where factory_id = :f"
        params["f"] = str(factory_id)
    q += " order by name asc"
    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()
    return [{"id": r[0], "factory_id": r[1], "name": r[2]} for r in rows]

@app.post("/api/workstations", dependencies=[Depends(require_admin)])
def create_workstation(body: WorkstationIn):
    with engine.begin() as conn:
        row = conn.execute(
            text("""insert into workstations (factory_id, name) values (:f, :n)
                    returning id, factory_id, name"""),
            {"f": str(body.factory_id), "n": body.name.strip()},
        ).fetchone()
    return {"id": row[0], "factory_id": row[1], "name": row[2]}


class WorkerUpdateIn(BaseModel):
    full_name: Optional[str] = None
    worker_code: Optional[str] = None
    payout: Optional[FREQ] = None
    payout_anchor_date: Optional[date] = None
    factory_id: Optional[UUID] = None
    team_id: Optional[UUID] = None
    is_active: Optional[bool] = None

@app.patch("/api/workers/{worker_id}", dependencies=[Depends(require_admin_or_supervisor)])
def update_worker(worker_id: UUID, body: WorkerUpdateIn):
    sets = []
    params = {"id": str(worker_id)}
    for k, v in body.model_dump(exclude_unset=True).items():
        sets.append(f"{k} = :{k}")
        params[k] = str(v) if isinstance(v, UUID) else v
    if not sets:
        return {"ok": True}
    q = "update workers set " + ", ".join(sets) + " where id = :id"
    with engine.begin() as conn:
        conn.execute(text(q), params)
    return {"ok": True}

class WorkerRateUpsertIn(BaseModel):
    worker_id: UUID
    task_type_id: UUID
    rate_ngn: float

@app.get("/api/worker-rates/{worker_id}", dependencies=[Depends(require_admin_or_supervisor)])
def list_worker_rates(worker_id: UUID):
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select wr.id, wr.worker_id, wr.task_type_id, wr.rate_ngn,
                 tt.code, tt.name, tt.unit
          from worker_rates wr
          join task_types tt on tt.id = wr.task_type_id
          where wr.worker_id = :wid
          order by tt.name asc
        """), {"wid": str(worker_id)}).fetchall()
    return [{
        "id": r[0], "worker_id": r[1], "task_type_id": r[2], "rate_ngn": float(r[3]),
        "task_code": r[4], "task_name": r[5], "unit": r[6]
    } for r in rows]

@app.post("/api/worker-rates", dependencies=[Depends(require_admin)])
def upsert_worker_rate(body: WorkerRateUpsertIn):
    with engine.begin() as conn:
        conn.execute(text("""
          insert into worker_rates (worker_id, task_type_id, rate_ngn)
          values (:w, :t, :r)
          on conflict (worker_id, task_type_id)
          do update set rate_ngn = excluded.rate_ngn
        """), {"w": str(body.worker_id), "t": str(body.task_type_id), "r": body.rate_ngn})
    return {"ok": True}

@app.delete("/api/worker-rates/{rate_id}", dependencies=[Depends(require_admin)])
def delete_worker_rate(rate_id: UUID):
    with engine.begin() as conn:
        conn.execute(text("delete from worker_rates where id=:id"), {"id": str(rate_id)})
    return {"ok": True}
    

class WorkTaskUpdateIn(BaseModel):
    quantity: Optional[float] = None
    note: Optional[str] = None
    task_type_id: Optional[UUID] = None

@app.patch("/api/work-tasks/{task_id}", dependencies=[Depends(require_admin_or_supervisor)])
def update_pending_task(task_id: UUID, body: WorkTaskUpdateIn, u=Depends(current_user)):
    with engine.begin() as conn:
        assert_workday_open_by_task(conn, task_id)
        # Ensure task exists and is pending
        row = conn.execute(text("""
            select wt.id, wt.status, wd.logged_by
            from work_tasks wt
            join work_days wd on wd.id = wt.work_day_id
            where wt.id = :id
        """), {"id": str(task_id)}).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        if row[1] != "pending":
            raise HTTPException(400, "Only pending tasks can be edited")
        # Supervisor can only edit tasks they logged
        if u["role"] == "supervisor" and str(row[2]) != str(u["id"]):
            raise HTTPException(403, "Supervisors can only edit tasks they logged")

        sets = []
        params = {"id": str(task_id), "by": str(u["id"])}
        payload = body.model_dump(exclude_unset=True)

        if "quantity" in payload:
            if payload["quantity"] is None or payload["quantity"] < 0:
                raise HTTPException(400, "Quantity must be >= 0")
            sets.append("quantity = :quantity")
            params["quantity"] = payload["quantity"]

        if "note" in payload:
            sets.append("note = :note")
            params["note"] = payload["note"]

        if "task_type_id" in payload:
            sets.append("task_type_id = :task_type_id")
            params["task_type_id"] = str(payload["task_type_id"])

        if not sets:
            return {"ok": True}

        sets.append("updated_at = now()")
        sets.append("updated_by = :by")

        conn.execute(
            text("update work_tasks set " + ", ".join(sets) + " where id = :id"),
            params
        )

        audit(conn, u["id"], u["role"], "TASK_EDIT", "work_task", task_id, payload)

    return {"ok": True}


@app.delete("/api/work-tasks/{task_id}", dependencies=[Depends(require_admin_or_supervisor)])
def delete_pending_task(task_id: UUID, u=Depends(current_user)):
    with engine.begin() as conn:
        assert_workday_open_by_task(conn, task_id)
        row = conn.execute(text("""
            select wt.id, wt.status, wd.logged_by
            from work_tasks wt
            join work_days wd on wd.id = wt.work_day_id
            where wt.id = :id
        """), {"id": str(task_id)}).fetchone()
        if not row:
            raise HTTPException(404, "Task not found")
        if row[1] != "pending":
            raise HTTPException(400, "Only pending tasks can be deleted")
        if u["role"] == "supervisor" and str(row[2]) != str(u["id"]):
            raise HTTPException(403, "Supervisors can only delete tasks they logged")

        conn.execute(text("delete from work_tasks where id = :id"), {"id": str(task_id)})
        audit(conn, u["id"], u["role"], "TASK_DELETE", "work_task", task_id, {})

    return {"ok": True}


@app.get("/api/audit", dependencies=[Depends(require_admin)])
def list_audit(
    entity_type: Optional[str] = None,
    entity_id: Optional[UUID] = None,
    limit: int = 100
):
    limit = max(1, min(limit, 500))
    q = """
      select created_at, actor_role, action, entity_type, entity_id, metadata
      from audit_logs
      where 1=1
    """
    params = {}
    if entity_type:
        q += " and entity_type = :et"
        params["et"] = entity_type
    if entity_id:
        q += " and entity_id = :eid"
        params["eid"] = str(entity_id)

    q += " order by created_at desc limit :lim"
    params["lim"] = limit

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).fetchall()

    return [{
        "created_at": r[0],
        "actor_role": r[1],
        "action": r[2],
        "entity_type": r[3],
        "entity_id": r[4],
        "metadata": r[5],
    } for r in rows]


@app.get("/api/reports/task-totals", dependencies=[Depends(require_admin_or_supervisor)])
def report_task_totals(start: date, end: date):
    # sums by task type within date range (APPROVED only)
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select tt.code, tt.name, tt.unit,
                 sum(wt.quantity) as total_qty,
                 sum(wt.approved_pay_ngn) as total_pay
          from work_tasks wt
          join work_days wd on wd.id = wt.work_day_id
          join task_types tt on tt.id = wt.task_type_id
          where wd.work_date between :s and :e
            and wt.status = 'approved'
          group by tt.code, tt.name, tt.unit
          order by tt.name asc
        """), {"s": start, "e": end}).fetchall()

    return [{
        "task_code": r[0],
        "task_name": r[1],
        "unit": r[2],
        "total_quantity": float(r[3] or 0),
        "total_pay_ngn": float(r[4] or 0),
    } for r in rows]


@app.get("/api/reports/by-workstation", dependencies=[Depends(require_admin_or_supervisor)])
def report_by_workstation(start: date, end: date):
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select coalesce(ws.name, 'Unassigned') as workstation,
                 sum(wt.approved_pay_ngn) as total_pay
          from work_tasks wt
          join work_days wd on wd.id = wt.work_day_id
          left join workstations ws on ws.id = wd.workstation_id
          where wd.work_date between :s and :e
            and wt.status = 'approved'
          group by coalesce(ws.name, 'Unassigned')
          order by total_pay desc
        """), {"s": start, "e": end}).fetchall()

    return [{"workstation": r[0], "total_pay_ngn": float(r[1] or 0)} for r in rows]


@app.get("/api/reports/by-supervisor", dependencies=[Depends(require_admin_or_supervisor)])
def report_by_supervisor(start: date, end: date):
    # how much value each logger generated (approved pay)
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select au.email,
                 count(distinct wd.id) as days_logged,
                 count(wt.id) filter (where wt.status='approved') as tasks_approved,
                 sum(wt.approved_pay_ngn) filter (where wt.status='approved') as approved_pay
          from work_days wd
          join app_users au on au.id = wd.logged_by
          left join work_tasks wt on wt.work_day_id = wd.id
          where wd.work_date between :s and :e
          group by au.email
          order by approved_pay desc nulls last
        """), {"s": start, "e": end}).fetchall()

    return [{
        "supervisor_email": r[0],
        "days_logged": int(r[1] or 0),
        "tasks_approved": int(r[2] or 0),
        "approved_pay_ngn": float(r[3] or 0),
    } for r in rows]
    

@app.post("/api/work-days/{work_day_id}/close", dependencies=[Depends(require_admin_or_supervisor)])
def close_day(work_day_id: UUID, u=Depends(current_user)):
    with engine.begin() as conn:
        wd = conn.execute(text("select is_closed from work_days where id=:id"), {"id": str(work_day_id)}).fetchone()
        if not wd:
            raise HTTPException(404, "Work day not found")
        if wd[0]:
            return {"ok": True}

        conn.execute(text("""
          update work_days
          set is_closed=true, closed_by=:by, closed_at=now()
          where id=:id
        """), {"id": str(work_day_id), "by": str(u["id"])})

        audit(conn, u["id"], u["role"], "WORKDAY_CLOSE", "work_day", work_day_id, {})

    return {"ok": True}


@app.post("/api/work-days/{work_day_id}/reopen", dependencies=[Depends(require_admin)])
def reopen_day(work_day_id: UUID, u=Depends(current_user)):
    with engine.begin() as conn:
        wd = conn.execute(text("select is_closed from work_days where id=:id"), {"id": str(work_day_id)}).fetchone()
        if not wd:
            raise HTTPException(404, "Work day not found")
        if not wd[0]:
            return {"ok": True}

        conn.execute(text("""
          update work_days
          set is_closed=false, closed_by=null, closed_at=null
          where id=:id
        """), {"id": str(work_day_id)})

        audit(conn, u["id"], u["role"], "WORKDAY_REOPEN", "work_day", work_day_id, {})

    return {"ok": True}


class PayrollRunCreateIn(BaseModel):
    as_of: date
    note: Optional[str] = None

@app.post("/api/payroll-runs", dependencies=[Depends(require_admin_or_supervisor)])
def create_payroll_run(body: PayrollRunCreateIn, u=Depends(current_user)):
    run_id = uuid4()
    with engine.begin() as conn:
        now = datetime.now(timezone.utc)
        conn.execute(text("""
          insert into payroll_runs (id, as_of, created_by, note)
          values (:id, :as_of, :by, :note)
        """), {"id": str(run_id), "as_of": body.as_of, "by": str(u["id"]), "note": body.note})

        workers_rows = conn.execute(text("""
          select id, full_name, payout, payout_anchor_date
          from workers
          where is_active = true
          order by full_name asc
        """)).fetchall()

        for w in workers_rows:
            wid = UUID(str(w[0]))
            start, end = period_for_worker(w[2], w[3], body.as_of)

            rows = conn.execute(text("""
	    select wt.id, tt.code, wt.quantity
	    from work_tasks wt
	    join work_days wd on wd.id = wt.work_day_id
	    join task_types tt on tt.id = wt.task_type_id
	    where wd.worker_id = :worker_id
	      and wd.work_date between :start and :end
	      and wt.status = 'approved'
	      and wt.paid_run_id is null
	"""), {"worker_id": str(worker_id), "start": period_start, "end": period_end}).fetchall()

            total_pay = sum(Decimal(str(r[2])) for r in rows).quantize(Decimal("0.01"))
            combed = sum(Decimal(str(r[1])) for r in rows if r[0] == "COMBING")
            woven = sum(Decimal(str(r[1])) for r in rows if r[0] == "WEAVING")
            
            conn.execute(text("""
              insert into payroll_run_items
                (run_id, worker_id, worker_name, payout, period_start, period_end,
                 approved_total_pay_ngn, approved_combed_kg, approved_woven_m)
              values
                (:run, :wid, :name, :payout, :ps, :pe, :pay, :ckg, :wm)
              on conflict (run_id, worker_id) do nothing
            """), {
                "run": str(run_id),
                "wid": str(wid),
                "name": w[1],
                "payout": w[2],
                "ps": start,
                "pe": end,
                "pay": str(total_pay),
                "ckg": str(combed),
                "wm": str(woven),
            })
            
            task_ids = [str(r[0]) for r in rows]
            if task_ids:
            	conn.execute(text("""
			update work_tasks
			set paid_run_id = :run_id,
			    paid_at = :paid_at
			where id = any(:task_ids::uuid[])
			  and status = 'approved'
			  and paid_run_id is null
		    """), {
			"run_id": str(run_id),
			"paid_at": now,
			"task_ids": task_ids
		    })

        audit(conn, u["id"], u["role"], "PAYROLL_RUN_CREATE", "payroll_run", run_id, {"as_of": str(body.as_of)})

    return {"run_id": str(run_id)}

@app.get("/api/payroll-runs", dependencies=[Depends(require_admin_or_supervisor)])
def list_payroll_runs(limit: int = 50):
    limit = max(1, min(limit, 200))
    with engine.begin() as conn:
        rows = conn.execute(text("""
          select id, as_of, created_at, note
          from payroll_runs
          order by created_at desc
          limit :lim
        """), {"lim": limit}).fetchall()
    return [{"id": r[0], "as_of": r[1], "created_at": r[2], "note": r[3]} for r in rows]

@app.get("/api/payroll-runs/{run_id}", dependencies=[Depends(require_admin_or_supervisor)])
def get_payroll_run(run_id: UUID):
    with engine.begin() as conn:
        hdr = conn.execute(text("select id, as_of, created_at, note from payroll_runs where id=:id"), {"id": str(run_id)}).fetchone()
        if not hdr:
            raise HTTPException(404, "Run not found")
        items = conn.execute(text("""
          select worker_id, worker_name, payout, period_start, period_end,
                 approved_total_pay_ngn, approved_combed_kg, approved_woven_m
          from payroll_run_items
          where run_id=:id
          order by worker_name asc
        """), {"id": str(run_id)}).fetchall()
    return {
        "run": {"id": hdr[0], "as_of": hdr[1], "created_at": hdr[2], "note": hdr[3]},
        "items": [{
            "worker_id": i[0],
            "worker_name": i[1],
            "payout": i[2],
            "period_start": i[3],
            "period_end": i[4],
            "approved_total_pay_ngn": float(i[5]),
            "approved_combed_kg": float(i[6]),
            "approved_woven_m": float(i[7]),
        } for i in items]
    }

@app.get("/api/payroll-runs/{run_id}/export.csv", dependencies=[Depends(require_admin_or_supervisor)])
def export_payroll_run_csv(run_id: UUID):
    with engine.begin() as conn:
        hdr = conn.execute(text("select as_of, created_at, note from payroll_runs where id=:id"), {"id": str(run_id)}).fetchone()
        if not hdr:
            raise HTTPException(404, "Run not found")
        items = conn.execute(text("""
          select worker_id, worker_name, payout, period_start, period_end,
                 approved_total_pay_ngn, approved_combed_kg, approved_woven_m
          from payroll_run_items
          where run_id=:id
          order by worker_name asc
        """), {"id": str(run_id)}).fetchall()

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["run_id", str(run_id)])
    writer.writerow(["as_of", hdr[0], "created_at", hdr[1], "note", hdr[2] or ""])
    writer.writerow([])
    writer.writerow(["worker_id","worker_name","payout","period_start","period_end","approved_total_pay_ngn","approved_combed_kg","approved_woven_m"])
    for i in items:
        writer.writerow([i[0], i[1], i[2], i[3], i[4], float(i[5]), float(i[6]), float(i[7])])

    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="payroll_run_{run_id}.csv"'},
    )

@app.get("/api/reports/task-totals/export.csv", dependencies=[Depends(require_admin_or_supervisor)])
def report_task_totals_csv(start: date, end: date):
    data = report_task_totals(start=start, end=end)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["start", start, "end", end])
    w.writerow(["task_code","task_name","unit","total_quantity","total_pay_ngn"])
    for r in data:
        w.writerow([r["task_code"], r["task_name"], r["unit"], r["total_quantity"], r["total_pay_ngn"]])
    return Response(out.getvalue(), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="report_task_totals_{start}_{end}.csv"'})

@app.get("/api/reports/by-workstation/export.csv", dependencies=[Depends(require_admin_or_supervisor)])
def report_by_workstation_csv(start: date, end: date):
    data = report_by_workstation(start=start, end=end)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["start", start, "end", end])
    w.writerow(["workstation","total_pay_ngn"])
    for r in data:
        w.writerow([r["workstation"], r["total_pay_ngn"]])
    return Response(out.getvalue(), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="report_workstations_{start}_{end}.csv"'})

@app.get("/api/reports/by-supervisor/export.csv", dependencies=[Depends(require_admin_or_supervisor)])
def report_by_supervisor_csv(start: date, end: date):
    data = report_by_supervisor(start=start, end=end)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["start", start, "end", end])
    w.writerow(["supervisor_email","days_logged","tasks_approved","approved_pay_ngn"])
    for r in data:
        w.writerow([r["supervisor_email"], r["days_logged"], r["tasks_approved"], r["approved_pay_ngn"]])
    return Response(out.getvalue(), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="report_supervisors_{start}_{end}.csv"'})
        

@app.get("/api/payroll/due")
def payroll_due(as_of: date = Query(default=None), u=Depends(get_current_user)):
    # supervisors see only their factory scope (if any); admins see all
    if as_of is None:
        as_of = date.today()

    with engine.begin() as conn:
        # load workers (respect factory scope)
        if u["role"] == "supervisor" and u.get("factory_id"):
            wrows = conn.execute(text("""
                select id, full_name, payout, payout_anchor_date, factory_id
                from workers
                where is_active = true and factory_id = :fid
                order by full_name asc
            """), {"fid": str(u["factory_id"])}).fetchall()
        else:
            wrows = conn.execute(text("""
                select id, full_name, payout, payout_anchor_date, factory_id
                from workers
                where is_active = true
                order by full_name asc
            """)).fetchall()

        due = []
        for r in wrows:
            worker_id, full_name, payout, anchor, factory_id = r
            if not anchor:
                continue

            pstart, pend = compute_period(payout, anchor, as_of)

            # Due means: period has ended (or is today end) AND there is approved unpaid work in it
            if pend > as_of:
                continue

            x = conn.execute(text("""
                select
                  coalesce(sum(case when tt.code='COMBING' then wt.quantity else 0 end), 0) as combed_kg,
                  coalesce(sum(case when tt.code in ('WEAVING','TWISTING') then wt.quantity else 0 end), 0) as woven_m,
                  coalesce(sum(case when tt.code='COMBING' then wt.quantity*tt.rate_ngn_per_unit else 0 end), 0)
                  + coalesce(sum(case when tt.code in ('WEAVING','TWISTING') then wt.quantity*tt.rate_ngn_per_unit else 0 end), 0)
                  + coalesce(sum(case when tt.code not in ('COMBING','WEAVING','TWISTING') then wt.quantity*tt.rate_ngn_per_unit else 0 end), 0)
                  as total_pay
                from work_tasks wt
                join work_days wd on wd.id = wt.work_day_id
                join task_types tt on tt.id = wt.task_type_id
                where wd.worker_id = :wid
                  and wd.work_date between :start and :end
                  and wt.status='approved'
                  and wt.paid_run_id is null
            """), {"wid": str(worker_id), "start": pstart, "end": pend}).fetchone()

            combed_kg = float(x[0])
            woven_m = float(x[1])
            total_pay = float(x[2])

            if total_pay > 0:
                due.append({
                    "worker_id": str(worker_id),
                    "full_name": full_name,
                    "payout": payout,
                    "period_start": str(pstart),
                    "period_end": str(pend),
                    "approved_combed_kg": combed_kg,
                    "approved_woven_m": woven_m,
                    "approved_total_pay_ngn": total_pay,
                })

    return due

        
def assert_workday_open_by_task(conn, task_id: UUID):
    row = conn.execute(text("""
      select wd.is_closed
      from work_tasks wt
      join work_days wd on wd.id = wt.work_day_id
      where wt.id = :tid
    """), {"tid": str(task_id)}).fetchone()

    if not row:
        raise HTTPException(404, "Task not found")

    if row[0] is True:
        raise HTTPException(400, "Work day is closed")


