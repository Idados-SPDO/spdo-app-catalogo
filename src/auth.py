# src/auth.py
import streamlit as st
from src.db_snowflake import get_session, users_get, users_check_password

def init_auth():
    if "auth" not in st.session_state:
        st.session_state.auth = {"logged": False, "user": None}

def login_user(username: str, password: str) -> bool:
    session = get_session()
    ok = users_check_password(session, username, password)
    if ok:
        info = users_get(session, username)
        st.session_state.auth = {"logged": True, "user": info}
    return ok

def is_authenticated() -> bool:
    return bool(st.session_state.get("auth", {}).get("logged", False))

def current_user():
    return st.session_state.get("auth", {}).get("user", None)

def logout_user():
    st.session_state.auth = {"logged": False, "user": None}


def require_roles(*allowed_roles: str):
    from src.auth import init_auth, is_authenticated, current_user  # mantém compatível com seu setup

    init_auth()
    if not is_authenticated():
        st.error("Faça login para continuar.")
        st.stop()

    role = (current_user().get("role") or "USER").upper().strip()
    allowed = {r.upper().strip() for r in allowed_roles}

    if role not in allowed:
        st.error("Você não tem permissão para acessar esta página.")
        st.stop()
