import streamlit as st
from typing import Optional

USERS = {
    "spdo_admin":       {"password": "123", "name": "SPDO Admin",        "role": "admin"},
    "olivio.junior":    {"password": "123", "name": "Olívio Junior",     "role": "admin"},
    "felipe.fortunato": {"password": "123", "name": "Felipe Fortunato",  "role": "admin"},
    "diego.silva":      {"password": "123", "name": "Diego Silva",       "role": "admin"},
    "vanderlei.sampaio":{"password": "123", "name": "Vanderlei Sampaio", "role": "admin"},
    "ana.chaves":       {"password": "123", "name": "Ana Chaves",        "role": "admin"},
}


def init_auth():
    if "auth" not in st.session_state:
        st.session_state.auth = {"is_auth": False, "username": None, "name": None, "role": None}

def login_user(username: str, password: str) -> bool:
    u = USERS.get(username)
    if not u or u["password"] != password:
        return False
    st.session_state.auth = {"is_auth": True, "username": username, "name": u["name"], "role": u["role"]}
    return True

def logout_user():
    st.session_state.auth = {"is_auth": False, "username": None, "name": None, "role": None}

def is_authenticated() -> bool:
    return bool(st.session_state.get("auth", {}).get("is_auth"))

def current_user() -> Optional[dict]:
    return st.session_state.get("auth")
