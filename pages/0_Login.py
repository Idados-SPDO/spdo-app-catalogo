# pages/0_Login.py
import streamlit as st
from src.auth import init_auth, login_user, is_authenticated, current_user, logout_user

st.set_page_config(page_title="Login • Catálogo", layout="wide")
init_auth()

st.title("🔐 Login")

if is_authenticated():
    user = current_user()
    st.success(f"Você já está logado como **{user['name']}** ({user['username']}).")
    if st.button("🚪 Sair"):
        logout_user()
        st.rerun()
    st.info("Use o menu superior/esquerdo para navegar.")
else:
    with st.form("login_form"):
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
    if submitted:
        if login_user(u.strip(), p):
            st.success("Login efetuado!")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")
