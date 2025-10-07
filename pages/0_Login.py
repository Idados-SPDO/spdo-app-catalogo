import streamlit as st
from src.auth import init_auth, login_user, is_authenticated, current_user, logout_user
from src.db_snowflake import (
    get_session,
    users_list_usernames,
    users_update_password,
    users_get,
)

st.set_page_config(page_title="Login • Catálogo", layout="wide")
init_auth()

st.title("🔐 Login")

session = get_session()

if is_authenticated():
    user = current_user()
    st.success(f"Você já está logado como **{user['name']}** ({user['username']}).")
    if st.button("🚪 Sair", key="btn_logout_login_page"):
        logout_user()
        st.rerun()
    st.info("Use o menu superior/esquerdo para navegar.")
else:
    # carrega usuários
    usernames = users_list_usernames(session)
    if not usernames:
        st.error("Nenhum usuário cadastrado. Peça para um admin criar na base.")
        st.stop()

    # --- SELETOR DE USUÁRIO (fora do form, valor sempre atualizado) ---
    st.subheader("Acesso")
    username_login = st.selectbox("Usuário", usernames, key="login_sel_user")

    # --- Formulário de login (apenas senha + botão) ---
    with st.form("login_form"):
        senha_login = st.text_input("Senha", type="password", key="login_pwd_curr")
        submitted_login = st.form_submit_button("Entrar", use_container_width=True)

    if submitted_login:
        u = (username_login or "").strip()
        p = (senha_login or "").strip()
        if not p:
            st.warning("Informe a senha.")
        else:
            if login_user(u, p):
                info = users_get(session, u)
                st.success(f"Bem-vindo(a), {info['name']}!")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

    # -------- Modal de troca de senha (sem pedir senha atual) --------
    # Estado para abrir/fechar e um nonce para chaves únicas dos inputs do modal
    if "open_pwd" not in st.session_state:
        st.session_state["open_pwd"] = False
    if "pwd_nonce" not in st.session_state:
        st.session_state["pwd_nonce"] = 0

    # Botão que abre o modal — agora usa o selectbox acima (sempre atualizado)
    chosen_user = st.session_state.get("login_sel_user", usernames[0])
    if st.button("🔧 Trocar senha do usuário selecionado", key="btn_open_pwd"):
        st.session_state["open_pwd"] = True
        st.session_state["pwd_nonce"] += 1
        st.rerun()

    @st.dialog("Trocar senha")
    def change_pwd_dialog(user_to_change: str):
        st.write(f"Usuário: **{user_to_change}**")

        nonce = st.session_state.get("pwd_nonce", 0)
        key_pwd  = f"dlg_new_pwd_{nonce}"
        key_conf = f"dlg_new_conf_{nonce}"

        new_pwd  = st.text_input("Nova senha", type="password", key=key_pwd)
        new_conf = st.text_input("Confirmar nova senha", type="password", key=key_conf)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Salvar", type="primary", key=f"dlg_save_{nonce}"):
                n = (new_pwd or "").strip()
                c = (new_conf or "").strip()
                if not n or not c:
                    st.warning("Preencha a nova senha e a confirmação."); return
                if n != c:
                    st.warning("Nova senha e confirmação não conferem."); return
                if len(n) < 4:
                    st.warning("A nova senha deve ter pelo menos 4 caracteres."); return
                try:
                    users_update_password(session, user_to_change.strip(), n)
                    st.success("Senha atualizada com sucesso!")
                    st.session_state["open_pwd"] = False
                    st.session_state["pwd_nonce"] += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao atualizar senha: {e}")
        with col2:
            if st.button("Cancelar", key=f"dlg_cancel_{nonce}"):
                st.session_state["open_pwd"] = False
                st.session_state["pwd_nonce"] += 1
                st.rerun()

    if st.session_state.get("open_pwd"):
        change_pwd_dialog(chosen_user)
