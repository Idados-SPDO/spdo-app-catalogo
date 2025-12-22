import streamlit as st
import pandas as pd
from snowflake.snowpark import functions as F

from src.auth import require_roles, current_user
from src.db_snowflake import (
    get_session,
    users_create_or_update,
    users_list_usernames,
)
from src.variables import FQN_USERS

DEFAULT_PASSWORD = "123"
require_roles("ADMIN")

st.set_page_config(page_title="Catálogo • Usuários", layout="wide")
st.title("👤 Configuração de Usuários")

session = get_session()

ROLES_VALIDAS = ["USER", "OPERACIONAL", "ADMIN"]
DEFAULT_LIMIT = 1000  # limite interno (sem mostrar na tela)


def _esc(s: str) -> str:
    return (s or "").replace("'", "''")


# Carrega usuários
t = session.table(FQN_USERS)
cols = [c.upper() for c in t.schema.names]

base_cols = [c for c in ["USERNAME", "NAME", "ROLE"] if c in cols]
if "USERNAME" not in base_cols or "ROLE" not in base_cols:
    st.error("TB_CATALOGO_USER precisa ter, no mínimo, as colunas USERNAME e ROLE.")
    st.stop()

# Buscar (sem max linhas na tela)
q = st.text_input("Buscar", key="usr_search", placeholder="Digite o nome do usuário")

tf = t.select([F.col(c) for c in base_cols])

if q.strip():
    tf = tf.filter(F.col("USERNAME").ilike(f"%{q.strip()}%"))

df = tf.limit(DEFAULT_LIMIT).to_pandas()

if df.empty:
    st.warning("Nenhum usuário encontrado.")
    st.stop()

df_original = df.copy()

st.subheader("Usuários")

# Adiciona coluna de exclusão (somente UI)
df_ui = df.copy()
df_ui.insert(0, "EXCLUIR", False)

cfg = {
    "EXCLUIR": st.column_config.CheckboxColumn("Excluir?", default=False),
    "USERNAME": st.column_config.TextColumn("Usuário", disabled=True),
    "ROLE": st.column_config.SelectboxColumn("Permissão", options=ROLES_VALIDAS, required=True),
}
if "NAME" in df_ui.columns:
    cfg["NAME"] = st.column_config.TextColumn("Nome")

edited = st.data_editor(
    df_ui,
    hide_index=True,
    use_container_width=True,
    column_config=cfg,
    key="users_editor_db",
)

# Lista de exclusão
to_delete = (
    edited.loc[edited["EXCLUIR"] == True, "USERNAME"]
    .dropna()
    .astype(str)
    .tolist()
)

# DataFrame para update (remove a coluna EXCLUIR)
edited_upd = edited.drop(columns=["EXCLUIR"], errors="ignore")
df_original_upd = df_original.copy()

cA, cB = st.columns([1, 2])

with cA:
    if st.button("Salvar alterações", use_container_width=True):
        bad = edited_upd.loc[~edited_upd["ROLE"].isin(ROLES_VALIDAS)]
        if not bad.empty:
            st.error("Há usuários com ROLE inválida. Use: USER, OPERACIONAL ou ADMIN.")
            st.stop()

        key = "USERNAME"
        compare_cols = [c for c in ["ROLE", "NAME"] if c in edited_upd.columns]

        merged = edited_upd.merge(
            df_original_upd[[key] + compare_cols],
            on=key,
            how="left",
            suffixes=("", "_OLD"),
        )

        changed_mask = False
        for c in compare_cols:
            changed_mask = changed_mask | (merged[c].astype("string") != merged[f"{c}_OLD"].astype("string"))

        changed = merged.loc[changed_mask, [key] + compare_cols].copy()

        if changed.empty:
            st.info("Nenhuma alteração detectada.")
            st.stop()

        try:
            for _, r in changed.iterrows():
                sets = []
                if "ROLE" in compare_cols:
                    sets.append(f"ROLE = '{_esc(str(r['ROLE']))}'")
                if "NAME" in compare_cols:
                    if pd.isna(r.get("NAME")):
                        sets.append("NAME = NULL")
                    else:
                        sets.append(f"NAME = '{_esc(str(r.get('NAME')))}'")

                set_sql = ", ".join(sets)
                session.sql(
                    f"UPDATE {FQN_USERS} SET {set_sql} WHERE USERNAME = '{_esc(str(r['USERNAME']))}'"
                ).collect()

            st.success(f"Alterações aplicadas: {len(changed)} usuário(s).")
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao salvar alterações: {e}")
            st.stop()

# --- Exclusão de usuários ---
st.divider()
st.subheader("Exclusão de usuários")

if not to_delete:
    st.info("Marque usuários na coluna **Excluir?** para habilitar a exclusão.")
else:
    st.warning(f"Usuários marcados para exclusão: **{len(to_delete)}**", icon="⚠️")
    st.write(", ".join(to_delete))

confirm = st.text_input("Digite EXCLUIR para confirmar", placeholder="EXCLUIR", key="confirm_delete_users")

# (Opcional) impedir apagar o próprio usuário logado
try:
    me = (current_user().get("username") or "").strip().lower()
except Exception:
    me = ""

can_delete = (confirm.strip().upper() == "EXCLUIR") and (len(to_delete) > 0)

if st.button("Excluir selecionados", use_container_width=True, disabled=not can_delete):
    # remove você mesmo da lista, se estiver marcado
    safe_list = [u for u in to_delete if u.strip().lower() != me]

    if not safe_list:
        st.error("Não é permitido excluir o usuário atualmente logado.")
        st.stop()

    try:
        in_list = ", ".join([f"'{_esc(u)}'" for u in safe_list])
        session.sql(f"DELETE FROM {FQN_USERS} WHERE USERNAME IN ({in_list})").collect()
        st.success(f"Usuários excluídos: {len(safe_list)}")
        st.rerun()
    except Exception as e:
        st.error(f"Falha ao excluir usuários: {e}")
        st.stop()

st.divider()
st.subheader("Adicionar usuário")

st.caption(f"Senha inicial padrão: **{DEFAULT_PASSWORD}** (recomendado trocar depois).")

with st.form("add_user_form", clear_on_submit=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        new_username = st.text_input("Usuário", placeholder="ex.: joao.silva")
    with c2:
        new_name = st.text_input("Nome", placeholder="Nome completo")
    with c3:
        new_role = st.selectbox("Permissão", ROLES_VALIDAS, index=0)

    # opcional: permitir definir senha já na criação (se preferir, pode remover este bloco)
    set_custom_pwd = st.checkbox("Definir senha agora (opcional)", value=False)
    custom_pwd = ""
    custom_conf = ""
    if set_custom_pwd:
        c4, c5 = st.columns(2)
        with c4:
            custom_pwd = st.text_input("Senha", type="password")
        with c5:
            custom_conf = st.text_input("Confirmar senha", type="password")

    submitted = st.form_submit_button("Adicionar", use_container_width=True)

    if submitted:
        u = (new_username or "").strip()
        n = (new_name or "").strip()
        r = (new_role or "").strip().upper()

        if not u:
            st.error("Usuário é obrigatório.")
            st.stop()

        if r not in ROLES_VALIDAS:
            st.error("ROLE inválida.")
            st.stop()

        # Checa duplicidade
        existing = set(users_list_usernames(session))
        if u in existing:
            st.error("Já existe um usuário com esse username.")
            st.stop()

        # senha a aplicar
        if set_custom_pwd:
            p = (custom_pwd or "").strip()
            c = (custom_conf or "").strip()
            if not p or not c:
                st.error("Preencha senha e confirmação.")
                st.stop()
            if p != c:
                st.error("Senha e confirmação não conferem.")
                st.stop()
            if len(p) < 4:
                st.error("A senha deve ter pelo menos 4 caracteres.")
                st.stop()
            pwd_to_set = p
        else:
            pwd_to_set = DEFAULT_PASSWORD

        try:
            # Cria usuário com hash/salt no banco
            users_create_or_update(session, u, (n or u), r, pwd_to_set)
            st.success(
                f"Usuário criado: **{u}** | ROLE: **{r}** | "
                f"Senha inicial: **{pwd_to_set}**"
            )
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao criar usuário: {e}")
            st.stop()
