import streamlit as st
from agent import SQLAgent

@st.cache_resource
def load_agent():
    return SQLAgent()

agent = load_agent()

st.title("📚 Chat")

if "messages" not in st.session_state:
    st.session_state.messages = []

question = st.chat_input("Digite sua pergunta")

if question:
    st.session_state.messages.append({"role": "user", 
                                      "content": question})

    with st.spinner("Consultando o agente..."):
        response, sql_query = agent.run(question)

    agent_reply = response
    st.session_state.messages.append({
        "role": "agent", 
        "content": agent_reply, 
        "sql": sql_query
    })

for msg in st.session_state.messages:
    with st.chat_message("user" if msg["role"] == "user" else "assistant"):
        st.markdown(msg["content"])
        if msg["role"] == "agent" and msg.get("sql"):
            with st.expander("Ver SQL utilizada"):
                st.code(msg["sql"], language="sql")