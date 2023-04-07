sage
from jina import Client
from frontend_config import SERVER, PORT, TOP_K
from docarray import Document
import streamlit as st
from streamlit_chat import message
import pickle

st.set_page_config(page_title="IntelliSearch", page_icon=":robot:")

st.header("Intelligent semantic search")

if "generated" not in st.session_state:
    st.session_state["generated"] = []

if "past" not in st.session_state:
    st.session_state["past"] = []

st.sidebar.markdown("# Introduction\n\nThis chatbot is built using: \n\n - [**DocArray**](https://docarray.jina.ai): The data structure for unstructured data\n- [**Jina**](https://github.com/jina-ai/jina/): Cloud native neural search framework\n- [**Streamlit**](https://streamlit.io/) - frontend\n- [**Streamlit-chat**](https://github.com/AI-Yash/st-chat): Chat plugin for Streamlit\n - [**SentenceTransformers**](https://huggingface.co/sentence-transformers):SentenceTransformers 🤗 is a Python framework for state-of-the-art sentence, text and image embeddings.\n)")


def query(payload):
    return search_by_text(payload["inputs"]["text"])


def search_by_text(input, server=SERVER, port=PORT, limit=TOP_K):
    client = Client(host=server, protocol="http", port=port)
    response = client.search(
        Document(text=input),
        parameters={"limit": limit},
        return_results=True,
        show_progress=True,
    )
    
  
    #results = response[0]
    res = response[0].matches#results[0].matches
    rest = res[0]#res.pop()
    match = [rest.text ]
    for c in rest.chunks:
        match.append(c.text+"\n")
    match.append( rest.uri.strip("''"))
    
 
        

    return match


user_input = st.text_input("What's your question?", "", key="input")

if user_input:
    output = {}
    out = query(
        {
            "inputs": {
                "past_user_inputs": st.session_state.past,
                "generated_responses": st.session_state.generated,
                "text": user_input,
            },
            "parameters": {"repetition_penalty": 1.33},
        }
    )
    
    link = out.pop()
    output["generated_text"] = out

    st.session_state.past.append(user_input)
    st.session_state.generated.append(output["generated_text"])

message_container = st.container()
if st.session_state["generated"]:

    for i in range(len(st.session_state["generated"]) - 1, -1, -1):
        with message_container:
            message(st.session_state["past"][i], is_user=True, key=str(i) + "_user")
            message(st.session_state["generated"][i], key=str(i))
        st.markdown('Check out <{}> for more information.'.format( link))
