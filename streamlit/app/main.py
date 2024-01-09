import streamlit as st
import os


st.title(":globe_with_meridians: Central ARBO")

st.markdown("## Upload de dados")
lab = st.selectbox('Laboratório', ['Einstein', 'Hilab', 'HlaGyn'])
lab = lab.lower()

uploaded_file = st.file_uploader(
    "Selecione os arquivos", 
    type=['csv', 'txt', 'xlsx', 'xls', 'tsv']
)
if uploaded_file is not None and lab is not None:
    # save uploaded file to disk
    path = os.path.join("/data", lab)

    st.markdown(
        f"**Confirmar upload?**\n{uploaded_file.name}"
    )
    confirm_bt = st.button("Confirmar")
    
    if confirm_bt:
        st.text("Upload confirmado")
        with open(os.path.join(path, uploaded_file.name), "wb") as f:
            f.write(uploaded_file.getbuffer())
