import streamlit as st
import os


st.set_page_config(
    page_title="Real Estate Price Predictor",
    layout="wide"
)


left, right = st.columns([1, 2])

with left: 
    st.write('\n\n\n\n\n\n')
    st.title('Real Estate Price Predictor')
    st.write(''' Estimate property prices using historical market data and
                machine learning models. Enter property details to receive
                an instant price prediction.''')
    st.write('\n\n\n\n\n\n')
    pressed = st.button('Start Predicting')
    if pressed:
        st.switch_page('pages/2_Prediction.py')
with right:
    st.image(os.path.join(os.getcwd(), 'static', 'sample_house.png'))

