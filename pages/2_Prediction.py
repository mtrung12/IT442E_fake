import streamlit as st
import json 
import pandas as pd
import pickle
from preprocess import DataProcessor

house_type_ls = ['Main street house', 'Villa', 'Alley house', 'Townhouse']
legal_ls = [
        "Full legal ownership",
        "Purchase contract",
        "Pending legal paperwork",
        "Shared ownership",
        "Deposit agreement",
        "Handwritten document",
        "Other legal documents",
        "No ownership document",
        "Undefined or unclear legal status"
    ]

with open('saved_data/vn_available_locations.json', 'r', encoding='utf-8') as f:
        lct_hierachy = json.load(f)
    
province_list = list(lct_hierachy.keys()) 
    
def display():
    # Province 
    st.subheader('Provinces/City')
    select_city = st.selectbox('Choose the province/city:', province_list, index = None, placeholder='Select a province/city')

    # District
    st.subheader('District')
    if select_city:
        dist_hierachy = lct_hierachy[select_city]
        district_list = list(dist_hierachy.keys())
    else:
        district_list = []
    select_district = st.selectbox('Choose the district', district_list, index = None, placeholder='Select a district')

    # Ward
    st.subheader('Ward')
    if select_district:
        ward_hierachy = dist_hierachy[select_district]
        ward_list = list(ward_hierachy.keys())
    else:
        ward_list = []
    select_ward = st.selectbox('Choose the ward', ward_list, index = None, placeholder='Select a ward')

    # Street
    st.subheader('Street')
    if select_ward:
        street_list = ward_hierachy[select_ward]
    else:
        street_list = []
    select_street = st.selectbox('Choose the ward', street_list, index = None, placeholder='Select a street')

    # house type
    st.subheader('House type')
    house_type = st.selectbox('Choose the house type', house_type_ls, index = None, placeholder='Select a house type')

    # Size
    st.subheader('Size')
    size = st.number_input('Fill in the house size in m^2', min_value = 0, format = '%d')

    # Floors
    st.subheader('Floors')
    floors = st.select_slider("Number of Floors", options = list(range(1, 11)) + ["10+"], value = 1)

    # Bedrooms
    st.subheader('Bedrooms')
    bedrooms = st.select_slider("Number of Bedrooms", options = list(range(1, 11)) + ["10+"], value = 1)

    # toilets
    st.subheader('Toilets')
    toilets = st.select_slider("Number of Toilets", options = list(range(1, 11)) + ["10+"], value = 1)

    # legal docs
    st.subheader('Legal documents')
    legal = st.selectbox('Choose the type of available document for the house', legal_ls, index = None, placeholder = 'Select a legal status of the house')

    st.write('\n\n\n\n\n\n')

    all_filled = all([
        select_city, 
        select_district, 
        select_ward, 
        select_street, 
        house_type, 
        legal
    ]) and (size > 0)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        submit = st.button('Predict house price', use_container_width=True, disabled=not all_filled)

    if submit:
        house_info = {
            "street": select_street,
            "ward": select_ward,
            "district": select_district,
            "city": select_city,
            "size": size,
            "property_legal_document": legal,
            "bed_rooms": bedrooms,
            "toilets": toilets,
            "floors": floors,
            "house_type": house_type
        }
        house_input = pd.DataFrame({k: [v] for k, v in house_info.items()})
        predicted_price = predict(house_input)
        st.balloons()
        
        result_sidebar(house_info, predicted_price)
        price_dialog(predicted_price)

def result_sidebar(house_info, predicted_price):
    with st.sidebar:
        st.title('Result Summary:')
        st.subheader(f'Estimated Price:\n {predicted_price:,.0f} VND')
        st.write('\n\n\n\n\n\n\n\n')
        st.subheader('House details:\n\n')
        for key, value in house_info.items():
            st.write(f"{key}: {value}")

@st.dialog("Prediction result")
def price_dialog(price):
    st.success(f'## Estimated Price: {price:,.0f} VND')
    st.info('Checkout the sidebar for further details.')
    
def predict(X):
    model_path = 'saved_models/random_forest_model.pkl'
    processor_path = 'saved_models/processor.pkl'
    with open(processor_path, 'rb') as f:
        processor = pickle.load(f)
    scaled_X = processor.transform(X)
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    return model.predict(scaled_X)[0]*1e9
    

def main():
    display()
    
if __name__ == "__main__":
    main()