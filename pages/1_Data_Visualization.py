import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

df = pd.read_csv('saved_data/df_clean.csv')

house_type_mapping = {
    1: 'Main Street House',
    2: 'Villa',
    3: 'Alley House',
    4: 'Townhouse',
}

def house_by_city():
    city_counts = df['city'].value_counts()
    top_3_cities = city_counts.head(3)
    other_cities_count = city_counts.iloc[3:].sum()
    if other_cities_count > 0:
        plot_data = pd.concat([top_3_cities, pd.Series([other_cities_count], index=['Others'])])
    else:
        plot_data = top_3_cities
        
    plt.figure(figsize=(10, 8))
    plt.pie(plot_data, labels=plot_data.index, autopct='%1.1f%%', startangle=180, colors=['navy', 'steelblue', 'skyblue', 'lightgray'], textprops={'color': 'white'})
    plt.title('Distribution of Houses by City')
    plt.axis('equal')  
    plt.legend(plot_data.index, title="Cities", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    return plt


def house_by_type():
    type_counts = df['house_type'].value_counts()
    type_labels = type_counts.index.map(lambda x: house_type_mapping.get(x, x))
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=type_labels, y=type_counts.values, color = 'lightcoral')
    plt.title('Distribution of Houses by Type')
    plt.xlabel('House Type')
    plt.ylabel('Number of Houses')
    plt.xticks(rotation=45)
    return plt

def price_distribution():
    plt.figure(figsize=(10, 6))
    sns.histplot(df['price'], kde=True, bins=30, color = 'steelblue')
    plt.title('Distribution of Price')
    plt.xlabel('Price (billion VND)')
    plt.ylabel('Number of Houses')
    return plt

def size_distribution():
    plt.figure(figsize=(10, 6))
    sns.histplot(df['size'], kde=True, bins=30, color = 'crimson')
    plt.title('Distribution of House Sizes')
    plt.xlabel('Size (m^2)')
    plt.ylabel('Number of Houses')
    return plt


def main():
    st.set_page_config(layout="wide")
    st.title("House Data Visualization")

    st.subheader("Distribution of House Sizes")
    size_dist = size_distribution()
    st.pyplot(size_dist)
    
    st.subheader("Distribution of Price")
    price_dist = price_distribution()
    st.pyplot(price_dist)

    st.subheader("Distribution of Houses by City")
    city_dist = house_by_city()
    st.pyplot(city_dist)

    st.subheader("Distribution of Houses by Type")
    type_dist = house_by_type()
    st.pyplot(type_dist)

if __name__ == "__main__":
    main()
