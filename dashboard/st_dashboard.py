import streamlit as st
import pandas as pd
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import plotly.express as px
import altair as alt
import io

st.set_page_config(
    page_title="Used Car Data in MY",
    page_icon="https://raw.githubusercontent.com/aidil-syakirin/used-car-data/refs/heads/main/dashboard/myvi%20kuning.svg",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

#fetching the view file from azure blob storage
container_name = st.secrets["AZURE_CONTAINER_NAME"]
account_name = st.secrets["AZURE_STORAGE_ACCOUNT_NAME"]

try:
    # Create credential object using service principal
    credential = ClientSecretCredential(
        tenant_id=st.secrets["AZURE_TENANT_ID"] ,
        client_id=st.secrets["AZURE_CLIENT_ID"] ,
        client_secret=st.secrets["AZURE_CLIENT_SECRET"]
    )

    # Create the BlobServiceClient using service principal authentication
    service_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=credential
    )

    # Get container client
    container_client = service_client.get_container_client(container_name)

    # Replace 'your_file.parquet' with your actual parquet file name
    blob_name = 'used-car-data-view.parquet'
    blob_client = container_client.get_blob_client(blob_name)
    
    # Download the blob content
    downloaded_blob = blob_client.download_blob()
except Exception as e:
    st.error(f"Authentication Error: {str(e)}")
    st.error(f"Tenant ID being used: {st.secrets['AZURE_TENANT_ID']}")
    st.error(f"Client ID being used: {st.secrets['AZURE_CLIENT_ID']}")
    st.error(f"Storage Account: {account_name}")
    st.error(f"Container: {container_name}")
    raise

# Convert to DataFrame
df = pd.read_parquet(io.BytesIO(downloaded_blob.readall()))
df = df[['car_model','year','price','variant','mileage','state','location']]

#preprocessing the value of myvi to make it uniform
df['car_model'] = df['car_model'].replace('myvi', 'Myvi')
df['price'] = pd.to_numeric(df['price'], errors='coerce') 

#dashboard sidebar config
with st.sidebar:
    st.title('Used Car Data in MY')
    
    car_model_list = list(df.car_model.unique())[::-1]
    
    selected_car_model = st.selectbox('Select a car model', car_model_list, index=len(car_model_list)-1)
    df_selected_car_model = df[df.car_model == selected_car_model]

    params_list = ['year', 'state', 'variant']
    selected_params = st.selectbox('Select a Y parameter', params_list)

    # Price analysis by state
    avg_price_by_state_year = df_selected_car_model.groupby(['state', 'year'])['price'].agg([
        'mean',
        'count',
        'std'
    ]).reset_index()
    
    # Calculate price difference from national average
    yearly_avg = df_selected_car_model.groupby('year')['price'].mean().reset_index()
    avg_price_by_state_year = avg_price_by_state_year.merge(yearly_avg, on='year', suffixes=('', '_national'))
    avg_price_by_state_year['price_diff'] = avg_price_by_state_year['mean'] - avg_price_by_state_year['price']
    
    # Original state count code
    df_selected_car_model_sorted = df_selected_car_model.groupby('state').size().reset_index(name='count')
    df_selected_car_model_sorted = df_selected_car_model_sorted.sort_values(by='count', ascending=False)

    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.selectbox('Select a color theme', color_theme_list)

    # Add minimum sample size filter
    min_sample = st.sidebar.slider('Minimum number of cars per state-year', 1, 50, 5)
    avg_price_by_state_year = avg_price_by_state_year[avg_price_by_state_year['count'] >= min_sample]

def make_heatmap(input_df, selected_car_model, input_y, input_x, input_color, input_color_theme):
    # Create a copy and ensure price is numeric
    df_with_bins = input_df.copy()
    df_with_bins['price'] = pd.to_numeric(df_with_bins['price'], errors='coerce')
    
    # Create price bins
    if selected_car_model == 'Myvi':
        price_bins = [0, 10000, 20000, 30000, 40000, 50000, float('inf')]
        price_labels = ['0-10k', '10k-20k', '20k-30k', '30k-40k', '40k-50k','50k+']
    else:
        price_bins = [0, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, float('inf')]
        price_labels = ['0-10k', '10k-20k', '20k-30k', '30k-40k', '40k-50k', '50k-60k', '60k-70k', '70k-80k', '80k-90k', '90k+']
    
    # Add price range column
    df_with_bins['price_range'] = pd.cut(df_with_bins['price'], 
                                        bins=price_bins, 
                                        labels=price_labels, 
                                        include_lowest=True)
    
    # Create a count by year and price range
    count_matrix = df_with_bins.groupby([input_y, 'price_range']).size().reset_index(name='count')
    
    heatmap = alt.Chart(count_matrix).mark_rect().encode(
            y=alt.Y(f'{input_y}:O', axis=alt.Axis(title=input_y, titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            x=alt.X('price_range:O', axis=alt.Axis(title="Price Range (RM)", titleFontSize=18, titlePadding=15, titleFontWeight=900)),
            color=alt.Color('count:Q',
                             legend=alt.Legend(title="Number of Cars"),
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900
        ).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    return heatmap

col = st.columns((1.5, 4.5, 2), gap='medium')

with col[1]:
    st.markdown(f'### Car Model Data: {selected_car_model}')
    st.markdown(f'### Price vs {selected_params} heatmap')

    heatmap = make_heatmap(df_selected_car_model, selected_car_model, selected_params, 'price', 'count', selected_color_theme)
    st.altair_chart(heatmap)

    # Add price difference visualization
    st.markdown('#### Price Difference by State and Manufacturing Year')
    
    price_diff_chart = alt.Chart(avg_price_by_state_year).mark_rect().encode(
        x=alt.X('year:O', title='Manufacturing Year'),
        y=alt.Y('state:O', title='State'),
        color=alt.Color('price_diff:Q',
                       scale=alt.Scale(scheme=selected_color_theme, domainMid=0),
                       title='Price Difference from National Average (RM)'),
        tooltip=[
            alt.Tooltip('state:N', title='State'),
            alt.Tooltip('year:O', title='Manufacturing Year'),
            alt.Tooltip('mean:Q', title='Average Price', format=',.0f'),
            alt.Tooltip('price_diff:Q', title='Difference from National Avg', format=',.0f'),
            alt.Tooltip('count:Q', title='Number of Cars'),
            alt.Tooltip('std:Q', title='Standard Deviation', format=',.0f')
        ]
    ).properties(
        width=900,
        height=400,
        title=f'Price Differences for {selected_car_model} by State and Manufacturing Year'
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=12
    )
    
    st.altair_chart(price_diff_chart)

with col[2]:
    st.markdown('#### Top Listings by State')

    st.dataframe(df_selected_car_model_sorted,
                 column_order=("state", "count"),
                 hide_index=True,
                 width=None,
                 column_config={
                    "state": st.column_config.TextColumn(
                        "State",
                    ),
                    "count": st.column_config.ProgressColumn(
                        "Total Listings",
                        format="%d",
                        min_value=0,
                        max_value=max(df_selected_car_model_sorted['count']),
                     )}
                 )
    
    with st.expander('About', expanded=True):
        st.write('''
            - Data: [Carlist.my]
            - :orange[**Price Difference by State and Year**]: The black spot indicate no listing registered in that state
            - :red[*Note*]: year in this dashboard means manufacturing year of the car
            ''')
        
# from urllib.request import urlopen
# import json
# with urlopen('https://raw.githubusercontent.com/mptwaktusolat/jakim.geojson/refs/heads/master/malaysia.district.geojson') as response:
#     counties = json.load(response)

# fig = px.choropleth(geojson=counties, scope = 'asia'
#                           )
# fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# fig.show()

