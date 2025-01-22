# End-to-End Data Visualization Project

## **Objective**
Create an end-to-end data visualization project consisting of:
- Web scraping
- ELT pipeline
- Data architecture and management
- Data visualization dashboard

---

## **Project Summary**

### **Pipeline Overview**
The current ELT pipeline is manually triggered, as my local machine is no longer capable of hosting an Airflow instance. I am actively exploring alternatives, including:
- Using **Windows Task Scheduler** to automate tasks.
- Hosting an **Airflow instance in the cloud**.

### **Project Components**

#### **Web Scraping**
`win_scheduler/pd_extraction.py`  
- A Python script leveraging **BeautifulSoup (bs4)** for web scraping.
- Scrapes data and stores it in a local folder for initial staging.
- Data is manually uploaded to **Azure Data Lake** for further processing.

#### **Database Writing**
`win_scheduler/pd_db_write.py`  
- Fetches the scraped data from Azure Data Lake.
- Performs preprocessing transformations to align with the staging table schema.
- Writes the transformed data into an **Azure SQL** database.

#### **View Creation**
`win_scheduler/pd_create_view.py`  
- Captures a snapshot of the current database state.
- Creates a view in **Azure Blob Storage** to serve as the source data for the dashboard.

#### **Data Visualization Dashboard**
`dashboard/st_dashboard.py`  
- A dashboard built with **Streamlit** for data visualization.
- Displays insights from the processed and transformed data.

---

## **Improvements in Progress**

1. **Expand Data Sources**:
   - Include 1-2 additional data sources to reduce data skew.
   - Current data is overly concentrated in Klang Valley (~50%), limiting representativeness.
  
   ![image](https://github.com/user-attachments/assets/cadcdb02-0a88-4583-b5a5-66a9edaed3e2)


2. **Enhance Dashboard Visualizations**:
   - Provide more meaningful and actionable visualizations.
   - Use insights to highlight trends, distributions, and key findings from the data.

---

## **Future Work**
- Automate the pipeline using **Windows Task Scheduler** or migrate to a cloud-hosted **Airflow** instance.
- Optimize data processing for scalability and performance.
- Integrate additional data sources for broader insights.
- Enhance the Streamlit dashboard with interactive features and detailed analytics.

