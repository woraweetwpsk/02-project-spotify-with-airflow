# Spotify Project

เป็น Project ที่นำข้อมูลมาจาก [Kaggle][Kaggle_Link] โดยเป็นข้อมูลเกี่ยวกับเพลงที่ Streaming อยู่บน Platform Spotify และ Platform อื่นๆ
เก็บรวบรวมเพลงที่มียอด stream บน Spotify ในปี 2024 โดยนำมาทำความสะอาดข้อมูลและนำมาวิเคราะห์แล้วแสดงออกมาในรูปแบบของ Dashboard

[Kaggle_Link]:https://www.kaggle.com/datasets/nelgiriyewithana/most-streamed-spotify-songs-2024/data

---
### Architecture

![a](https://github.com/woraweetwpsk/02-project-spotify-with-airflow/blob/main/images/architecture.png?raw=true)

- Orchestration: Apache Airflow
- Transformation: Pandas
- Data Lake: Google Cloud Storage
- Data Warehouse: Bigquery
- Data Visualization: Looker Studio
- Languge: Python  

---
### DAG Tasks

1. นำไฟล์ raw data Upload ไปยัง GCS raw bucket
2. Transform และ Cleansing ข้อมูลด้วย Pandas และ Upload ไปยัง GCS transform bucket
3. ตรวจสอบ dataset และ table บน Bigquery หากยังไม่มีให้สร้างขึ้นมา
4. Upload ไฟล์จาก GCS transform bucket ไปยัง Bigquery

---
### Dashboard

![d](https://github.com/woraweetwpsk/02-project-spotify-with-airflow/blob/main/images/dashboard.png?raw=true)

---
