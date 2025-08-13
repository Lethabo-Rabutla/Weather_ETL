# Weather ETL Pipeline

A robust **Python-based ETL (Extract, Transform, Load) pipeline** that automatically fetches, processes, visualizes, and stores weather forecast data for multiple South African cities. This project demonstrates expertise in **data engineering, API integration, data visualization, and database management** using modern Python tools.

## **Features**

* Fetches **hourly 10-day weather forecasts** via the OpenWeatherMap API.
* Processes and transforms raw JSON data into structured **Pandas DataFrames**.
* Converts temperatures from Kelvin to Celsius for readability.
* Generates **insightful visualizations**:

  * Temperature trends over time (line plots)
  * Temperature heatmaps for each day
* Stores data efficiently in a **PostgreSQL database**.
* Fully configurable via **environment variables**.
* Modular, clean, and maintainable Python code — ideal for automation in **Airflow DAGs**.

## **Tech Stack**

* **Python 3.12+**
* **Pandas** — data processing and manipulation
* **Matplotlib & Seaborn** — advanced visualization
* **SQLAlchemy & Psycopg2** — database connectivity
* **PostgreSQL** — relational database storage
* **Requests** — API integration
* **dotenv** — secure configuration via `.env` files
* **Airflow (optional)** — scheduling and orchestration

## **Project Structure**

```
Weather_ETL/
├── weather_etl_pipeline.py   # Main ETL pipeline (extract, transform, load, visualize)
├── main.py                   # Entry point to run the pipeline
├── plots/                    # Folder for generated plots
├── requirements.txt          # Project dependencies
├── .env                      # Environment variables (API keys, DB credentials)
└── airflow_dags/             # Optional Airflow DAGs for scheduling ETL tasks
```

## **Setup Instructions**

1. **Clone the repository**

```bash
git clone https://github.com/Lethabo-Rabutla/Weather_ETL.git
cd Weather_ETL
```

2. **Create and activate a Python virtual environment**

```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. **Configure environment variables**
   Create a `.env` file in the project root:

```
API_KEY=your_openweathermap_api_key
DB_USERNAME=your_postgres_user
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
```

5. **Run the ETL pipeline**

```bash
python main.py
```

6. **View generated plots**
   Plots will be saved in the `plots/` directory:

* `<City>_temperature_trend.png`
* `<City>_temperature_heatmap.png`

7. **(Optional) Schedule ETL via Airflow**

* Place `weather_etl_dag.py` in your Airflow `dags/` folder.
* Configure Airflow variables/connections for API\_KEY and database credentials.
* The DAG fetches weather data every hour and stores it in PostgreSQL automatically.

## **Key Highlights**

* **Modular Design:** Functions are separated by responsibility — fetching, transforming, visualizing, and loading data.
* **Error Handling:** Fails gracefully with clear error messages for API or database issues.
* **Data-Driven Insights:** Line plots and heatmaps provide actionable insights at a glance.
* **Scalable:** Easily extendable to more cities or additional weather parameters.
* **Production-Ready:** Can be scheduled with **Airflow** for automated, recurring ETL tasks.

## **Future Enhancements**

* Integrate **real-time alerts** for extreme weather conditions.
* Add support for **historical weather data** storage and analysis.
* Containerize the project using **Docker** for cloud deployment.
* Extend Airflow DAGs to include **data quality checks** and **email notifications**.

## **Author**

**Lethabo Rabutla**

* Recent Computer Science Graduate
* Specializing in **Software Development, Data Engineering, and ETL Pipelines**
* GitHub: [https://github.com/Lethabo-Rabutla](https://github.com/Lethabo-Rabutla)
