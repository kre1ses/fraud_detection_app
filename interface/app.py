import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2
import plotly.express as px
import plotly.graph_objects as go

# Конфигурация подключений
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

POSTGRES_CONFIG = {
    'host': os.getenv("POSTGRES_HOST", "postgres"),
    'database': os.getenv("POSTGRES_DB", "fraud_detection"),
    'user': os.getenv("POSTGRES_USER", "postgres"),
    'password': os.getenv("POSTGRES_PASSWORD", "postgres"),
    'port': os.getenv("POSTGRES_PORT", "5432")
}

def connect_to_db():
    """Подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {str(e)}")
        return None

def get_recent_fraud_transactions():
    """Получение 10 последних мошеннических транзакций"""
    conn = connect_to_db()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT transaction_id, score, created_at 
            FROM scoring_results 
            WHERE fraud_flag = true 
            ORDER BY created_at DESC 
            LIMIT 10
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Ошибка получения данных: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_recent_scores(limit=100):
    """Получение последних скоров для гистограммы"""
    conn = connect_to_db()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = f"""
            SELECT score, fraud_flag, created_at 
            FROM scoring_results 
            ORDER BY created_at DESC 
            LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Ошибка получения данных: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

def show_results_section():
    """Раздел для отображения результатов"""
    st.header("Результаты скоринга")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Обновить результаты", key="refresh_results"):
            st.rerun()
    
    with col2:
        if st.button("Очистить кэш", key="clear_cache"):
            st.cache_data.clear()
            st.rerun()
    
    # Получение и отображение мошеннических транзакций
    st.subheader("Последние мошеннические транзакции")
    fraud_df = get_recent_fraud_transactions()
    
    if not fraud_df.empty:
        st.dataframe(
            fraud_df.style.format({
                'score': '{:.6f}',
                'created_at': lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else ''
            }),
            use_container_width=True
        )
        
        # Статистика
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Количество", len(fraud_df))
        with col2:
            st.metric("Максимальный скор", f"{fraud_df['score'].max():.6f}")
        with col3:
            st.metric("Минимальный скор", f"{fraud_df['score'].min():.6f}")
    else:
        st.info("Мошеннических транзакций не обнаружено")
    
    # Гистограмма распределения скоров
    st.subheader("Распределение скоров (последние 100 транзакций)")
    scores_df = get_recent_scores(100)
    
    if not scores_df.empty:
        # Создание гистограммы
        fig = px.histogram(
            scores_df, 
            x='score',
            nbins=20,
            title='Распределение скоринговых оценок',
            labels={'score': 'Score', 'count': 'Count'},
            color='fraud_flag',
            color_discrete_map={True: 'red', False: 'blue'}
        )
        
        fig.update_layout(
            xaxis_title="Score",
            yaxis_title="Количество транзакций",
            showlegend=True,
            legend_title="Мошенничество"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Статистика по скорам
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Общее количество", len(scores_df))
        with col2:
            st.metric("Средний скор", f"{scores_df['score'].mean():.6f}")
        with col3:
            st.metric("Мошеннических", f"{scores_df['fraud_flag'].sum()}")
        with col4:
            st.metric("Доля мошеннических", f"{(scores_df['fraud_flag'].sum() / len(scores_df) * 100):.1f}%")
    else:
        st.info("Нет данных для построения гистограммы")

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Создание вкладок
tab1, tab2 = st.tabs(["📤 Отправка данных", "📊 Результаты"])

with tab1:
    st.title("📤 Отправка данных в Kafka")
    
    # Блок загрузки файлов
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"],
        key="file_uploader"
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Добавляем файл в состояние
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

    # Список загруженных файлов
    if st.session_state.uploaded_files:
        st.subheader("🗂 Список загруженных файлов")
        
        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])
            
            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")
            
            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

with tab2:
    show_results_section()