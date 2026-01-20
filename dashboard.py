# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Аналитика дронов", layout="wide")
st.title(" Аналитика дронов")

def safe_load_csv(path, **kwargs):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        st.error(f"Ошибка при загрузке {path}: {e}")
        return None

# ===================================================================
#  СЫРЫЕ ДАННЫЕ (до 1 млн строк)
# ===================================================================
raw_file = "drone_events_million.csv"
raw_df = safe_load_csv(raw_file, nrows=1_000_000)

if raw_df is not None:
    st.header(" Сырые события дронов (выборка 1M строк)")

    with st.expander(" Просмотр данных"):
        st.dataframe(raw_df.head(20), use_container_width=True)

    st.markdown("###  Ключевые метрики дронов")

    col_kpi1, col_kpi2 = st.columns(2)

    total_events = len(raw_df)
    with col_kpi1:
        st.metric(label="Всего событий", value=f"{total_events:,}")

    unique_drones = raw_df['drone_id'].nunique() if 'drone_id' in raw_df.columns else 0
    with col_kpi2:
        st.metric(label="Уникальных дронов", value=str(unique_drones))

    # === ГРАФИК 1: Распределение типов событий ===
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📌 Распределение типов событий")
        if 'event_type' in raw_df.columns:
            event_counts = raw_df['event_type'].value_counts().reset_index()
            event_counts.columns = ['event_type', 'count']
            fig = px.pie(
                event_counts,
                names='event_type',
                values='count',
                title="Доли типов событий",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)

    # === ГРАФИК 2: Средняя батарея по топ-10 дронам ===
    with col2:
        st.subheader(" Ср. уровень батареи (Топ-10 активных дронов)")
        if {'drone_id', 'battery'}.issubset(raw_df.columns):
            valid_bat = raw_df.dropna(subset=['battery']).copy()
            if not valid_bat.empty:
                drone_stats = (
                    valid_bat.groupby('drone_id')
                    .agg(avg_battery=('battery', 'mean'), count=('battery', 'size'))
                    .reset_index()
                    .nlargest(10, 'count')
                    .sort_values('avg_battery', ascending=False)
                )
                fig = px.bar(
                    drone_stats,
                    x='drone_id',
                    y='avg_battery',
                    color='avg_battery',
                    color_continuous_scale='RdYlGn',
                    text=drone_stats['avg_battery'].round(1).astype(str) + '%',
                    title="Средний уровень батареи по дронам"
                )
                fig.update_layout(xaxis_tickangle=-45, coloraxis_showscale=False)
                fig.update_traces(textposition='outside')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Нет данных о батарее.")
        else:
            st.warning("Отсутствуют колонки 'drone_id' или 'battery'.")

    # === ГРАФИК 3: Зависимость эффективности от уровня батареи ===
    st.subheader(" Эффективность vs Уровень заряда")
    if {'battery', 'drone_efficiency'}.issubset(final_df.columns if 'final_df' in locals() else {}):
        eff_data = final_df[['battery', 'drone_efficiency']].dropna()
        if not eff_data.empty:
            fig = px.box(
                eff_data,
                x='drone_efficiency',
                y='battery',
                title="Распределение батареи по категориям эффективности",
                labels={'drone_efficiency': 'Эффективность', 'battery': 'Уровень заряда (%)'},
                color='drone_efficiency',
                color_discrete_map={'High': 'green', 'Medium': 'orange', 'Low': 'red'}
            )
            fig.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных для сравнения эффективности и батареи.")
    elif {'battery'}.issubset(raw_df.columns):
        raw_df['timestamp'] = pd.to_datetime(raw_df['timestamp'], errors='coerce')
        valid_bat = raw_df.dropna(subset=['battery', 'drone_id']).copy()

        if not valid_bat.empty:
            drone_avg_bat = valid_bat.groupby('drone_id')['battery'].mean().reset_index()
            def assign_efficiency(bat):
                if bat >= 80:
                    return 'High'
                elif bat >= 50:
                    return 'Medium'
                else:
                    return 'Low'
            drone_avg_bat['drone_efficiency'] = drone_avg_bat['battery'].apply(assign_efficiency)

            fig = px.box(
                drone_avg_bat,
                x='drone_efficiency',
                y='battery',
                title="Оценка эффективности по среднему уровню батареи",
                labels={'drone_efficiency': 'Оценка эффективности', 'battery': 'Ср. уровень заряда (%)'},
                color='drone_efficiency',
                color_discrete_map={'High': 'green', 'Medium': 'orange', 'Low': 'red'}
            )
            fig.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Нет данных о батарее для анализа.")
    else:
        st.warning("Колонка 'battery' не найдена. Невозможно показать зависимость.")

    # === ГРАФИК 4: Тепловая карта плотности событий (x, y) ===
    st.subheader(" Плотность событий (X-Y координаты)")
    if {'x', 'y'}.issubset(raw_df.columns):
        sample_heat = raw_df[['x', 'y']].dropna().sample(min(30000, len(raw_df)))
        fig = px.density_heatmap(
            sample_heat,
            x='x',
            y='y',
            nbinsx=60,
            nbinsy=60,
            title="Пространственная плотность событий дронов",
            color_continuous_scale='Viridis'
        )
        fig.update_layout(xaxis_title="X Координата", yaxis_title="Y Координата")
        st.plotly_chart(fig, use_container_width=True)

    # === ГРАФИК 5: Приоритет зон + батарея ===
    st.subheader(" Уровень батареи по приоритету зоны")
    if {'x', 'y', 'battery'}.issubset(raw_df.columns):
        def classify_priority(row):
            dist = np.sqrt((row['x'] - 800)**2 + (row['y'] - 350)**2)
            if dist < 200:
                return 'High'
            elif dist <= 400:
                return 'Medium'
            else:
                return 'Low'
        raw_df['zone_priority'] = raw_df.apply(classify_priority, axis=1)
        priority_battery = raw_df.groupby('zone_priority')['battery'].mean().reindex(['High', 'Medium', 'Low']).reset_index()
        priority_battery.columns = ['Приоритет зоны', 'Ср. батарея']
        fig = px.bar(
            priority_battery,
            x='Приоритет зоны',
            y='Ср. батарея',
            color='Приоритет зоны',
            color_discrete_map={'High': '#d73027', 'Medium': '#fc8d59', 'Low': '#fee08b'},
            text=priority_battery['Ср. батарея'].round(1).astype(str) + '%',
            title="Средний уровень батареи по приоритету зоны"
        )
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

else:
    st.warning(f"⚠️ Файл сырых данных `{raw_file}` не найден.")

# ===================================================================
# 📈 ИТОГОВЫЕ ДАННЫЕ
# ===================================================================
final_file = "drone_swarm_analytics.csv"
final_df = safe_load_csv(final_file)

if final_df is not None and not final_df.empty:
    st.header(" Обработанные данные дронов")

    with st.expander(" Просмотр обработанных данных"):
        st.dataframe(final_df, use_container_width=True)

    # === КАРТОЧКИ KPI ПОСЛЕ ОБРАБОТКИ — без Топ-эффективного и Общего времени ===
    st.markdown("###  Ключевые метрики после обработки")

    col_kpi5, col_kpi6 = st.columns(2)

    total_drones = len(final_df)
    with col_kpi5:
        st.metric(label="Дронов в отчёте", value=str(total_drones))

    avg_zones = final_df['processed_zones'].mean() if 'processed_zones' in final_df.columns else 0
    with col_kpi6:
        st.metric(label="Ср. зон/дрон", value=f"{avg_zones:.1f}" if avg_zones > 0 else "N/A")

    # === ГРАФИК 6: Эффективность дронов ===
    col3, col4 = st.columns(2)
    with col3:
        st.subheader(" Эффективность дронов")
        if 'drone_efficiency' in final_df.columns:
            eff = final_df['drone_efficiency'].value_counts().reset_index()
            eff.columns = ['Эффективность', 'Количество']
            fig = px.bar(
                eff,
                x='Эффективность',
                y='Количество',
                color='Эффективность',
                color_discrete_map={'High': 'green', 'Medium': 'orange', 'Low': 'red'},
                title="Распределение дронов по эффективности"
            )
            st.plotly_chart(fig, use_container_width=True)

    # === ГРАФИК 7: Обработанные зоны vs батарея ===
    with col4:
        st.subheader(" Зоны vs Ср. уровень батареи")
        if {'processed_zones', 'avg_battery_during_mission'}.issubset(final_df.columns):
            fig = px.scatter(
                final_df,
                x='avg_battery_during_mission',
                y='processed_zones',
                size='processed_zones',
                color='processed_zones',
                color_continuous_scale='plasma',
                title="Обработанные зоны vs Средний уровень батареи",
                labels={
                    'avg_battery_during_mission': 'Ср. уровень батареи (%)',
                    'processed_zones': 'Обработанные зоны'
                }
            )
            st.plotly_chart(fig, use_container_width=True)

# ===================================================================
#  МАСШТАБИРУЕМОСТЬ
# ===================================================================
st.header(" Масштабируемость: Время обработки vs Объём данных")

benchmark_file = "processing_benchmark.csv"
if os.path.exists(benchmark_file):
    bench_df = safe_load_csv(benchmark_file)
    if bench_df is not None and not bench_df.empty:
        fig = px.line(
            bench_df,
            x='Records',
            y='TimeSec',
            markers=True,
            title="Измеренная масштабируемость",
            labels={'Records': 'Количество записей', 'TimeSec': 'Время (сек)'}
        )
        fig.update_traces(line_color='#2ca02c')
        st.plotly_chart(fig, use_container_width=True)
else:
    sim_data = pd.DataFrame({
        "Records": [10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000],
        "TimeSec": [2.1, 4.8, 9.3, 18.7, 46.2, 92.5]
    })
    fig = px.line(
        sim_data,
        x='Records',
        y='TimeSec',
        markers=True,
        title="Симуляция линейной масштабируемости (O(n))",
        labels={'Records': 'Количество записей', 'TimeSec': 'Время (сек)'}
    )
    fig.update_traces(line_color='#ff7f0e', line_dash='dot')
    st.plotly_chart(fig, use_container_width=True)

# ===================================================================
#  Футер
# ===================================================================
st.markdown("---")
st.caption("💡 Дашборд автоматически обновляется при изменении CSV-файлов. Обновите страницу, чтобы увидеть новые результаты.")
