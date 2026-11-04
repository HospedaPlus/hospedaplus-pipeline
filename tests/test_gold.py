"""Gold model: business rules of each mart on small, known inputs."""

import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

from spark_jobs import gold_modelo as g

TODAY = dt.date(2026, 11, 4)


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def strings(spark, rows):
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in rows])
    return spark.read.option("primitivesAsString", "true").json(rdd)


def res(code, check_in, nights=2, booked="2026-10-01", status="confirmed", channel="BOOKING", total=400_000.0,
        cancelled=None, penalty=0.0, hotel="HP-CO-001", room="DOUBLE", dup=False):
    ci = dt.date.fromisoformat(check_in)
    return {"code": code, "source": "F2", "hotel_code": hotel, "channel_code": channel,
            "channel_type": "ota" if channel == "BOOKING" else "direct", "room_type": room, "status": status,
            "fecha_reserva": dt.date.fromisoformat(booked), "check_in": ci, "check_out": ci + dt.timedelta(nights),
            "noches": nights, "habitaciones": 1, "huespedes": 2,
            "lead_time_dias": (ci - dt.date.fromisoformat(booked)).days, "reembolsable": True,
            "es_cancelada": status in ("cancelled", "no_show"), "es_duplicado": dup, "currency": "COP",
            "fx_estimada": False, "total_cop": total, "comision_pct": 15.0, "comision_cop": total * 0.15,
            "neto_cop": total * 0.85, "penalidad_cop": penalty,
            "cancelada_en": dt.datetime.fromisoformat(cancelled) if cancelled else None}


def reservas(spark, rows):
    return spark.createDataFrame(rows, schema=(
        "code string, source string, hotel_code string, channel_code string, channel_type string, "
        "room_type string, status string, fecha_reserva date, check_in date, check_out date, noches int, "
        "habitaciones int, huespedes int, lead_time_dias int, reembolsable boolean, es_cancelada boolean, "
        "es_duplicado boolean, currency string, fx_estimada boolean, total_cop double, comision_pct double, "
        "comision_cop double, neto_cop double, penalidad_cop double, cancelada_en timestamp"))


def test_p1_rate_by_channel_and_lead_ignores_duplicates(spark):
    rows = [res("A", "2026-10-20", booked="2026-10-01"),
            res("B", "2026-10-21", booked="2026-10-02", status="cancelled", cancelled="2026-10-19 10:00:00",
                penalty=100_000.0),
            res("C", "2026-10-22", booked="2026-10-03", dup=True)]
    fr = g.fact_reservas(reservas(spark, rows))
    m = g.mart_p1_cancelacion(fr).collect()
    assert len(m) == 1 and m[0].reservas == 2 and m[0].canceladas == 1 and m[0].tasa_cancelacion == 0.5
    assert m[0].penalidad_cop == 100_000.0 and m[0].ingreso_perdido_cop == 300_000.0


def test_neon_rates_use_neon_buckets_and_only_past_stays(spark):
    rows = [res("A", "2026-10-10", booked="2026-10-05"),                       # 5 days -> 0-7
            res("B", "2026-10-11", booked="2026-10-06", status="cancelled"),   # 5 days -> 0-7
            res("C", "2026-12-01", booked="2026-11-28", status="cancelled")]   # future stay: left out
    out = {r.lead_bucket: r for r in g.neon_cancellation_rates(g.fact_reservas(reservas(spark, rows)), TODAY).collect()}
    assert set(out) == {"0-7"} and out["0-7"].cancel_rate == 0.5 and out["0-7"].reservas == 2


def test_p4_net_revenue_excludes_cancelled(spark):
    rows = [res("A", "2026-10-20", total=400_000.0), res("B", "2026-10-21", status="cancelled")]
    m = g.mart_p4_comisiones(g.fact_reservas(reservas(spark, rows))).collect()
    assert len(m) == 1 and m[0].bruto_cop == 400_000.0 and m[0].comision_cop == 60_000.0
    assert m[0].neto_cop == 340_000.0 and m[0].pct_comision_efectiva == 15.0


def test_on_the_books_counts_bookings_alive_14_days_before(spark):
    night = "2026-11-20"  # cut = 2026-11-06
    rows = [res("A", night, nights=1, booked="2026-11-01"),                                          # counts
            res("B", night, nights=1, booked="2026-11-10"),                                          # too late
            res("C", night, nights=1, booked="2026-11-01", status="cancelled", cancelled="2026-11-05 09:00:00"),
            res("D", night, nights=1, booked="2026-11-01", status="cancelled", cancelled="2026-11-08 09:00:00")]
    otb = g.on_the_books(g.nights(reservas(spark, rows))).collect()
    assert otb[0].otb_14d == 2  # A and D (cancelled only after the cut)


def test_suggested_price_rules(spark):
    rates = strings(spark, [{"hotel_id": "1", "room_type_id": "1", "season_code": "HIGH", "currency": "COP",
                             "amount": "125000", "valid_from": "2026-11-05", "valid_to": "2026-11-07"}])
    hotels = strings(spark, [{"id": "1", "code": "HP-CO-001"}])
    rooms = strings(spark, [{"id": "1", "code": "DOUBLE"}])
    otb = spark.createDataFrame([("HP-CO-001", "DOUBLE", dt.date(2026, 11, 5), 13, 10),   # +30 % -> x1.10
                                 ("HP-CO-001", "DOUBLE", dt.date(2026, 11, 6), 5, 10),    # -50 % -> x0.92
                                 ("HP-CO-001", "DOUBLE", dt.date(2026, 11, 7), 10, 10)],  # flat
                                "hotel_code string, room_type string, fecha date, otb_14d long, "
                                "otb_14d_anio_anterior long")
    cal = spark.createDataFrame([("HP-CO-001", dt.date(2026, 11, 7), True, False)],
                                "hotel_code string, fecha date, es_festivo boolean, hay_evento boolean")
    fx = spark.createDataFrame([(dt.date(2026, 11, 4), "COP", 1.0)], "fecha date, moneda string, cop_por_unidad double")
    p = {r.fecha.day: r for r in g.mart_p3_precio_sugerido(rates, hotels, rooms, otb, cal, fx, TODAY).collect()}
    assert p[5].tarifa_base == 100_000.0                     # 125.000 / HIGH 1,25
    assert p[5].precio_sugerido == pytest.approx(137_500.0)  # 125.000 x 1,10
    assert p[6].precio_sugerido == pytest.approx(115_000.0)  # 125.000 x 0,92
    assert p[7].precio_sugerido == pytest.approx(143_750.0)  # 125.000 x 1,15 (holiday)
    assert all(75_000 <= r.precio_sugerido <= 160_000 for r in p.values())


def test_rn10_recent_reviews_weigh_double(spark):
    fres = spark.createDataFrame([("HP-CO-001", dt.date(2026, 8, 20), 2, "negativo"),   # 76 days before: weight 1
                                  ("HP-CO-001", dt.date(2026, 11, 4), 5, "positivo")],  # weight 2
                                 "hotel_code string, fecha_publicacion date, puntaje int, sentimiento string")
    m = g.mart_p5_reputacion(fres).filter("fecha_corte = '2026-11-04'").collect()[0]
    assert m.calificacion_rn10 == 4.0 and m.resenas_90d == 2 and m.pct_positivas == 50.0


def test_writeback_parse_rejects_bad_rows():
    import importlib.util
    import pathlib
    root = pathlib.Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("wb", root / "dags/gold_neon_writeback.py")
    wb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wb)
    assert wb.parse('{"hotel_code": "HP-CO-001", "lead_bucket": "8-30", "cancel_rate": 0.31234}') == \
        ("HP-CO-001", "8-30", 0.3123)
    with pytest.raises(ValueError):
        wb.parse('{"hotel_code": "HP-CO-001", "lead_bucket": "0-1", "cancel_rate": 0.3}')


def test_build_end_to_end_passes_checks(spark):
    rows = [res("A", "2026-10-20", booked="2026-10-01"),
            res("B", "2026-10-20", booked="2026-10-02", status="cancelled", cancelled="2026-10-19 10:00:00")]
    r = reservas(spark, rows)
    d = [dt.date(2026, 10, 20), dt.date(2026, 10, 21)]
    ocup = spark.createDataFrame([("HP-CO-001", "DOUBLE", x, 10, 1, 0, 0, 0, 0) for x in d],
                                 "hotel_code string, room_type string, fecha date, habitaciones int, vendidas int, "
                                 "limite_sobreventa int, sobrevendidas int, reubicadas long, "
                                 "habitaciones_rechazadas long")
    cal = spark.createDataFrame([("HP-CO-001", x, False, None, False, None, None) for x in d],
                                "hotel_code string, fecha date, es_festivo boolean, nombre_festivo string, "
                                "hay_evento boolean, evento string, categoria_evento string")
    cancel = r.filter("es_cancelada").select("code", "total_cop", "penalidad_cop", "cancelada_en")
    fx = spark.createDataFrame([(dt.date(2026, 10, 1), "COP", 1.0)], "fecha date, moneda string, cop_por_unidad double")
    dims = {"hotels": strings(spark, [{"id": "1", "code": "HP-CO-001", "name": "X", "category": "urban",
                                       "city": "Bogota", "country": "CO", "currency": "COP",
                                       "timezone": "America/Bogota"}]),
            "channels": strings(spark, [{"code": "BOOKING", "name": "Booking", "channel_type": "ota",
                                         "commission_pct": "15"}]),
            "room_types": strings(spark, [{"id": "1", "code": "DOUBLE", "name": "Doble", "max_guests": "2"}]),
            "rates": None}
    silver = {"reservas": r, "cancelaciones": cancel, "ocupacion_diaria": ocup, "calendario_hotel": cal,
              "tasa_cambio_diaria": fx, "resenas": None, "historico_f1": None}
    t = g.build(silver, dims, TODAY, spark)
    g.check(t)
    fo = {x.fecha.day: x for x in t["fact_ocupacion_diaria"].collect()}
    assert fo[20].ingreso_cop == 200_000.0 and fo[21].ingreso_cop == 200_000.0  # 400.000 over 2 nights
    rev = t["mart_p3_revenue"].filter("fecha = '2026-10-20'").first()
    assert rev.adr_cop == 200_000.0 and rev.revpar_cop == 20_000.0 and rev.ocupacion_pct == 10.0
    assert t["dim_hotel"].first().habitaciones == 10
