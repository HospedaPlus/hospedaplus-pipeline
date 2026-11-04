import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

from spark_jobs import quality, silver_calendario, silver_f1, silver_ocupacion, silver_resenas


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def strings(spark, rows):
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in rows])
    return spark.read.option("primitivesAsString", "true").json(rdd)


def hotels(spark):
    return strings(spark, [{"id": "1", "code": "HP-CO-001", "country": "CO"},
                           {"id": "2", "code": "HP-PT-001", "country": "PT"}])


# ---- ocupacion ---------------------------------------------------------------------------------

def test_ocupacion_counts_relocations_and_refused_nights(spark):
    inv = strings(spark, [
        {"hotel_id": "1", "room_type_id": "1", "inventory_date": "2026-11-10", "total_rooms": "10",
         "sold_rooms": "11", "overbooking_limit": "1"},
        {"hotel_id": "1", "room_type_id": "1", "inventory_date": "2026-11-11", "total_rooms": "10",
         "sold_rooms": "5", "overbooking_limit": "0"},
    ])
    rel = strings(spark, [{"hotel_id": "1", "room_type_id": "1", "relocation_date": "2026-11-10"}])
    rej = strings(spark, [{"hotel_id": "1", "room_type_id": "1", "check_in": "2026-11-10",
                           "check_out": "2026-11-12", "rooms_count": "2"}])
    rooms = strings(spark, [{"id": "1", "code": "DBL"}])
    out = silver_ocupacion.transform(inv, rel, rej, hotels(spark), rooms)
    silver_ocupacion.check(out)
    got = {r.fecha: r for r in out.collect()}
    a, b = got[dt.date(2026, 11, 10)], got[dt.date(2026, 11, 11)]
    assert a.ocupacion_pct == 110.0 and a.sobrevendidas == 1 and a.reubicadas == 1 and a.disponibles == 0
    assert a.habitaciones_rechazadas == 2 and b.habitaciones_rechazadas == 2  # both nights of the stay
    assert b.reubicadas == 0 and b.hotel_code == "HP-CO-001" and b.room_type == "DBL"


# ---- calendario --------------------------------------------------------------------------------

def test_calendario_holidays_and_events(spark, tmp_path):
    d = tmp_path / "anio=2026"
    d.mkdir()
    (d / "pais=CO.json").write_text(json.dumps([{"date": "2026-11-02", "localName": "Todos los Santos"},
                                                {"date": "2026-11-05", "localName": "X"}]))
    (d / "pais=PT.json").write_text(json.dumps([{"date": "2026-11-03", "localName": "Y"}]))
    hol = silver_calendario.holidays(spark.read.option("multiLine", "true").json(str(tmp_path)))
    ev = strings(spark, [
        {"hotel_code": "HP-CO-001", "name": "Feria", "start_date": "2026-11-02", "end_date": "2026-11-03",
         "category": "medium"},
        {"hotel_code": "HP-CO-001", "name": "Concierto", "start_date": "2026-11-03", "end_date": "2026-11-03",
         "category": "high"},
    ])
    out = silver_calendario.transform(hol, ev, hotels(spark))
    silver_calendario.check(out)
    got = {(r.hotel_code, r.fecha): r for r in out.collect()}
    co2, co3 = got[("HP-CO-001", dt.date(2026, 11, 2))], got[("HP-CO-001", dt.date(2026, 11, 3))]
    assert co2.es_festivo and co2.nombre_festivo == "Todos los Santos" and co2.evento == "Feria"
    assert not co3.es_festivo and co3.categoria_evento == "high"
    assert got[("HP-PT-001", dt.date(2026, 11, 3))].es_festivo  # PT holiday only in PT
    assert not got[("HP-PT-001", dt.date(2026, 11, 2))].es_festivo
    assert len(got) == 2 * 4  # 2 hotels x 2..5 Nov


def test_calendario_accepts_wrapped_payload(spark, tmp_path):
    d = tmp_path / "anio=2026"
    d.mkdir()
    (d / "pais=CO.json").write_text(json.dumps({"holidays": [{"date": "2026-12-25", "name": "Navidad"}]}))
    hol = silver_calendario.holidays(spark.read.option("multiLine", "true").json(str(tmp_path))).collect()
    assert [(r.pais, r.nombre_festivo) for r in hol] == [("CO", "Navidad")]


# ---- resenas -----------------------------------------------------------------------------------

def review(i, platform, text, published, updated=None, rating=4, hotel="HP-CO-001"):
    return {"id": str(i), "external_id": f"e{i}", "platform": platform, "hotel_id": hotel, "language": "es",
            "rating": str(rating), "title": None, "text": text, "reviewer_name": "Ana",
            "reviewer_country": "CO", "stay_date": "2026-11-01", "published_at": published,
            "created_at": published, "updated_at": updated or published}


def test_resenas_dedup_versions_and_platforms(spark):
    raw = strings(spark, [
        review(1, "booking", "Muy buen hotel.", "2026-11-02T10:00:00"),
        review(1, "booking", "Muy buen hotel.", "2026-11-02T10:00:00", updated="2026-11-03T10:00:00"),
        review(2, "google", "muy buen hotel", "2026-11-02T08:00:00"),       # copy, published first
        review(3, "expedia", "Pésimo", "2026-11-02T09:00:00", rating=1),
    ])
    out = silver_resenas.transform(raw, hotels(spark))
    silver_resenas.check(out)
    got = {r.review_id: r for r in out.collect()}
    assert set(got) == {2, 3}
    assert got[2].duplicados == 1 and got[2].copias_en == ["booking"] and got[2].sentimiento == "positivo"
    assert got[3].sentimiento == "negativo" and got[3].hotel_pais == "CO"


def test_resenas_rejects_unknown_hotel(spark):
    out = silver_resenas.transform(strings(spark, [review(1, "booking", "x", "2026-11-02T10:00:00",
                                                          hotel="HP-XX-999")]), hotels(spark))
    with pytest.raises(quality.QualityError):
        silver_resenas.check(out)


# ---- f1 ----------------------------------------------------------------------------------------

def test_f1_types_channel_and_invalid_rows(spark, tmp_path):
    head = ("hotel,is_canceled,lead_time,arrival_date_year,arrival_date_month,arrival_date_week_number,"
            "arrival_date_day_of_month,stays_in_weekend_nights,stays_in_week_nights,adults,children,babies,meal,"
            "country,market_segment,distribution_channel,is_repeated_guest,previous_cancellations,"
            "previous_bookings_not_canceled,reserved_room_type,assigned_room_type,booking_changes,deposit_type,"
            "agent,company,days_in_waiting_list,customer_type,adr,required_car_parking_spaces,"
            "total_of_special_requests,reservation_status,reservation_status_date")
    rows = ["City Hotel,1,45,2016,March,10,5,1,2,2,NA,0,BB,PRT,Online TA,TA/TO,0,0,0,A,A,0,Non Refund,9,NULL,0,"
            "Transient,100.5,0,0,Canceled,2016-02-01",
            "Resort Hotel,0,0,2015,July,27,1,0,0,0,0,0,BB,PRT,Direct,Direct,0,0,0,C,C,3,No Deposit,NULL,NULL,0,"
            "Transient,0,0,0,Check-Out,2015-07-01"]
    f = tmp_path / "f1.csv"
    f.write_text(head + "\n" + "\n".join(rows) + "\n")
    valid, dropped = silver_f1.split_valid(silver_f1.transform(spark.read.option("header", "true").csv(str(f))))
    silver_f1.check(valid)
    r = valid.collect()
    assert dropped == 1 and len(r) == 1
    r = r[0]
    assert r.es_cancelada and r.fecha_llegada == dt.date(2016, 3, 5) and r.noches == 3 and r.huespedes == 2
    assert r.channel_type == "ota" and r.deposit_type == "Non Refund" and r.ingreso_eur == 301.5
    assert r.rango_anticipacion == "30-90 dias"
