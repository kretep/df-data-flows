CREATE TABLE IF NOT EXISTS public.p1_meter
(
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    active_tariff integer NOT NULL,
    total_power_import_kwh real NOT NULL,
    total_power_import_t1_kwh real NOT NULL,
    total_power_import_t2_kwh real NOT NULL,
    total_power_export_kwh real NOT NULL,
    total_power_export_t1_kwh real NOT NULL,
    total_power_export_t2_kwh real NOT NULL,
    active_power_w real NOT NULL,
    active_power_l1_w real NOT NULL,
    active_power_l2_w real NOT NULL,
    active_power_l3_w real NOT NULL,

    CONSTRAINT p1_meter_pkey PRIMARY KEY (created_at)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.p1_meter
    OWNER to postgres;
