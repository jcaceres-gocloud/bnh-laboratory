CREATE TABLE IF NOT EXISTS processed.organizacion (
    id_organizacion TEXT NOT NULL,
    jurisdiccion    TEXT,
    lote_id         TEXT,
    nombre          TEXT NOT NULL,
    descripcion     TEXT,
    c_organizacion  TEXT NOT NULL,
    fecha_alta      DATE NOT NULL,
    fecha_baja      DATE,
    processed_at    TIMESTAMP NOT NULL
);
