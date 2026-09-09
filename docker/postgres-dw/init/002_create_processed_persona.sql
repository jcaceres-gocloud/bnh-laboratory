CREATE TABLE IF NOT EXISTS processed.persona (
    id_persona                  TEXT NOT NULL,
    jurisdiccion                TEXT,
    lote_id                     TEXT,
    fecha_nacimiento            DATE,
    cuit                        TEXT,
    c_documento                 TEXT,
    nro_documento               TEXT,
    c_pais_nacimiento           TEXT,
    c_provincia_nacimiento      TEXT,
    c_departamento_nacimiento   TEXT,
    c_localidad_nacimiento      TEXT,
    c_municipio_nacimiento      TEXT,
    lugar_nacimiento            TEXT,
    c_fallecido                 TEXT,
    fecha_fallecido             DATE,
    c_es_indigena               TEXT,
    processed_at                TIMESTAMP NOT NULL
);
