"""DDL e DML do PostgreSQL."""
from include.config import HISTORY_TABLE, SNAPSHOT_TABLE

CREATE_TABLES = f"""
CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
    reference_date DATE          NOT NULL,
    category       TEXT          NOT NULL,
    product_count  INTEGER       NOT NULL,
    avg_price      NUMERIC(12,2) NOT NULL,
    min_price      NUMERIC(12,2) NOT NULL,
    max_price      NUMERIC(12,2) NOT NULL,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (reference_date, category)
);

CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    id             BIGSERIAL PRIMARY KEY,
    run_id         TEXT          NOT NULL,
    reference_date DATE          NOT NULL,
    category       TEXT          NOT NULL,
    product_count  INTEGER       NOT NULL,
    avg_price      NUMERIC(12,2) NOT NULL,
    min_price      NUMERIC(12,2) NOT NULL,
    max_price      NUMERIC(12,2) NOT NULL,
    loaded_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    UNIQUE (run_id, category)
);
"""

# Snapshot: DELETE + INSERT na mesma transação => estado final = exatamente o da run,
# inclusive removendo categorias que deixaram de existir.
DELETE_SNAPSHOT_DATE = f"DELETE FROM {SNAPSHOT_TABLE} WHERE reference_date = %(reference_date)s"

INSERT_SNAPSHOT = f"""
INSERT INTO {SNAPSHOT_TABLE}
    (reference_date, category, product_count, avg_price, min_price, max_price)
VALUES
    (%(reference_date)s, %(category)s, %(product_count)s, %(avg_price)s, %(min_price)s, %(max_price)s)
"""

# Histórico: append, mas protegido contra re-execução da mesma run (run_id, category).
INSERT_HISTORY = f"""
INSERT INTO {HISTORY_TABLE}
    (run_id, reference_date, category, product_count, avg_price, min_price, max_price)
VALUES
    (%(run_id)s, %(reference_date)s, %(category)s, %(product_count)s, %(avg_price)s, %(min_price)s, %(max_price)s)
ON CONFLICT (run_id, category) DO NOTHING
"""
