CREATE TABLE orders (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    item_name  VARCHAR(255) NOT NULL,
    quantity   INT          NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
