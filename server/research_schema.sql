CREATE TABLE IF NOT EXISTS research_reports
(
    id BIGSERIAL PRIMARY KEY,
    report_type VARCHAR(30) NOT NULL,
    report_id VARCHAR(50) NOT NULL,
    title TEXT,
    securities_company VARCHAR(100),
    analyst VARCHAR(100),
    stock_code VARCHAR(20),
    stock_name VARCHAR(100),
    classification VARCHAR(100),
    published_date DATE,
    views INTEGER,
    investment_opinion VARCHAR(50),
    target_price NUMERIC,
    target_price_text VARCHAR(100),
    content TEXT,
    pdf_url TEXT,
    pdf_path TEXT,
    detail_url TEXT,
    list_url TEXT,
    crawl_time TIMESTAMP WITH TIME ZONE,
    parquet_path TEXT,
    schema_version INTEGER,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    UNIQUE(report_type, report_id)
);

CREATE INDEX IF NOT EXISTS idx_research_stock
ON research_reports(stock_code);

CREATE INDEX IF NOT EXISTS idx_research_date
ON research_reports(published_date);

CREATE INDEX IF NOT EXISTS idx_research_type
ON research_reports(report_type);

CREATE INDEX IF NOT EXISTS idx_research_company
ON research_reports(securities_company);

CREATE INDEX IF NOT EXISTS idx_research_pdf_path
ON research_reports(pdf_path);
