CREATE TABLE IF NOT EXISTS game_knowledge_sources (
    source_key VARCHAR(96) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    base_url VARCHAR(1024) NOT NULL,
    is_enabled TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_games (
    game_key VARCHAR(160) NOT NULL,
    title VARCHAR(512) NOT NULL,
    aliases_json LONGTEXT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (game_key),
    KEY ix_game_knowledge_games_title (title(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_boards (
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    game_key VARCHAR(160) NOT NULL,
    board_name VARCHAR(512) NOT NULL,
    board_url VARCHAR(1024) NOT NULL,
    platform_name VARCHAR(128) NULL,
    is_enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, board_external_id),
    KEY ix_game_knowledge_boards_game (game_key),
    CONSTRAINT fk_game_knowledge_boards_source
        FOREIGN KEY (source_key) REFERENCES game_knowledge_sources (source_key),
    CONSTRAINT fk_game_knowledge_boards_game
        FOREIGN KEY (game_key) REFERENCES game_knowledge_games (game_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_board_candidates (
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    board_name VARCHAR(512) NULL,
    board_url VARCHAR(1024) NOT NULL,
    candidate_game_title VARCHAR(512) NULL,
    mapped_game_key VARCHAR(160) NULL,
    mapping_source VARCHAR(24) NOT NULL DEFAULT 'unmapped',
    taxonomy_path_json LONGTEXT NULL,
    discovery_method VARCHAR(64) NOT NULL,
    review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
    discovered_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, board_external_id),
    KEY ix_game_knowledge_board_candidates_review (review_status, updated_at),
    KEY ix_game_knowledge_candidates_discovery
        (source_key, discovery_method, review_status, mapping_source),
    CONSTRAINT fk_game_knowledge_board_candidates_source
        FOREIGN KEY (source_key) REFERENCES game_knowledge_sources (source_key),
    CONSTRAINT fk_game_knowledge_board_candidates_game
        FOREIGN KEY (mapped_game_key) REFERENCES game_knowledge_games (game_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_directory_pages (
    source_key VARCHAR(96) NOT NULL,
    category_external_id VARCHAR(96) NOT NULL,
    category_url VARCHAR(1024) NOT NULL,
    parent_category_id VARCHAR(96) NULL,
    discovered_at DATETIME NOT NULL,
    scanned_at DATETIME NULL,
    PRIMARY KEY (source_key, category_external_id),
    KEY ix_game_knowledge_directory_unscanned (source_key, scanned_at, category_external_id),
    CONSTRAINT fk_game_knowledge_directory_pages_source
        FOREIGN KEY (source_key) REFERENCES game_knowledge_sources (source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_directory_taxonomy (
    source_key VARCHAR(96) NOT NULL,
    category_external_id VARCHAR(96) NOT NULL,
    taxonomy_path_json LONGTEXT NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, category_external_id),
    CONSTRAINT fk_game_knowledge_directory_taxonomy_page
        FOREIGN KEY (source_key, category_external_id)
        REFERENCES game_knowledge_directory_pages (source_key, category_external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_discovery_state (
    source_key VARCHAR(96) NOT NULL,
    state_key VARCHAR(64) NOT NULL,
    checked_at DATETIME NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, state_key),
    CONSTRAINT fk_game_knowledge_discovery_state_source
        FOREIGN KEY (source_key) REFERENCES game_knowledge_sources (source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_board_scan_state (
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    next_listing_page INT UNSIGNED NOT NULL DEFAULT 1,
    listing_complete TINYINT(1) NOT NULL DEFAULT 0,
    last_scanned_at DATETIME NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, board_external_id),
    CONSTRAINT fk_game_knowledge_board_scan_state_board
        FOREIGN KEY (source_key, board_external_id)
        REFERENCES game_knowledge_boards (source_key, board_external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_rights_grants (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_key VARCHAR(96) NOT NULL,
    scope_type VARCHAR(16) NOT NULL,
    scope_key VARCHAR(321) NOT NULL,
    permission_basis VARCHAR(32) NOT NULL,
    can_store_metadata TINYINT(1) NOT NULL DEFAULT 0,
    can_store_full_text TINYINT(1) NOT NULL DEFAULT 0,
    can_run_llm_analysis TINYINT(1) NOT NULL DEFAULT 0,
    can_use_commercial_derivatives TINYINT(1) NOT NULL DEFAULT 0,
    valid_from DATETIME NOT NULL,
    valid_until DATETIME NULL,
    evidence_ref VARCHAR(1024) NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    KEY ix_game_knowledge_grants_scope (source_key, scope_type, scope_key, valid_until),
    CONSTRAINT fk_game_knowledge_grants_source
        FOREIGN KEY (source_key) REFERENCES game_knowledge_sources (source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_articles (
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    article_external_id VARCHAR(160) NOT NULL,
    game_key VARCHAR(160) NOT NULL,
    article_url VARCHAR(1024) NOT NULL,
    title VARCHAR(1024) NOT NULL,
    article_type VARCHAR(64) NOT NULL DEFAULT 'discussion',
    tags_json LONGTEXT NULL,
    published_at DATETIME NULL,
    source_updated_at DATETIME NULL,
    posts_complete TINYINT(1) NOT NULL DEFAULT 0,
    expected_post_count INT UNSIGNED NULL,
    next_post_page INT UNSIGNED NOT NULL DEFAULT 1,
    last_post_page_at DATETIME NULL,
    thread_page_count INT UNSIGNED NULL,
    content_status VARCHAR(32) NOT NULL DEFAULT 'metadata_only',
    latest_content_hash CHAR(64) NULL,
    first_seen_at DATETIME NOT NULL,
    last_seen_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, board_external_id, article_external_id),
    KEY ix_game_knowledge_articles_game_date (game_key, published_at),
    KEY ix_game_knowledge_articles_type_date (article_type, published_at),
    KEY ix_game_knowledge_articles_completeness
        (posts_complete, content_status, published_at),
    KEY ix_game_knowledge_articles_post_progress
        (source_key, posts_complete, last_post_page_at, board_external_id, article_external_id),
    CONSTRAINT fk_game_knowledge_articles_board
        FOREIGN KEY (source_key, board_external_id)
        REFERENCES game_knowledge_boards (source_key, board_external_id),
    CONSTRAINT fk_game_knowledge_articles_game
        FOREIGN KEY (game_key) REFERENCES game_knowledge_games (game_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_article_posts (
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    article_external_id VARCHAR(160) NOT NULL,
    post_external_id VARCHAR(160) NOT NULL,
    post_number INT UNSIGNED NOT NULL,
    post_type VARCHAR(32) NOT NULL DEFAULT 'reply',
    published_at DATETIME NULL,
    content_status VARCHAR(32) NOT NULL DEFAULT 'metadata_only',
    latest_content_hash CHAR(64) NULL,
    first_seen_at DATETIME NOT NULL,
    last_seen_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (source_key, board_external_id, article_external_id, post_external_id),
    KEY ix_game_knowledge_posts_article_number
        (source_key, board_external_id, article_external_id, post_number),
    CONSTRAINT fk_game_knowledge_posts_article
        FOREIGN KEY (source_key, board_external_id, article_external_id)
        REFERENCES game_knowledge_articles (source_key, board_external_id, article_external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_article_versions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    article_external_id VARCHAR(160) NOT NULL,
    post_external_id VARCHAR(160) NOT NULL,
    content_hash CHAR(64) NOT NULL,
    content_format VARCHAR(32) NOT NULL,
    body_content LONGTEXT NOT NULL,
    body_text LONGTEXT NOT NULL,
    media_refs_json LONGTEXT NULL,
    rights_grant_id BIGINT UNSIGNED NOT NULL,
    captured_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_game_knowledge_article_version
        (source_key, board_external_id, article_external_id, post_external_id, content_hash),
    KEY ix_game_knowledge_article_versions_article
        (source_key, board_external_id, article_external_id, post_external_id, captured_at),
    CONSTRAINT fk_game_knowledge_article_versions_post
        FOREIGN KEY (source_key, board_external_id, article_external_id, post_external_id)
        REFERENCES game_knowledge_article_posts
            (source_key, board_external_id, article_external_id, post_external_id),
    CONSTRAINT fk_game_knowledge_article_versions_grant
        FOREIGN KEY (rights_grant_id) REFERENCES game_knowledge_rights_grants (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_crawl_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dag_run_id VARCHAR(250) NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    request_count INT UNSIGNED NOT NULL DEFAULT 0,
    board_candidate_count INT UNSIGNED NOT NULL DEFAULT 0,
    board_page_count INT UNSIGNED NOT NULL DEFAULT 0,
    thread_page_count INT UNSIGNED NOT NULL DEFAULT 0,
    completed_article_count INT UNSIGNED NOT NULL DEFAULT 0,
    rights_blocked_count INT UNSIGNED NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    error_summary TEXT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_game_knowledge_crawl_run (dag_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE OR REPLACE VIEW game_knowledge_analysis_input_posts AS
SELECT
    a.source_key,
    g.game_key,
    g.title AS game_title,
    b.board_external_id,
    b.board_name,
    a.article_external_id,
    a.article_url,
    a.title AS article_title,
    a.article_type,
    a.tags_json,
    p.post_external_id,
    p.post_number,
    p.post_type,
    p.published_at AS post_published_at,
    v.content_hash,
    v.content_format,
    v.body_content,
    v.body_text,
    v.media_refs_json,
    v.rights_grant_id
FROM game_knowledge_articles a
JOIN game_knowledge_sources s ON s.source_key = a.source_key
JOIN game_knowledge_games g ON g.game_key = a.game_key
JOIN game_knowledge_boards b
  ON b.source_key = a.source_key AND b.board_external_id = a.board_external_id
JOIN game_knowledge_article_posts p
  ON p.source_key = a.source_key
 AND p.board_external_id = a.board_external_id
 AND p.article_external_id = a.article_external_id
JOIN game_knowledge_article_versions v
  ON v.source_key = p.source_key
 AND v.board_external_id = p.board_external_id
 AND v.article_external_id = p.article_external_id
 AND v.post_external_id = p.post_external_id
 AND v.content_hash = p.latest_content_hash
JOIN game_knowledge_rights_grants r ON r.id = v.rights_grant_id
WHERE s.is_enabled = 1
  AND b.is_enabled = 1
  AND a.posts_complete = 1
  AND a.content_status = 'full_text_stored'
  AND p.content_status = 'full_text_stored'
  AND r.can_store_metadata = 1
  AND r.can_store_full_text = 1
  AND r.can_run_llm_analysis = 1
  AND r.valid_from <= UTC_TIMESTAMP()
  AND (r.valid_until IS NULL OR r.valid_until >= UTC_TIMESTAMP());

CREATE TABLE IF NOT EXISTS game_knowledge_design_analyses (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_key VARCHAR(96) NOT NULL,
    board_external_id VARCHAR(160) NOT NULL,
    article_external_id VARCHAR(160) NOT NULL,
    input_content_hash CHAR(64) NOT NULL,
    rights_grant_id BIGINT UNSIGNED NOT NULL,
    analysis_schema_version VARCHAR(32) NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(32) NOT NULL,
    gameplay_loop_json LONGTEXT NULL,
    mechanics_json LONGTEXT NULL,
    progression_json LONGTEXT NULL,
    strategy_patterns_json LONGTEXT NULL,
    distinctive_features_json LONGTEXT NULL,
    evidence_json LONGTEXT NULL,
    confidence DECIMAL(5,4) NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_game_knowledge_analysis_version
        (source_key, board_external_id, article_external_id, input_content_hash,
         analysis_schema_version, model_name, prompt_version),
    CONSTRAINT fk_game_knowledge_analysis_article
        FOREIGN KEY (source_key, board_external_id, article_external_id)
        REFERENCES game_knowledge_articles (source_key, board_external_id, article_external_id),
    CONSTRAINT fk_game_knowledge_analysis_grant
        FOREIGN KEY (rights_grant_id) REFERENCES game_knowledge_rights_grants (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS game_knowledge_ingestion_runs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dag_run_id VARCHAR(250) NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    package_count INT UNSIGNED NOT NULL DEFAULT 0,
    article_count INT UNSIGNED NOT NULL DEFAULT 0,
    post_body_stored_count INT UNSIGNED NOT NULL DEFAULT 0,
    rights_blocked_record_count INT UNSIGNED NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    error_summary TEXT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_game_knowledge_ingestion_run (dag_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE OR REPLACE VIEW game_knowledge_board_ingestion_status AS
SELECT
    b.source_key,
    b.game_key,
    g.title AS game_title,
    b.board_external_id,
    b.board_name,
    b.is_enabled,
    COALESCE(s.listing_complete, 0) AS listing_complete,
    COALESCE(s.next_listing_page, 1) AS next_listing_page,
    s.last_scanned_at,
    COUNT(a.article_external_id) AS indexed_article_count,
    SUM(CASE
        WHEN a.posts_complete = 1 AND a.content_status = 'full_text_stored' THEN 1
        ELSE 0
    END) AS complete_article_count,
    SUM(CASE
        WHEN a.article_external_id IS NOT NULL
         AND (a.posts_complete = 0 OR a.content_status <> 'full_text_stored') THEN 1
        ELSE 0
    END) AS pending_article_count
FROM game_knowledge_boards b
JOIN game_knowledge_games g ON g.game_key = b.game_key
LEFT JOIN game_knowledge_board_scan_state s
  ON s.source_key = b.source_key AND s.board_external_id = b.board_external_id
LEFT JOIN game_knowledge_articles a
  ON a.source_key = b.source_key AND a.board_external_id = b.board_external_id
GROUP BY
    b.source_key, b.game_key, g.title, b.board_external_id, b.board_name,
    b.is_enabled, s.listing_complete, s.next_listing_page, s.last_scanned_at;

CREATE OR REPLACE VIEW game_knowledge_game_ingestion_status AS
SELECT
    source_key,
    game_key,
    game_title,
    COUNT(*) AS active_board_count,
    SUM(listing_complete) AS listing_complete_board_count,
    SUM(indexed_article_count) AS indexed_article_count,
    SUM(complete_article_count) AS complete_article_count,
    SUM(pending_article_count) AS pending_article_count
FROM game_knowledge_board_ingestion_status
WHERE is_enabled = 1
GROUP BY source_key, game_key, game_title;
