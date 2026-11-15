"""Ingest permissioned, source-neutral game article packages into `game`."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from lib.get_sql import get_sql_from_config


SOURCE_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,95}$")
DEFAULT_INBOX = Path(__file__).with_name("database") / "incoming"
SCHEMA_PATH = Path(__file__).with_name("database") / "schema.sql"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _get_database_connection():
    return get_sql_from_config("game")


def ensure_schema_and_catalog() -> None:
    conn, cursor, engine = _get_database_connection()
    now = _now_utc()
    try:
        for statement in SCHEMA_PATH.read_text(encoding="utf-8").split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        cursor.execute(
            """
            INSERT INTO game_knowledge_sources
                (source_key, source_name, base_url, is_enabled, created_at, updated_at)
            VALUES (%s, %s, %s, 1, %s, %s)
            ON DUPLICATE KEY UPDATE
                source_name = VALUES(source_name), base_url = VALUES(base_url),
                updated_at = VALUES(updated_at)
            """,
            ("bahamut", "巴哈姆特哈啦區", "https://forum.gamer.com.tw", now, now),
        )
        cursor.execute(
            """
            SELECT id FROM game_knowledge_rights_grants
            WHERE source_key = 'bahamut' AND scope_type = 'source' AND scope_key = '*'
              AND permission_basis = 'user_personal_use'
              AND valid_from <= UTC_TIMESTAMP()
              AND (valid_until IS NULL OR valid_until >= UTC_TIMESTAMP())
            ORDER BY id DESC LIMIT 1
            """
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO game_knowledge_rights_grants
                    (source_key, scope_type, scope_key, permission_basis,
                     can_store_metadata, can_store_full_text, can_run_llm_analysis,
                     can_use_commercial_derivatives, valid_from, valid_until,
                     evidence_ref, created_at)
                VALUES (%s, 'source', '*', 'user_personal_use', 1, 1, 1, 0,
                        %s, NULL, %s, %s)
                """,
                (
                    "bahamut", now,
                    "User instruction 2026-11-01: personal-use crawl; keep content in the local game database; no external LLM transfer; no commercial derivatives.",
                    now,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        engine.dispose()


def _required_text(record: dict, field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"package field {field!r} must be a non-empty string")
    return value.strip()


def _parse_datetime(value):
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError("timestamps must be ISO-8601 strings")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _url_belongs_to_source(url: str, base_url: str) -> bool:
    candidate_host = (urlsplit(url).hostname or "").lower()
    source_host = (urlsplit(base_url).hostname or "").lower()
    return bool(candidate_host and source_host and (
        candidate_host == source_host or candidate_host.endswith("." + source_host)
    ))


def _grant_for_article(cursor, record: dict):
    source_key = record["source_key"]
    board_id = record["board_external_id"]
    article_id = record["article_external_id"]
    article_scope = f"{board_id}/{article_id}"
    cursor.execute(
        """
        SELECT id, can_store_metadata, can_store_full_text,
               can_run_llm_analysis, can_use_commercial_derivatives
        FROM game_knowledge_rights_grants
        WHERE source_key = %s
          AND valid_from <= UTC_TIMESTAMP()
          AND (valid_until IS NULL OR valid_until >= UTC_TIMESTAMP())
          AND (
              (scope_type = 'article' AND scope_key = %s)
              OR (scope_type = 'board' AND scope_key = %s)
              OR (scope_type = 'source' AND scope_key = '*')
          )
        ORDER BY CASE scope_type WHEN 'article' THEN 1 WHEN 'board' THEN 2 ELSE 3 END,
                 valid_from DESC, id DESC
        LIMIT 1
        """,
        (source_key, article_scope, board_id),
    )
    return cursor.fetchone()


def _upsert_record(conn, cursor, record: dict) -> dict:
    source_key = _required_text(record, "source_key").lower()
    if not SOURCE_KEY_PATTERN.fullmatch(source_key):
        raise ValueError("source_key must contain lowercase letters, digits, '_' or '-'")
    record["source_key"] = source_key
    for field in (
        "game_key", "game_title", "board_external_id", "board_name", "board_url",
        "article_external_id", "article_url", "title",
    ):
        record[field] = _required_text(record, field)

    posts = record.get("posts", [])
    if not isinstance(posts, list):
        raise ValueError("package field 'posts' must be a list")
    posts_complete = record.get("posts_complete")
    if not isinstance(posts_complete, bool):
        raise ValueError("package field 'posts_complete' must be a boolean")
    expected_post_count = record.get("expected_post_count")
    if expected_post_count is not None and (
        isinstance(expected_post_count, bool)
        or not isinstance(expected_post_count, int)
        or expected_post_count < 1
    ):
        raise ValueError("expected_post_count must be a positive integer")
    if posts_complete:
        if not posts:
            raise ValueError("a completion batch must include at least one post")
        if expected_post_count is not None and expected_post_count < len(posts):
            raise ValueError("expected_post_count cannot be smaller than the completion batch")

    cursor.execute(
        "SELECT is_enabled, base_url FROM game_knowledge_sources WHERE source_key = %s",
        (source_key,),
    )
    source = cursor.fetchone()
    if not source or not source[0]:
        return {"imported": False, "stored_posts": 0, "rights_blocked": 1}
    for url_field in ("board_url", "article_url"):
        url = record[url_field]
        if not url.startswith(("https://", "http://")) or not _url_belongs_to_source(
            url, source[1]
        ):
            raise ValueError(f"{url_field} must be an HTTP(S) URL under the registered source")

    grant = _grant_for_article(cursor, record)
    if not grant or not grant[1]:
        return {"imported": False, "stored_posts": 0, "rights_blocked": 1}

    now = _now_utc()
    source_key = record["source_key"]
    game_key = record["game_key"]
    board_id = record["board_external_id"]
    article_id = record["article_external_id"]
    tags = record.get("tags", [])
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise ValueError("tags must be a list of strings")
    tags_json = json.dumps(tags, ensure_ascii=False)

    cursor.execute(
        """
        INSERT INTO game_knowledge_games (game_key, title, aliases_json, created_at, updated_at)
        VALUES (%s, %s, '[]', %s, %s)
        ON DUPLICATE KEY UPDATE title = VALUES(title), updated_at = VALUES(updated_at)
        """,
        (game_key, record["game_title"], now, now),
    )
    cursor.execute(
        """
        INSERT INTO game_knowledge_boards
            (source_key, board_external_id, game_key, board_name, board_url,
             platform_name, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE game_key = VALUES(game_key),
            board_name = VALUES(board_name), board_url = VALUES(board_url),
            platform_name = VALUES(platform_name), updated_at = VALUES(updated_at)
        """,
        (
            source_key, board_id, game_key, record["board_name"], record["board_url"],
            record.get("platform_name"), now, now,
        ),
    )
    cursor.execute(
        """
        INSERT INTO game_knowledge_articles
            (source_key, board_external_id, article_external_id, game_key, article_url,
             title, article_type, tags_json, published_at, source_updated_at,
             posts_complete, expected_post_count, first_seen_at, last_seen_at,
             created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE game_key = VALUES(game_key),
            article_url = VALUES(article_url), title = VALUES(title),
            article_type = VALUES(article_type), tags_json = VALUES(tags_json),
            published_at = VALUES(published_at), source_updated_at = VALUES(source_updated_at),
            posts_complete = IF(%s, VALUES(posts_complete), posts_complete),
            expected_post_count = IF(%s, VALUES(expected_post_count), expected_post_count),
            last_seen_at = VALUES(last_seen_at), updated_at = VALUES(updated_at)
        """,
        (
            source_key, board_id, article_id, game_key, record["article_url"],
            record["title"], record.get("article_type", "discussion"), tags_json,
            _parse_datetime(record.get("published_at")),
            _parse_datetime(record.get("source_updated_at")), int(posts_complete),
            expected_post_count, now, now, now, now, int(bool(posts)), int(bool(posts)),
        ),
    )

    if not posts:
        conn.commit()
        return {"imported": True, "stored_posts": 0, "rights_blocked": 0}

    stored_posts = rights_blocked = 0
    stored_post_hashes = []
    seen_post_ids = set()
    seen_post_numbers = set()
    full_text_allowed = bool(grant[2])
    for post in posts:
        if not isinstance(post, dict):
            raise ValueError("each post in 'posts' must be an object")
        post_id = _required_text(post, "post_external_id")
        try:
            post_number = int(post["post_number"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("each post needs a positive integer post_number") from exc
        if post_number < 1:
            raise ValueError("post_number must be positive")
        if post_id in seen_post_ids or post_number in seen_post_numbers:
            raise ValueError("posts must have unique post_external_id and post_number values")
        seen_post_ids.add(post_id)
        seen_post_numbers.add(post_number)
        post_type = post.get("post_type", "reply")
        if post_type not in {"original", "reply"}:
            raise ValueError("post_type must be 'original' or 'reply'")
        body = post.get("body_content")
        body_text = post.get("body_text")
        content_format = post.get("content_format", "text/markdown")
        media_refs = post.get("media_refs", [])
        if content_format not in {"text/markdown", "text/html", "text/plain"}:
            raise ValueError("content_format must be text/markdown, text/html or text/plain")
        if body is not None and not isinstance(body, str):
            raise ValueError("body_content must be a string")
        if body_text is not None and not isinstance(body_text, str):
            raise ValueError("body_text must be a string")
        if not isinstance(media_refs, list) or any(not isinstance(ref, dict) for ref in media_refs):
            raise ValueError("media_refs must be a list of objects")
        if body and not (body_text and body_text.strip()):
            if not media_refs:
                raise ValueError(
                    "body_content requires normalized text or a media reference"
                )
            body_text = ""

        cursor.execute(
            """
            INSERT INTO game_knowledge_article_posts
                (source_key, board_external_id, article_external_id, post_external_id,
                 post_number, post_type, published_at, content_status,
                 first_seen_at, last_seen_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'metadata_only', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE post_number = VALUES(post_number),
                post_type = VALUES(post_type), published_at = VALUES(published_at),
                last_seen_at = VALUES(last_seen_at), updated_at = VALUES(updated_at)
            """,
            (
                source_key, board_id, article_id, post_id, post_number, post_type,
                _parse_datetime(post.get("published_at")), now, now, now, now,
            ),
        )

        if not body or not body.strip():
            continue
        if not full_text_allowed:
            cursor.execute(
                """
                UPDATE game_knowledge_article_posts
                SET content_status = 'awaiting_rights', updated_at = %s
                WHERE source_key = %s AND board_external_id = %s
                  AND article_external_id = %s AND post_external_id = %s
                """,
                (now, source_key, board_id, article_id, post_id),
            )
            rights_blocked += 1
            continue

        serialized_content = json.dumps(
            {
                "content_format": content_format,
                "body_content": body,
                "body_text": body_text,
                "media_refs": media_refs,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        content_hash = hashlib.sha256(serialized_content.encode("utf-8")).hexdigest()
        stored_post_hashes.append((post_number, post_id, content_hash))
        cursor.execute(
            """
            INSERT IGNORE INTO game_knowledge_article_versions
                (source_key, board_external_id, article_external_id, post_external_id,
                 content_hash, content_format, body_content, body_text, media_refs_json,
                 rights_grant_id, captured_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                source_key, board_id, article_id, post_id, content_hash,
                content_format, body, body_text,
                json.dumps(media_refs, ensure_ascii=False), grant[0], now,
            ),
        )
        cursor.execute(
            """
            UPDATE game_knowledge_article_posts
                SET content_status = 'full_text_stored', latest_content_hash = %s,
                updated_at = %s
            WHERE source_key = %s AND board_external_id = %s
              AND article_external_id = %s AND post_external_id = %s
            """,
            (content_hash, now, source_key, board_id, article_id, post_id),
        )
        stored_posts += 1

    complete_body_set = False
    if posts_complete:
        cursor.execute(
            """
            SELECT COUNT(*),
                   SUM(content_status = 'full_text_stored'
                       AND latest_content_hash IS NOT NULL)
            FROM game_knowledge_article_posts
            WHERE source_key = %s AND board_external_id = %s AND article_external_id = %s
            """,
            (source_key, board_id, article_id),
        )
        observed_count, authorized_count = cursor.fetchone()
        observed_count = int(observed_count or 0)
        authorized_count = int(authorized_count or 0)
        if expected_post_count is not None and observed_count != expected_post_count:
            raise ValueError(
                "complete thread post count does not match expected_post_count"
            )
        if observed_count == 0 or authorized_count != observed_count:
            raise ValueError("complete threads require an authorized body for every post")
        expected_post_count = observed_count
        cursor.execute(
            """
            SELECT post_number, post_external_id, latest_content_hash
            FROM game_knowledge_article_posts
            WHERE source_key = %s AND board_external_id = %s AND article_external_id = %s
            ORDER BY post_number, post_external_id
            """,
            (source_key, board_id, article_id),
        )
        stored_post_hashes = list(cursor.fetchall())
        complete_body_set = True
    if complete_body_set:
        serialized_hashes = "\n".join(
            f"{post_number}:{post_id}:{content_hash}"
            for post_number, post_id, content_hash in sorted(stored_post_hashes)
        )
        thread_hash = hashlib.sha256(serialized_hashes.encode("utf-8")).hexdigest()
        content_status = "full_text_stored"
    else:
        thread_hash = None
        content_status = (
            "partial" if stored_posts else
            "awaiting_rights" if rights_blocked else
            "metadata_only"
        )
    cursor.execute(
        """
        UPDATE game_knowledge_articles
        SET posts_complete = %s, expected_post_count = %s,
            content_status = %s, latest_content_hash = %s, updated_at = %s
        WHERE source_key = %s AND board_external_id = %s AND article_external_id = %s
        """,
        (
            int(complete_body_set), expected_post_count, content_status, thread_hash,
            now, source_key, board_id, article_id,
        ),
    )

    conn.commit()
    return {
        "imported": True,
        "stored_posts": stored_posts,
        "rights_blocked": rights_blocked,
    }


def ingest_authorized_packages(dag_run_id: str) -> dict:
    inbox = Path(os.environ.get("GAME_KNOWLEDGE_INBOX", str(DEFAULT_INBOX)))
    package_paths = sorted(inbox.glob("*.jsonl")) if inbox.exists() else []
    conn, cursor, engine = _get_database_connection()
    now = _now_utc()
    imported = stored_posts = blocked = read_count = 0
    errors = []
    try:
        cursor.execute(
            """
            INSERT INTO game_knowledge_ingestion_runs
                (dag_run_id, started_at, status)
            VALUES (%s, %s, 'running')
            ON DUPLICATE KEY UPDATE started_at = VALUES(started_at),
                finished_at = NULL, status = 'running', error_summary = NULL
            """,
            (dag_run_id, now),
        )
        conn.commit()
        for package_path in package_paths:
            for line_number, line in enumerate(
                package_path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                if not line.strip():
                    continue
                read_count += 1
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("each JSONL line must be an object")
                    result = _upsert_record(conn, cursor, record)
                    imported += int(result["imported"])
                    stored_posts += result["stored_posts"]
                    blocked += result["rights_blocked"]
                except Exception as exc:
                    conn.rollback()
                    errors.append(f"{package_path.name}:{line_number}: {exc}")

        status = "failed" if errors else "success"
        cursor.execute(
            """
            UPDATE game_knowledge_ingestion_runs
            SET finished_at = %s, package_count = %s, article_count = %s,
                post_body_stored_count = %s, rights_blocked_record_count = %s,
                status = %s, error_summary = %s
            WHERE dag_run_id = %s
            """,
            (
                _now_utc(), len(package_paths), read_count, stored_posts, blocked,
                status, "\n".join(errors)[:60000] if errors else None, dag_run_id,
            ),
        )
        conn.commit()
        if errors:
            raise RuntimeError(f"{len(errors)} package records failed; see ingestion run audit")
        return {
            "package_count": len(package_paths),
            "thread_count": read_count,
            "metadata_imported_count": imported,
            "post_body_stored_count": stored_posts,
            "rights_blocked_record_count": blocked,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        engine.dispose()
