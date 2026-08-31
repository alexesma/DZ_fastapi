#!/usr/bin/env python3
"""Внешний релей EmailOutbox и TelegramOutbox (DZ_fastapi).

На проде исходящие SMTP-порты закрыты хостером, а ответы клиентам должны
уходить С адреса Яндекс-ящика. Приложение кладёт письма в очередь, а этот
скрипт (запускается на машине с открытыми портами — например, на твоём
компьютере) забирает их по HTTPS и отправляет через smtp.yandex.ru:465.

Цикл:
  1. передать X-Email-Relay-Token  — сервисная авторизация;
  2. POST /email-outbox/claim      — атомарно забрать письма к отправке;
  3. отправить через SMTP с нужного from-адреса;
  4. POST /email-outbox/{id}/mark-sent  или  /mark-error;
  5. таким же образом обработать /telegram-outbox через Telegram Bot API.

Запуск:
  pip install -r requirements.txt
  python relay.py --config config.json           # бесконечный цикл
  python relay.py --config config.json --once     # один проход
  python relay.py --config config.json --dry-run  # показать, но не отправлять

Несколько машин можно запускать против одной очереди: серверный claim
атомарно закрепляет письма за конкретным worker_id.
"""
import argparse
import base64
import imaplib
import json
import logging
import os
import smtplib
import socket
import sys
import time
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pathlib import Path

import requests

logger = logging.getLogger("email_relay")


def configure_stdio() -> None:
    """Keep Russian console output working on every Windows locale."""
    for stream in (sys.stdout, sys.stderr):
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError):
            pass


def application_dir() -> Path:
    """Return the portable app directory for source and PyInstaller builds."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def default_config_path() -> Path:
    return application_dir() / "config.json"


def configure_logging(config_path: Path) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        handlers.append(
            logging.FileHandler(
                config_path.parent / "relay.log",
                encoding="utf-8",
            )
        )
    except OSError:
        # The console remains available if the folder is read-only.
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )


class RelayConfig:
    def __init__(self, data: dict):
        self.api_base_url = str(data["api_base_url"]).rstrip("/")
        self.relay_api_token = str(data["relay_api_token"]).strip()
        self.poll_interval_seconds = int(data.get("poll_interval_seconds", 30))
        self.batch_limit = int(data.get("batch_limit", 25))
        self.verify_tls = bool(data.get("verify_tls", True))
        self.request_timeout = int(data.get("request_timeout_seconds", 30))
        # Уникальный идентификатор этой машины/процесса для «захвата» писем
        self.worker_id = str(
            data.get("worker_id")
            or f"{socket.gethostname()}:{os.getpid()}"
        )[:128]
        self.claim_lease_seconds = int(data.get("claim_lease_seconds", 300))
        # from_email -> { host, port, username, password, use_ssl }
        self.smtp_accounts = {
            str(k).strip().lower(): v
            for k, v in (data.get("smtp_accounts") or {}).items()
        }
        # запасной аккаунт, если from не совпал ни с одним ключом
        self.default_smtp = data.get("default_smtp")
        telegram = data.get("telegram") or {}
        self.telegram_enabled = bool(
            telegram.get("enabled", bool(telegram.get("bot_token")))
        )
        self.telegram_bot_token = str(
            telegram.get("bot_token") or ""
        ).strip()
        self.telegram_api_base_url = str(
            telegram.get("api_base_url") or "https://api.telegram.org"
        ).rstrip("/")
        self.telegram_timeout = int(
            telegram.get("request_timeout_seconds") or 45
        )
        self.telegram_proxy_url = str(
            telegram.get("proxy_url") or ""
        ).strip() or None
        if self.telegram_enabled and not self.telegram_bot_token:
            raise ValueError(
                "telegram.enabled=true, но telegram.bot_token не задан"
            )

    def smtp_for(self, from_email: str | None):
        key = str(from_email or "").strip().lower()
        if key in self.smtp_accounts:
            return self.smtp_accounts[key]
        return self.default_smtp


def load_config(path: str) -> RelayConfig:
    with open(path, "r", encoding="utf-8") as fh:
        return RelayConfig(json.load(fh))


class ApiClient:
    def __init__(self, config: RelayConfig):
        self.config = config
        self.session = requests.Session()
        self.session.verify = config.verify_tls
        self.session.headers["X-Email-Relay-Token"] = (
            config.relay_api_token
        )

    def _url(self, path: str) -> str:
        return f"{self.config.api_base_url}{path}"

    def _request_with_transport_retry(self, method: str, path: str, **kwargs):
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                return self.session.request(
                    method,
                    self._url(path),
                    timeout=self.config.request_timeout,
                    **kwargs,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt >= attempts:
                    raise
                delay = 2 ** (attempt - 1)
                logger.warning(
                    "Временный обрыв HTTPS при запросе %s. "
                    "Повтор %d/%d через %d с: %s",
                    path,
                    attempt + 1,
                    attempts,
                    delay,
                    exc,
                )
                time.sleep(delay)
        raise RuntimeError("Недостижимый код повтора HTTP-запроса")

    def login(self) -> None:
        logger.info("Сервисный токен релея настроен")

    def _get_with_retry(self, path: str, **kwargs):
        resp = self._request_with_transport_retry("GET", path, **kwargs)
        resp.raise_for_status()
        return resp

    def _post_with_retry(self, path: str, **kwargs):
        resp = self._request_with_transport_retry("POST", path, **kwargs)
        resp.raise_for_status()
        return resp

    def pending(self) -> list[dict]:
        resp = self._get_with_retry(
            "/email-outbox/pending",
            params={"limit": self.config.batch_limit},
        )
        return resp.json()

    def claim(self) -> list[dict]:
        """Атомарно захватить письма за этим воркером (безопасно при
        нескольких машинах). Если сервер старый и эндпоинта нет (404) —
        откатываемся на pending."""
        resp = self._request_with_transport_retry(
            "POST",
            "/email-outbox/claim",
            params={
                "worker": self.config.worker_id,
                "limit": self.config.batch_limit,
                "lease_seconds": self.config.claim_lease_seconds,
            },
        )
        if resp.status_code == 404:
            logger.warning(
                "Эндпоинт /email-outbox/claim недоступен — использую "
                "/pending (без защиты от дублей при нескольких машинах)"
            )
            return self.pending()
        resp.raise_for_status()
        return resp.json()

    def mark_sent(self, outbox_id: int) -> None:
        self._post_with_retry(f"/email-outbox/{outbox_id}/mark-sent")

    def mark_error(self, outbox_id: int, error: str, retry: bool = True) -> None:
        self._post_with_retry(
            f"/email-outbox/{outbox_id}/mark-error",
            json={"error": error[:2000], "retry": retry},
        )

    def telegram_pending(self) -> list[dict]:
        resp = self._get_with_retry(
            "/telegram-outbox/pending",
            params={"limit": self.config.batch_limit},
        )
        return resp.json()

    def telegram_claim(self) -> list[dict]:
        resp = self._request_with_transport_retry(
            "POST",
            "/telegram-outbox/claim",
            params={
                "worker": self.config.worker_id,
                "limit": self.config.batch_limit,
                "lease_seconds": self.config.claim_lease_seconds,
            },
        )
        if resp.status_code == 404:
            logger.warning(
                "Эндпоинт /telegram-outbox/claim недоступен — "
                "использую /pending"
            )
            return self.telegram_pending()
        resp.raise_for_status()
        return resp.json()

    def telegram_mark_sent(self, outbox_id: int) -> None:
        self._post_with_retry(
            f"/telegram-outbox/{outbox_id}/mark-sent"
        )

    def telegram_mark_error(
        self,
        outbox_id: int,
        error: str,
        retry: bool = True,
    ) -> None:
        self._post_with_retry(
            f"/telegram-outbox/{outbox_id}/mark-error",
            json={"error": error[:2000], "retry": retry},
        )


def build_message(item: dict, from_email: str) -> MIMEMultipart:
    msg = MIMEMultipart("mixed")
    msg["From"] = from_email
    msg["To"] = item["to_email"]
    subject = item.get("subject") or ""
    msg["Subject"] = str(Header(subject, "utf-8"))
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    if item.get("reply_to"):
        msg["Reply-To"] = item["reply_to"]
    if item.get("in_reply_to"):
        msg["In-Reply-To"] = item["in_reply_to"]
    if item.get("references"):
        msg["References"] = item["references"]

    alt = MIMEMultipart("alternative")
    body_text = item.get("body_text") or ""
    alt.attach(MIMEText(body_text, "plain", "utf-8"))
    if item.get("body_html"):
        alt.attach(MIMEText(item["body_html"], "html", "utf-8"))
    msg.attach(alt)

    for att in item.get("attachments") or []:
        content_b64 = att.get("content_base64") or att.get("content")
        if not content_b64:
            continue
        try:
            payload = base64.b64decode(content_b64)
        except Exception:  # noqa: BLE001
            continue
        part = MIMEApplication(payload)
        filename = att.get("filename") or "attachment"
        part.add_header(
            "Content-Disposition", "attachment", filename=str(filename)
        )
        msg.attach(part)
    return msg


def send_via_smtp(smtp_cfg: dict, from_email: str, to_email: str,
                  message: MIMEMultipart) -> None:
    host = smtp_cfg.get("host", "smtp.yandex.ru")
    port = int(smtp_cfg.get("port", 465))
    username = smtp_cfg.get("username") or from_email
    password = smtp_cfg["password"]
    use_ssl = bool(smtp_cfg.get("use_ssl", True))

    if use_ssl:
        server = smtplib.SMTP_SSL(host, port, timeout=60)
    else:
        server = smtplib.SMTP(host, port, timeout=60)
        server.starttls()
    try:
        server.login(username, password)
        server.sendmail(from_email, [to_email], message.as_string())
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001
            pass


def save_copy_to_sent(
    smtp_cfg: dict,
    from_email: str,
    message: MIMEMultipart,
) -> None:
    if not bool(smtp_cfg.get("save_to_sent", True)):
        return
    smtp_host = str(smtp_cfg.get("host") or "smtp.yandex.ru")
    imap_host = str(
        smtp_cfg.get("imap_host")
        or smtp_host.replace("smtp.", "imap.", 1)
    )
    imap_port = int(smtp_cfg.get("imap_port", 993))
    sent_folder = str(smtp_cfg.get("sent_folder") or "Sent")
    username = str(smtp_cfg.get("username") or from_email)
    password = smtp_cfg["password"]

    mailbox = imaplib.IMAP4_SSL(imap_host, imap_port, timeout=30)
    try:
        mailbox.login(username, password)
        status, _ = mailbox.append(
            sent_folder,
            r"(\Seen)",
            imaplib.Time2Internaldate(time.time()),
            message.as_bytes(),
        )
        if status != "OK":
            raise RuntimeError(
                f"IMAP APPEND в папку {sent_folder!r} вернул {status}"
            )
    finally:
        try:
            mailbox.logout()
        except Exception:  # noqa: BLE001
            pass


def process_once(client: ApiClient, config: RelayConfig,
                 dry_run: bool = False) -> int:
    # dry-run только смотрит (pending, без захвата); рабочий цикл — claim.
    items = client.pending() if dry_run else client.claim()
    if not items:
        return 0
    logger.info("Получено писем к отправке: %d", len(items))
    sent = 0
    for item in items:
        outbox_id = item["id"]
        attachment_errors = item.get("attachment_errors") or []
        if attachment_errors:
            msg = "; ".join(str(error) for error in attachment_errors)
            logger.error("Письмо #%s: %s", outbox_id, msg)
            if not dry_run:
                client.mark_error(outbox_id, msg, retry=False)
            continue
        from_email = item.get("from_email")
        smtp_cfg = config.smtp_for(from_email)
        if not smtp_cfg:
            msg = (
                f"Нет SMTP-настроек для отправителя {from_email!r} "
                f"(добавьте в smtp_accounts)"
            )
            logger.error("Письмо #%s: %s", outbox_id, msg)
            if not dry_run:
                # без ретрая — повтор не поможет, пока не поправят конфиг
                client.mark_error(outbox_id, msg, retry=False)
            continue

        if dry_run:
            logger.info(
                "[dry-run] #%s → %s (from %s): %s",
                outbox_id, item.get("to_email"), from_email,
                item.get("subject"),
            )
            continue

        try:
            message = build_message(item, from_email or smtp_cfg.get("username"))
            send_via_smtp(
                smtp_cfg,
                from_email or smtp_cfg.get("username"),
                item["to_email"],
                message,
            )
            try:
                save_copy_to_sent(
                    smtp_cfg,
                    from_email or smtp_cfg.get("username"),
                    message,
                )
            except Exception:  # noqa: BLE001
                # SMTP уже прошёл: не возвращаем письмо в очередь, иначе
                # следующий цикл отправит клиенту дубль.
                logger.exception(
                    "Письмо #%s отправлено, но копия не сохранена в Sent",
                    outbox_id,
                )
            client.mark_sent(outbox_id)
            sent += 1
            logger.info(
                "Отправлено #%s → %s", outbox_id, item.get("to_email")
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка отправки #%s", outbox_id)
            try:
                client.mark_error(outbox_id, str(exc), retry=True)
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось отметить ошибку #%s", outbox_id)
    return sent


class TelegramSendError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


def send_to_telegram(config: RelayConfig, item: dict) -> None:
    method = "sendDocument" if item.get("document_base64") else "sendMessage"
    url = (
        f"{config.telegram_api_base_url}/bot"
        f"{config.telegram_bot_token}/{method}"
    )
    proxies = None
    if config.telegram_proxy_url:
        proxies = {
            "http": config.telegram_proxy_url,
            "https": config.telegram_proxy_url,
        }

    try:
        if method == "sendDocument":
            payload = base64.b64decode(
                item["document_base64"],
                validate=True,
            )
            response = requests.post(
                url,
                data={
                    "chat_id": item["chat_id"],
                    "caption": item.get("caption") or "",
                },
                files={
                    "document": (
                        item.get("document_name") or "document.bin",
                        payload,
                        item.get("document_content_type")
                        or "application/octet-stream",
                    )
                },
                timeout=config.telegram_timeout,
                proxies=proxies,
            )
        else:
            data = {
                "chat_id": item["chat_id"],
                "text": item.get("text") or "",
            }
            if item.get("parse_mode"):
                data["parse_mode"] = item["parse_mode"]
            response = requests.post(
                url,
                data=data,
                timeout=config.telegram_timeout,
                proxies=proxies,
            )
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise TelegramSendError(str(exc), retryable=True) from exc
    except (ValueError, TypeError) as exc:
        raise TelegramSendError(
            f"Повреждён Telegram-документ: {exc}",
            retryable=False,
        ) from exc

    try:
        response_data = response.json()
    except ValueError:
        response_data = {}
    if response.ok and response_data.get("ok", True):
        return
    description = str(
        response_data.get("description") or response.text or response.status_code
    )
    retryable = response.status_code == 429 or response.status_code >= 500
    raise TelegramSendError(
        f"Telegram HTTP {response.status_code}: {description}",
        retryable=retryable,
    )


def process_telegram_once(
    client: ApiClient,
    config: RelayConfig,
    dry_run: bool = False,
) -> int:
    if not config.telegram_enabled:
        return 0
    items = (
        client.telegram_pending() if dry_run else client.telegram_claim()
    )
    if not items:
        return 0
    logger.info("Получено Telegram-сообщений к отправке: %d", len(items))
    sent = 0
    for item in items:
        outbox_id = item["id"]
        document_error = item.get("document_error")
        if document_error:
            logger.error("Telegram #%s: %s", outbox_id, document_error)
            if not dry_run:
                client.telegram_mark_error(
                    outbox_id,
                    str(document_error),
                    retry=False,
                )
            continue
        if dry_run:
            logger.info(
                "[dry-run] Telegram #%s → %s: %s",
                outbox_id,
                item.get("chat_id"),
                item.get("document_name") or item.get("text"),
            )
            continue
        try:
            send_to_telegram(config, item)
            client.telegram_mark_sent(outbox_id)
            sent += 1
            logger.info(
                "Отправлено Telegram #%s → %s",
                outbox_id,
                item.get("chat_id"),
            )
        except TelegramSendError as exc:
            logger.exception("Ошибка отправки Telegram #%s", outbox_id)
            try:
                client.telegram_mark_error(
                    outbox_id,
                    str(exc),
                    retry=exc.retryable,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Не удалось отметить ошибку Telegram #%s",
                    outbox_id,
                )
    return sent


def main() -> int:
    configure_stdio()
    parser = argparse.ArgumentParser(
        description="Релей EmailOutbox и TelegramOutbox"
    )
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="путь к config.json (по умолчанию рядом с программой)",
    )
    parser.add_argument("--once", action="store_true", help="один проход")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="показать письма, но не отправлять и не отмечать",
    )
    args = parser.parse_args()

    config_path = Path(args.config).expanduser().resolve()
    configure_logging(config_path)
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Не найден файл настроек: {config_path}. "
            "Положите config.json рядом с DZEmailRelay.exe."
        )
    config = load_config(str(config_path))
    client = ApiClient(config)
    client.login()

    if args.once or args.dry_run:
        process_once(client, config, dry_run=args.dry_run)
        process_telegram_once(client, config, dry_run=args.dry_run)
        return 0

    logger.info(
        "Релей запущен. Опрос каждые %d c. Ctrl+C для остановки.",
        config.poll_interval_seconds,
    )
    while True:
        try:
            process_once(client, config)
            process_telegram_once(client, config)
        except requests.RequestException as exc:
            logger.warning(
                "Сервер временно недоступен после повторных попыток: %s. "
                "Следующий опрос через %d с.",
                exc,
                config.poll_interval_seconds,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Непредвиденная ошибка в цикле релея")
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.info("Релей остановлен пользователем")
        sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Релей не запущен: %s", exc)
        if getattr(sys, "frozen", False) and sys.stdin.isatty():
            try:
                input("\nНажмите Enter, чтобы закрыть окно...")
            except EOFError:
                pass
        sys.exit(1)
