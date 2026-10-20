#!/usr/bin/env python3
"""Telegram mirror service, Python 3.11+.

INSTALL / RENDER
  pip install -r requirements.txt
  Settings: Render > Environment tab (recommended), or the CONFIG block near the
  top of this file. Environment variables always win over CONFIG values.
    BOT_TOKEN       token from @BotFather
    OWNER_ID        your numeric Telegram user id (alias: ADMIN_ID), from @userinfobot
    ENCRYPTION_KEY  Fernet key. Generate one with:  python bot.py --gen-key
    DATABASE_URL    durable PostgreSQL URL (if empty: temporary SQLite + warning)
    PUBLIC_URL      optional; Render supplies RENDER_EXTERNAL_URL automatically
  Validate everything without starting:  python bot.py --check-config
  Python: use 3.12 (Render's default is newer; set PYTHON_VERSION=3.12.8 or add a
  .python-version file containing 3.12).
  Local SQLite: DATABASE_URL=sqlite+aiosqlite:///mirror.db
  Render: use durable external PostgreSQL, not local SQLite. Build command:
  pip install -r requirements.txt. Start command: python bot.py
  Render supplies PORT and RENDER_EXTERNAL_URL automatically; these are hosting
  metadata, not additional application secrets. Health endpoint: /healthz.
  Outside Render: python bot.py --public-url https://your-public-https-host
  The HTTPS reverse proxy must forward to PORT (default 8080).
  Send /start to the bot in PRIVATE chat. Add account, add pair, edit settings,
  preview, then enable. The service must run as ONE process / ONE replica.
  Run local unit tests with: python bot.py --self-test
  Keep ENCRYPTION_KEY stable across redeploys; back up the database securely.
  PostgreSQL connections: use the provider's SSL-enabled URL/SSL configuration.
  The script creates a fresh schema; existing-schema upgrades need migrations.

  STAGING ACCEPTANCE (manual; not reported as tested by this delivery)
  1. Phone login: wrong code, keypad code, 2FA wrong/correct, /cancel, expiry.
  2. QR login: expiry/refresh, cancellation, scan, optional 2FA.
  3. Private request: pending -> approval -> notification -> only new posts.
  4. Test every media type and mixed photo/video albums; compare spoilers.
  5. Test astral emoji before/inside styled text, hidden URLs, usernames.
  6. Reply to a copied/uncopied parent; test mapped/unmapped public/private links.
  7. Edit/delete, link filters, keyword filters, OFF/ON and global pause/resume.
  8. Restart with pending work; kill during send; reconcile uncertain deliveries.
  9. Revoke session, remove channel rights, simulate a flood wait.
  These checks require your own authorized Telegram test channels.

IMPORTANT CONTRACT / LIMITATIONS
  This is an implementation with embedded tests, NOT a live-verified deployment.
  Live Telegram login, permissions, delivery and Render were not tested here.
  No system can promise 1-3 seconds for uploads, flood waits or a sleeping host.
  Free Render sleeps and has ephemeral storage; production needs always-on
  hosting and durable PostgreSQL. No self-pinging to evade platform limits.
  A bot sometimes CAN read channel updates when it is a channel member/admin;
  a user account is needed here for arbitrary joined source channels.
  Only mirror content you have permission to republish. Media is downloaded
  and re-uploaded when source references cannot be reused. An account's ability
  to access content is not proof of permission to republish it.
  API calls are not a database transaction. An unknown send outcome is quarantined
  as 'uncertain', never retried silently. The operator resolves it in Jobs. This
  avoids claiming impossible Bot API exactly-once delivery.
  Edits without a text/media/caption equivalent fail visibly. Poll vote state,
  paid media, giveaways, live locations, games, forwarded attribution, exact
  quote selection, poll edits, album membership changes and caption-above-media
  in MTProto mode are NOT mirrored. All ordinary media types listed in the UI
  are implemented. Source URL buttons can be rewritten/removed; callback buttons
  cannot be transferred between bots and are omitted with an owner warning.
  Deletes require a Telegram deletion update; channels provide no complete
  historical deletion feed. Pending joins cannot reliably distinguish rejection
  from an unanswered request. Both remain pending until user action or expiry.
  Read-only restart catch-up fetches NEW posts since each activation watermark;
  it does not discover edits/deletions that Telegram did not replay.
  One account is supported (replacement/logout supported). Pair assignment and
  multiple optional panel admins are intentionally not implemented.
  All pair options are editable in bot (typed prompts and JSON for list values),
  not config files. Credential drafts exist only in RAM, expire in 10 minutes,
  and are never stored unless login succeeds. Telegram deletion is best effort;
  bot chats are not end-to-end encrypted. QR login is preferable to typing 2FA.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import hashlib
import hmac
import io
import json
import logging
import os
import re
import sys
import tempfile
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNetworkError, TelegramRetryAfter
from aiogram.types import (BufferedInputFile, CallbackQuery, FSInputFile, InlineKeyboardButton,
                           InlineKeyboardMarkup, InputMediaAudio, InputMediaDocument,
                           InputMediaPhoto, InputMediaVideo, InputPollOption, Message,
                           MessageEntity, ReplyParameters)
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from sqlalchemy import JSON, BigInteger, Float, Integer, String, Text, UniqueConstraint, delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.engine import make_url
from telethon import TelegramClient, errors, events, functions, types, utils
from telethon.sessions import StringSession
import qrcode

# ---------------------------------------------------------------------------
# CONFIG (optional). Prefer Render environment variables. Values typed here are
# only used when the matching environment variable is empty. NEVER put real
# secrets here if this file is in a public GitHub repository.
# ---------------------------------------------------------------------------
CONFIG: dict[str, str] = {
    'BOT_TOKEN': '',        # e.g. 123456789:AAH...   (from @BotFather)
    'OWNER_ID': '',         # e.g. 123456789          (your numeric Telegram id; alias ADMIN_ID)
    'ENCRYPTION_KEY': '',   # run: python bot.py --gen-key   (keep it forever)
    'DATABASE_URL': '',     # e.g. postgresql://user:pass@host/db  (empty = temporary SQLite)
    'PUBLIC_URL': '',       # normally empty on Render (RENDER_EXTERNAL_URL is automatic)
}

LOG = logging.getLogger('mirror')
DEFAULTS: dict[str, Any] = {
    'username': '', 'link': '', 'username_mode': 'all', 'listed': [], 'whitelist': [],
    'button_mode': 'replace', 'post_using': 'bot', 'sync_edits': True,
    'sync_deletes': False, 'silent': False, 'link_filter': 'allow',
    'blacklist': [], 'keywords': [], 'skip_media': [], 'header': '', 'footer': '',
    'replacements': [], 'post_link_fallback': 'channel', 'orphan_quote': False,
    'custom_emoji': False, 'max_media_mb': 45,
}
HELP = {
    'username': '@mychannel; replaces source usernames.',
    'link': 'https://t.me/mychannel; replacement target.',
    'username_mode': 'all, listed, or whitelist.',
    'listed': 'JSON list, e.g. ["oldchannel", "someone"].',
    'whitelist': 'JSON list of names NEVER replaced, without @.',
    'button_mode': 'replace, remove, or keep (URL buttons only).',
    'post_using': 'bot or userbot. Posting account needs destination rights.',
    'sync_edits': 'true or false.', 'sync_deletes': 'true or false.',
    'silent': 'true, false, or null (inherit source silent flag).',
    'link_filter': 'allow, skip, or remove. Includes mentions and hidden links.',
    'blacklist': 'JSON list of forbidden substrings, case-insensitive.',
    'keywords': 'JSON list; require at least one match if nonempty.',
    'skip_media': 'JSON list: text, photo, video, document, audio, voice, video_note, animation, sticker, poll.',
    'header': 'Text prepended to each text/caption. Send - to clear.',
    'footer': 'Text appended to each text/caption. Send - to clear.',
    'replacements': 'JSON list of [find, replace] pairs; literal, case-sensitive.',
    'post_link_fallback': 'keep, remove, or channel for unmapped source-post links.',
    'orphan_quote': 'true to prepend up to 120 characters from an uncopied parent.',
    'custom_emoji': 'true to attempt custom emoji; false keeps base emoji only.',
    'max_media_mb': '1..1900 MB. Cloud Bot API has smaller limits (use <=45).',
}

class Base(DeclarativeBase):
    pass

class KV(Base):
    __tablename__ = 'settings'
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[Any] = mapped_column(JSON)

class Account(Base):
    __tablename__ = 'account'
    id: Mapped[int] = mapped_column(primary_key=True)
    secret: Mapped[str] = mapped_column(Text)

class Pair(Base):
    __tablename__ = 'pairs'
    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    destination: Mapped[int] = mapped_column(BigInteger)
    source_ref: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(String(100), default='')
    destination_name: Mapped[str] = mapped_column(String(100), default='')
    state: Mapped[str] = mapped_column(String(30), default='off')
    cursor: Mapped[int] = mapped_column(Integer, default=0)
    baseline: Mapped[int] = mapped_column(Integer, default=0)
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    error: Mapped[str] = mapped_column(Text, default='')
    copied: Mapped[int] = mapped_column(Integer, default=0)
    failures: Mapped[int] = mapped_column(Integer, default=0)
    last_post: Mapped[float] = mapped_column(Float, default=0)

class Mapping(Base):
    __tablename__ = 'mapping'
    __table_args__ = (UniqueConstraint('pair', 'source_id'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[int] = mapped_column(Integer, index=True)
    source_id: Mapped[int] = mapped_column(Integer)
    destination_id: Mapped[int] = mapped_column(Integer)

class Job(Base):
    __tablename__ = 'jobs'
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(220), unique=True)
    pair: Mapped[int] = mapped_column(Integer, index=True)
    destination: Mapped[int] = mapped_column(BigInteger, index=True)
    kind: Mapped[str] = mapped_column(String(12))
    ids: Mapped[list] = mapped_column(JSON)
    source_order: Mapped[int] = mapped_column(Integer, default=0)
    state: Mapped[str] = mapped_column(String(20), default='pending', index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    due: Mapped[float] = mapped_column(Float, default=0)
    error: Mapped[str] = mapped_column(Text, default='')

class MirrorError(Exception):
    """Expected user-actionable error."""

class Skip(MirrorError):
    pass

class Uncertain(MirrorError):
    pass

def u16(text: str) -> int:
    return len(text.encode('utf-16-le')) // 2

def py_index(text: str, offset: int) -> int:
    """Reject an offset splitting a surrogate pair rather than corrupting text."""
    return len(text.encode('utf-16-le')[:offset * 2].decode('utf-16-le'))

@dataclass(frozen=True)
class Edit:
    start: int  # Python codepoint positions, not Telegram units
    end: int
    value: str

def splice(text: str, entities: list, edits: list[Edit]) -> tuple[str, list]:
    """Apply nonoverlapping replacements and remap every entity in UTF-16 units.

    An entity boundary inside replaced text is expanded to that replacement's
    boundary. This deterministic policy preserves enclosing styles, but cannot
    preserve an intra-username style boundary when the username changes length.
    """
    edits = sorted(edits, key=lambda x: (x.start, x.end))
    last = 0
    chunks = []
    spans = []
    for e in edits:
        if e.start < last or not 0 <= e.start <= e.end <= len(text):
            raise MirrorError('Overlapping or invalid replacement.')
        chunks.extend((text[last:e.start], e.value))
        spans.append((u16(text[:e.start]), u16(text[:e.end]), u16(e.value)))
        last = e.end
    chunks.append(text[last:])
    def boundary(pos: int, right: bool) -> int:
        delta = 0
        for a, b, length in spans:
            if pos < a:
                break
            if pos == a:
                return a + delta
            if pos < b:
                return a + delta + (length if right else 0)
            delta += length - (b - a)
        return pos + delta
    result = []
    for entity in entities or []:
        e = copy.copy(entity)
        a = boundary(e.offset, False)
        b = boundary(e.offset + e.length, True)
        if b > a:
            e.offset, e.length = a, b - a
            result.append(e)
    return ''.join(chunks), result

# Avoid matching emails; reserve '+' invite hashes rather than treating them as usernames.
MENTION = re.compile(r'(?<![\w@])@([A-Za-z][A-Za-z0-9_]{3,31})(?!\w)')
TG = re.compile(r'(?<![\w@])(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,31})(?:/(\d+))?(?:\?[^\s<>]*)?', re.I)
PRIVATE_POST = re.compile(r'(?<![\w@])(?:https?://)?t\.me/c/\d+/\d+', re.I)
URL = re.compile(r'(?<![\w@])(?:https?://|www\.)[^\s<>]+|(?<![\w@])(?:t\.me|telegram\.me)/(?:\+)?[^\s<>]+', re.I)
LINK_TYPES = (types.MessageEntityUrl, types.MessageEntityTextUrl,
              types.MessageEntityMention, types.MessageEntityMentionName,
              types.InputMessageEntityMentionName, types.MessageEntityEmail)

def wants(name: str, cfg: dict) -> bool:
    name = name.lower().lstrip('@')
    white = {x.lower().lstrip('@') for x in cfg['whitelist']}
    listed = {x.lower().lstrip('@') for x in cfg['listed']}
    return name not in white and (cfg['username_mode'] != 'listed' or name in listed)

def media_kind(m: Any) -> str:
    if m.photo:
        return 'photo'
    if m.poll:
        return 'poll'
    for key in ('sticker', 'video_note', 'voice', 'audio', 'gif', 'video'):
        if getattr(m, key, None):
            return 'animation' if key == 'gif' else key
    return 'document' if m.document else 'text'

def bot_entities(entities: list, custom: bool) -> list[MessageEntity]:
    names = {
        'MessageEntityBold': 'bold', 'MessageEntityItalic': 'italic',
        'MessageEntityUnderline': 'underline', 'MessageEntityStrike': 'strikethrough',
        'MessageEntitySpoiler': 'spoiler', 'MessageEntityCode': 'code',
        'MessageEntityPre': 'pre', 'MessageEntityUrl': 'url',
        'MessageEntityTextUrl': 'text_link', 'MessageEntityMention': 'mention',
        'MessageEntityHashtag': 'hashtag', 'MessageEntityCashtag': 'cashtag',
        'MessageEntityEmail': 'email', 'MessageEntityPhone': 'phone_number',
        'MessageEntityBotCommand': 'bot_command',
        'MessageEntityCustomEmoji': 'custom_emoji',
    }
    out = []
    for e in entities:
        n = type(e).__name__
        if n == 'MessageEntityCustomEmoji' and not custom:
            continue
        if n == 'MessageEntityBlockquote':
            kind = 'expandable_blockquote' if getattr(e, 'collapsed', False) else 'blockquote'
        else:
            kind = names.get(n)
        if n in {'MessageEntityMentionName', 'InputMessageEntityMentionName'}:
            uid = getattr(e.user_id, 'user_id', e.user_id)
            out.append(MessageEntity(type='text_link', offset=e.offset, length=e.length, url=f'tg://user?id={uid}'))
            continue
        if kind is None:
            raise MirrorError(f'Unsupported formatting entity {n}; use a supported source format.')
        kw = {'type': kind, 'offset': e.offset, 'length': e.length}
        if kind == 'pre':
            kw['language'] = e.language or ''
        elif kind == 'text_link':
            kw['url'] = e.url
        elif kind == 'custom_emoji':
            kw['custom_emoji_id'] = str(e.document_id)
        out.append(MessageEntity(**kw))
    return out

@dataclass
class Draft:
    step: str
    values: dict = field(default_factory=dict)
    client: TelegramClient | None = None
    touched: float = field(default_factory=time.monotonic)
    task: asyncio.Task | None = None

class App:
    def __init__(self, token: str, owner: int, database: str, encryption: str, public_url: str):
        self.owner = owner
        self.bot = Bot(token)
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)
        if database.startswith('postgres://'):
            database = database.replace('postgres://', 'postgresql+asyncpg://', 1)
        elif database.startswith('postgresql://'):
            database = database.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif database.startswith('sqlite://'):
            database = database.replace('sqlite://', 'sqlite+aiosqlite://', 1)
        database_url = make_url(database)
        connect_args = {}
        if database_url.drivername == 'postgresql+asyncpg':
            if 'sslmode' in database_url.query:
                connect_args['ssl'] = database_url.query['sslmode']
            # libpq-only URL options that asyncpg rejects (e.g. Neon adds channel_binding).
            database_url = database_url.difference_update_query(['sslmode', 'channel_binding'])
        self.engine = create_async_engine(database_url, pool_pre_ping=True, connect_args=connect_args)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False)
        self.fernet = Fernet(encryption.encode())
        self.public_url = public_url.rstrip('/')
        self.webhook_secret = hmac.new(token.encode(), b'mirror-webhook-secret', hashlib.sha256).hexdigest()
        self.webhook_path = '/telegram/' + hashlib.sha256(token.encode()).hexdigest()[:32]
        self.client: TelegramClient | None = None
        self.draft: Draft | None = None
        self.tasks: list[asyncio.Task] = []
        self.started = time.time()
        self.wake = asyncio.Event()
        self.guard = asyncio.Lock()  # serialize panel state transitions and account setup
        self.ingest = asyncio.Lock()  # activation boundary vs update ingestion
        self.history = deque()
        self.pause = False
        self.alive = True
        self.ready = False
        self.keypad_message_id: int | None = None
        self.webhook_lock = asyncio.Lock()
        self.router.message.register(self.message)
        self.router.callback_query.register(self.callback)

    async def get(self, cls, ident):
        async with self.Session() as s:
            return await s.get(cls, ident)

    async def rows(self, cls, *where, order=None, limit=None):
        async with self.Session() as s:
            stmt = select(cls).where(*where)
            if order is not None:
                stmt = stmt.order_by(order)
            if limit is not None:
                stmt = stmt.limit(limit)
            return list((await s.scalars(stmt)).all())

    async def patch(self, cls, ident, **values):
        async with self.Session.begin() as s:
            await s.execute(update(cls).where(cls.id == ident).values(**values))

    async def setting(self, key: str, default=None):
        row = await self.get(KV, key)
        return row.value if row else default

    async def save_setting(self, key: str, value):
        async with self.Session.begin() as s:
            await s.merge(KV(key=key, value=value))

    async def alert(self, text: str):
        try:
            await self.bot.send_message(self.owner, text[:3900])
        except Exception:
            LOG.warning('Owner notification failed')

    def cfg(self, pair: Pair) -> dict:
        return DEFAULTS | pair.config

    async def mapped(self, pid: int, mid: int | None) -> int | None:
        if mid is None:
            return None
        rows = await self.rows(Mapping, Mapping.pair == pid, Mapping.source_id == mid)
        return rows[0].destination_id if rows else None

    def keyboard(self, rows):
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=label, callback_data=data) for label, data in row
        ] for row in rows])

    async def say(self, text: str, rows=None):
        return await self.bot.send_message(self.owner, text[:4000],
                                           reply_markup=self.keyboard(rows) if rows else None)

    async def menu(self):
        await self.say('Mirror admin\nConfigure an account, add a pair, preview, then enable.', [
            [('Accounts', 'accounts'), ('Pairs', 'pairs:0')],
            [('Username Rules / Filters', 'pairs:0'), ('Stats', 'stats')],
            [('Resume all' if self.pause else 'Pause all', 'pause'), ('Jobs', 'jobs')],
            [('Settings / Help', 'help')],
        ])

    async def clear_draft(self):
        draft, self.draft = self.draft, None
        if draft:
            if draft.task and draft.task is not asyncio.current_task():
                draft.task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await draft.task
            if draft.client and draft.client is not self.client:
                await draft.client.disconnect()
            draft.values.clear()

    def rate_ok(self) -> bool:
        now = time.monotonic()
        while self.history and now - self.history[0] > 10:
            self.history.popleft()
        if len(self.history) >= 25:
            return False
        self.history.append(now)
        return True

    async def private_owner(self, m: Message) -> bool:
        return bool(m.from_user and m.from_user.id == self.owner and m.chat.type == 'private')

    async def message(self, m: Message):
        if not await self.private_owner(m):
            return
        # Delete credential messages BEFORE rate limiting or any echo/error response.
        sensitive = self.draft and self.draft.step in {'api_hash', 'phone', 'code', 'password', 'api_id'}
        if sensitive:
            try:
                await m.delete()
            except Exception:
                await self.alert('Could not delete a credential message. Delete it manually in this chat.')
        if not self.rate_ok():
            return
        async with self.guard:
            if m.text and m.text.split('@')[0] in {'/start', '/cancel'}:
                await self.clear_draft()
                return await self.menu()
            if self.draft:
                self.draft.touched = time.monotonic()
                try:
                    await self.handle_input(m)
                except errors.FloodWaitError as exc:
                    await self.say(f'Telegram requires waiting {exc.seconds} seconds. Use /cancel, then retry later.')
                except errors.SessionPasswordNeededError:
                    self.draft.step = 'password'
                    await self.say('2FA enabled. Send the password, or /cancel. It is deleted after receipt. QR login avoids sharing it with the bot.')
                except (errors.PhoneCodeInvalidError, errors.PhoneCodeExpiredError):
                    await self.say('Code is wrong or expired. For an expired code use /cancel and start login again.')
                except errors.PasswordHashInvalidError:
                    await self.say('Incorrect 2FA password. Retry or /cancel.')
                except errors.PhoneNumberBannedError:
                    await self.clear_draft()
                    await self.say('Telegram banned this phone. Use an eligible account or contact Telegram support.')
                except errors.ApiIdInvalidError:
                    await self.clear_draft()
                    await self.say('Invalid API credentials. Get API ID/hash at https://my.telegram.org/apps and restart.')
                except MirrorError as exc:
                    await self.say(str(exc))
                except Exception as exc:
                    # Deliberately do NOT include Telegram request/string repr with login secrets.
                    await self.say(f'Action failed ({type(exc).__name__}). Correct the input or /cancel and retry. No credentials were logged.')
                return
            await self.menu()

    async def callback(self, q: CallbackQuery):
        if not q.from_user or q.from_user.id != self.owner or not q.message or q.message.chat.id != self.owner:
            await q.answer('Not authorized.', show_alert=True)
            return
        if not self.rate_ok():
            await q.answer('Slow down a moment.')
            return
        await q.answer()
        async with self.guard:
            try:
                await self.action(q.data or '')
            except errors.SessionPasswordNeededError:
                if self.draft:
                    self.draft.step = 'password'
                await self.say('2FA enabled. Send the password or /cancel. It will be deleted after receipt.')
            except (errors.PhoneCodeInvalidError, errors.PhoneCodeExpiredError):
                if self.draft:
                    self.draft.values['code'] = ''
                await self.say('Wrong or expired code. For a wrong code enter it again; for an expired code use /cancel and restart login.', self.keypad())
            except errors.FloodWaitError as exc:
                await self.say(f'Telegram requires waiting {exc.seconds} seconds. Retry later.')
            except MirrorError as exc:
                await self.say(str(exc))
            except Exception as exc:
                await self.say(f'Action failed ({type(exc).__name__}). Check account and channel permissions, then retry.')

    async def action(self, data: str):
        parts = data.split(':')
        cmd = parts[0]
        if cmd == 'home':
            await self.clear_draft()
            return await self.menu()
        if cmd == 'help':
            return await self.say('Setup: Accounts -> login -> Pairs -> Add -> select pair -> settings -> preview -> ON.\n'
                'All settings are in this panel. List-valued settings use JSON in chat. /cancel clears a prompt.\n'
                'The bot and, for userbot mode, the logged-in account need destination posting rights.\n'
                'Pause freezes ingestion; resume copies only newer posts, not posts during the pause.\n'
                'Existing queued posts remain queued while paused. OFF cancels unstarted jobs.\n'
                'Free Render cannot guarantee uptime. Use external PostgreSQL.\n'
                'Uncertain jobs require checking your destination before retrying.\n'
                'Only copy content you are authorized to republish.', [[('Home', 'home')]])
        if cmd == 'accounts':
            authorized = self.client is not None and await self.client.is_user_authorized()
            return await self.say('Account: ' + ('connected' if authorized else 'not connected'), [
                [('Add / replace account', 'login')], [('Logout', 'confirm:logout:0')], [('Home', 'home')]])
        if cmd == 'login':
            if self.client:
                return await self.say('Replacing an account requires logout first. This turns all pairs OFF.', [[('Logout', 'confirm:logout:0')]])
            await self.clear_draft()
            self.draft = Draft('api_id')
            return await self.say('Send API_ID (integer). Obtain API ID and API hash at https://my.telegram.org/apps. Never use credentials from strangers.')
        if cmd == 'mode':
            d = self.draft
            if not d or d.step != 'login_mode':
                raise MirrorError('Login expired. Open Accounts -> Add account.')
            if parts[1] == 'phone':
                d.step = 'phone'
                return await self.say('Send phone in international format, e.g. +15551234567. It will be deleted.')
            d.step = 'qr'
            d.task = asyncio.create_task(self.qr_login(d))
            return
        if cmd == 'key':
            d = self.draft
            if not d or d.step != 'code':
                raise MirrorError('No code request is active. Start login again.')
            key = parts[1]
            if key == 'submit':
                await self.complete_code(d.values.get('code', ''))
            else:
                code = d.values.get('code', '')
                d.values['code'] = code[:-1] if key == 'del' else (code + key)[:8]
                d.touched = time.monotonic()
                if self.keypad_message_id:
                    with contextlib.suppress(TelegramBadRequest):
                        await self.bot.edit_message_text(chat_id=self.owner, message_id=self.keypad_message_id,
                            text=f'Code digits entered: {len(d.values["code"])}', reply_markup=self.keyboard(self.keypad()))
            return
        if cmd == 'pairs':
            page = max(0, int(parts[1]))
            pairs = await self.rows(Pair, order=Pair.id)
            rows = [[(f'#{p.id} {p.state} {p.source_name or "private"} -> {p.destination_name or p.destination}', f'pair:{p.id}')] for p in pairs[page*6:page*6+6]]
            rows += [[('Previous', f'pairs:{max(0,page-1)}'), ('Next', f'pairs:{page+1}')], [('Add pair', 'addpair'), ('Home', 'home')]]
            return await self.say('Channel pairs. Enabling sets a NEW-post boundary; no historical posts are copied.', rows)
        if cmd == 'addpair':
            if not self.client:
                raise MirrorError('Add an account first.')
            await self.clear_draft()
            self.draft = Draft('source')
            return await self.say('Send source @username, public t.me link, or private invite link.')
        if cmd in {'pair', 'config', 'edit', 'toggle', 'preview', 'check', 'pending', 'delpair'}:
            p = await self.get(Pair, int(parts[1]))
            if not p:
                raise MirrorError('Pair no longer exists.')
            if cmd == 'pair':
                return await self.say(f'Pair #{p.id}\nState: {p.state}\nSource: {p.source_name or "private"}\nDestination: {p.destination_name or p.destination}\n{p.error}', [
                    [('OFF' if p.state == 'active' else 'ON', f'toggle:{p.id}'), ('Test preview', f'preview:{p.id}')],
                    [('Username Rules / Filters / Settings', f'config:{p.id}:0')],
                    [('Check permissions', f'check:{p.id}'), ('Check join', f'pending:{p.id}')],
                    [('Delete pair', f'confirm:delpair:{p.id}'), ('Back', 'pairs:0')]])
            if cmd == 'config':
                page = max(0, int(parts[2]))
                cfg = self.cfg(p)
                keys = list(DEFAULTS)[page*6:page*6+6]
                rows = [[(f'{key}: {str(cfg[key])[:35]}', f'edit:{p.id}:{key}')] for key in keys]
                rows += [[('Previous', f'config:{p.id}:{max(0,page-1)}'), ('Next', f'config:{p.id}:{min(3,page+1)}')], [('Pair', f'pair:{p.id}')]]
                return await self.say('Edit pair settings. Changes apply to unprocessed posts and future edits.', rows)
            if cmd == 'edit':
                key = parts[2]
                if key not in DEFAULTS:
                    raise MirrorError('Unknown setting.')
                await self.clear_draft()
                self.draft = Draft('setting', {'pair': p.id, 'key': key})
                return await self.say(f'{key}\n{HELP[key]}\nCurrent: {json.dumps(self.cfg(p)[key], ensure_ascii=False)}')
            if cmd == 'toggle':
                if p.state == 'pending':
                    raise MirrorError('Still waiting for channel approval. Use Check join.')
                if p.state == 'active':
                    async with self.ingest:
                        async with self.Session.begin() as s:
                            await s.execute(update(Pair).where(Pair.id == p.id).values(state='off'))
                            await s.execute(update(Job).where(Job.pair == p.id, Job.state == 'pending').values(state='cancelled'))
                    return await self.say('OFF. Unstarted jobs cancelled. An in-flight send might still finish.')
                await self.activate(p)
                return await self.say('ON. Only posts created after this activation boundary will be copied.')
            if cmd == 'check':
                return await self.say(await self.permissions(p.destination, self.cfg(p)['post_using']))
            if cmd == 'pending':
                await self.check_pending(p, manual=True)
                return
            if cmd == 'preview':
                return await self.preview(p)
            if cmd == 'delpair':
                if await self.rows(Job, Job.pair == p.id, Job.state == 'sending'):
                    raise MirrorError('A send is in flight. Turn OFF and retry deletion after it finishes.')
                async with self.Session.begin() as s:
                    for cls in (Mapping, Job):
                        await s.execute(delete(cls).where(cls.pair == p.id))
                    await s.execute(delete(Pair).where(Pair.id == p.id))
                return await self.say('Pair and its mapping deleted.')
        if cmd == 'pause':
            async with self.ingest:
                if self.pause:
                    if not self.client:
                        raise MirrorError('Reconnect account before resuming.')
                    for p in await self.rows(Pair, Pair.state == 'active'):
                        latest = await self.client.get_messages(p.source, limit=1)
                        boundary = latest[0].id if latest else p.cursor
                        await self.patch(Pair, p.id, cursor=boundary, baseline=boundary)
                self.pause = not self.pause
                await self.save_setting('pause', self.pause)
            return await self.say('Paused.' if self.pause else 'Resumed. Posts created during the pause are excluded.')
        if cmd == 'stats':
            pairs = await self.rows(Pair, order=Pair.id)
            lines = [f'Uptime: {int(time.time()-self.started)} seconds', f'Global paused: {self.pause}']
            lines += [f'#{p.id}: {p.copied} copied, {p.failures} failures, last {time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(p.last_post)) if p.last_post else "never"}' for p in pairs]
            return await self.say('\n'.join(lines), [[('Home', 'home')]])
        if cmd == 'jobs':
            jobs = await self.rows(Job, Job.state.in_(['failed', 'uncertain']), order=Job.id)
            rows = [[(f'#{j.id} {j.state} pair {j.pair}', f'job:{j.id}')] for j in jobs[:15]]
            return await self.say('First 15 failed/uncertain jobs. Uncertain sends block later jobs to that destination. Resolve them after inspecting the channel.', rows + [[('Home', 'home')]])
        if cmd == 'job':
            j = await self.get(Job, int(parts[1]))
            if j:
                return await self.say(f'Job #{j.id}: {j.state}\n{j.error}\nIf already delivered, choose Reconcile and enter destination message IDs in source order. Discard means deliberately not tracking this delivery.', [
                    [('Reconcile delivered IDs', f'reconcile:{j.id}')],
                    [('Retry (duplicate risk)', f'confirm:retry:{j.id}'), ('Discard', f'confirm:discard:{j.id}')]])
        if cmd == 'reconcile':
            await self.clear_draft()
            self.draft = Draft('reconcile', {'job': int(parts[1])})
            return await self.say('Send a JSON list of destination message IDs, in the same order as source IDs. Example: [456,457]. Verify the channel first.')
        if cmd == 'confirm':
            return await self.say('Confirm destructive action? Retry can duplicate a post if Telegram already accepted it.', [[('Confirm', f'yes:{parts[1]}:{parts[2]}'), ('Cancel', 'home')]])
        if cmd == 'yes':
            if parts[1] == 'logout':
                await self.clear_draft()
                self.pause = True
                await self.save_setting('pause', True)
                if await self.rows(Job, Job.state == 'sending'):
                    raise MirrorError('Paused. Wait for in-flight send completion, then logout again.')
                if self.client:
                    await self.client.log_out()
                    await self.client.disconnect()
                    self.client = None
                async with self.Session.begin() as s:
                    await s.execute(delete(Account))
                    await s.execute(update(Pair).values(state='off'))
                    await s.execute(update(Job).where(Job.state == 'pending').values(state='cancelled'))
                return await self.say('Logged out. Pairs OFF, queue cancelled, global pause ON. Login, enable chosen pairs and Resume all.')
            if parts[1] == 'delpair':
                return await self.action('delpair:' + parts[2])
            if parts[1] in {'retry', 'discard'}:
                j = await self.get(Job, int(parts[2]))
                if not j or j.state not in {'failed', 'uncertain'}:
                    raise MirrorError('Job is not awaiting operator action.')
                await self.patch(Job, j.id, state='pending' if parts[1] == 'retry' else 'cancelled', due=0, error='')
                self.wake.set()
                return await self.say('Job updated. Fix permissions first. Active pairs process retried jobs automatically; OFF pairs must be enabled separately.')

    def keypad(self):
        rows = [[(str(n), f'key:{n}') for n in range(start, start+3)] for start in (1, 4, 7)]
        return rows + [[('Delete', 'key:del'), ('0', 'key:0'), ('Submit', 'key:submit')]]

    async def handle_input(self, m: Message):
        d = self.draft
        text = m.text or ''
        if not d:
            return
        if d.step == 'api_id':
            try:
                value = int(text)
            except ValueError:
                raise MirrorError('API_ID must be a positive integer.')
            if value <= 0:
                raise MirrorError('API_ID must be positive.')
            d.values['api_id'] = value
            d.step = 'api_hash'
            return await self.say('Send API_HASH. It will be deleted after receipt.')
        if d.step == 'api_hash':
            if not re.fullmatch(r'[a-fA-F0-9]{32}', text.strip()):
                raise MirrorError('API_HASH must contain 32 hexadecimal characters.')
            d.values['api_hash'] = text.strip()
            d.client = TelegramClient(StringSession(), d.values['api_id'], d.values['api_hash'],
                                      auto_reconnect=True, connection_retries=10, flood_sleep_threshold=0)
            await d.client.connect()
            d.step = 'login_mode'
            return await self.say('Choose login method.', [[('Phone / code', 'mode:phone'), ('QR code', 'mode:qr')]])
        if d.step == 'phone':
            phone = re.sub(r'[ ()-]', '', text)
            if not re.fullmatch(r'\+\d{7,15}', phone):
                raise MirrorError('Use international format, e.g. +15551234567.')
            result = await d.client.send_code_request(phone)
            d.values.update(phone=phone, phone_code_hash=result.phone_code_hash, code='')
            d.step = 'code'
            sent = await self.say('Enter code using keypad or separated digits, e.g. 1 2 3 4 5. Do not send a contiguous login code.', self.keypad())
            self.keypad_message_id = sent.message_id
            return
        if d.step == 'code':
            if not re.fullmatch(r'\d(?:[ -]+\d){3,7}', text.strip()):
                raise MirrorError('Use keypad, or digits separated by spaces. /cancel restarts login.')
            return await self.complete_code(re.sub(r'\D', '', text))
        if d.step == 'password':
            if not text:
                raise MirrorError('Send your 2FA password as text or /cancel.')
            await d.client.sign_in(password=text)
            return await self.finish_login(d)
        if d.step == 'source':
            entity, pending, ref = await self.resolve_source(text.strip())
            if entity and (not isinstance(entity, types.Channel) or not entity.broadcast):
                raise MirrorError('Source must be a broadcast channel, not a group.')
            d.values.update(source=utils.get_peer_id(entity) if entity else None,
                            source_name=getattr(entity, 'username', None) or '', source_ref=ref, pending=pending)
            d.step = 'destination'
            return await self.say('Send destination @username / numeric channel ID, or forward an actual channel post here. Bot must be admin with post permission.')
        if d.step == 'destination':
            origin = m.forward_origin
            destination = origin.chat.id if origin and origin.type == 'channel' else text.strip()
            if isinstance(destination, str) and re.fullmatch(r'-?\d+', destination):
                destination = int(destination)
            chat = await self.bot.get_chat(destination)
            await self.require_permissions(chat.id, 'bot')
            if d.values['source'] == chat.id:
                raise MirrorError('Source and destination cannot be the same channel.')
            async with self.Session.begin() as s:
                # Directed cycles cause infinite reposting, even when pairs use different accounts.
                edges = [(p.source, p.destination) for p in (await s.scalars(select(Pair))).all()]
                reach = {chat.id}
                for _ in range(len(edges) + 1):
                    reach |= {b for a, b in edges if a in reach}
                if d.values['source'] in reach:
                    raise MirrorError('This pair creates a mirroring cycle. Choose another destination.')
                p = Pair(source=d.values['source'], destination=chat.id, source_ref=self.fernet.encrypt(d.values['source_ref'].encode()).decode(),
                         source_name=d.values['source_name'], destination_name=chat.username or '',
                         state='pending' if d.values['pending'] else 'off', config=dict(DEFAULTS))
                s.add(p)
                await s.flush()
                pid = p.id
            await self.clear_draft()
            return await self.say(f'Pair #{pid} saved. Configure username/link before enabling. Pending private channels auto-activate only after approval.', [[('Open pair', f'pair:{pid}')]])
        if d.step == 'setting':
            p = await self.get(Pair, d.values['pair'])
            if not p:
                raise MirrorError('Pair deleted. /cancel.')
            key = d.values['key']
            value = self.validate_setting(key, text)
            cfg = self.cfg(p) | {key: value}
            if key == 'post_using':
                await self.require_permissions(p.destination, value)
            await self.patch(Pair, p.id, config=cfg)
            await self.clear_draft()
            return await self.say('Setting saved.', [[('Pair settings', f'config:{p.id}:0')]])
        if d.step == 'reconcile':
            j = await self.get(Job, d.values['job'])
            if not j or j.state not in {'uncertain', 'failed'} or j.kind != 'new':
                raise MirrorError('Only uncertain/failed NEW-message jobs can be reconciled.')
            try:
                ids = json.loads(text)
            except ValueError:
                raise MirrorError('Enter a JSON array of positive destination IDs.')
            if not isinstance(ids, list) or len(ids) != len(j.ids) or any(type(x) is not int or x <= 0 for x in ids):
                raise MirrorError(f'Expected {len(j.ids)} positive destination IDs.')
            await self.commit_delivery(j, ids)
            await self.clear_draft()
            self.wake.set()
            return await self.say('Delivery mapping reconciled.')
        raise MirrorError('Use the current inline buttons or /cancel.')

    def validate_setting(self, key: str, text: str):
        default = DEFAULTS[key]
        if isinstance(default, bool):
            if key == 'silent' and text.lower() == 'null':
                return None
            if text.lower() not in {'true', 'false'}:
                raise MirrorError('Enter true or false.')
            return text.lower() == 'true'
        if isinstance(default, list):
            try:
                value = json.loads(text)
            except ValueError:
                raise MirrorError('Use a valid JSON list, e.g. ["one", "two"].')
            if not isinstance(value, list) or len(value) > 100:
                raise MirrorError('Use a JSON list of at most 100 entries.')
            if key == 'replacements':
                if any(not isinstance(x, list) or len(x) != 2 or not all(isinstance(v, str) for v in x) or not x[0] for x in value):
                    raise MirrorError('Use [["find", "replace"]], with nonempty find text.')
            elif any(not isinstance(x, str) or not x for x in value):
                raise MirrorError('Every entry must be a nonempty string.')
            return value
        if key == 'max_media_mb':
            try:
                value = int(text)
            except ValueError:
                raise MirrorError('Enter an integer from 1 to 1900.')
            if not 1 <= value <= 1900:
                raise MirrorError('Enter an integer from 1 to 1900.')
            return value
        choices = {'username_mode': {'all', 'listed', 'whitelist'}, 'button_mode': {'replace', 'remove', 'keep'},
                   'post_using': {'bot', 'userbot'}, 'link_filter': {'allow', 'skip', 'remove'},
                   'post_link_fallback': {'keep', 'remove', 'channel'}}
        value = '' if text == '-' else text
        if key in choices and value not in choices[key]:
            raise MirrorError('Allowed: ' + ', '.join(sorted(choices[key])))
        if key == 'username' and value and not re.fullmatch(r'@[A-Za-z][A-Za-z0-9_]{3,31}', value):
            raise MirrorError('Use @channelname.')
        if key == 'link' and value and not re.fullmatch(r'https://t\.me/[A-Za-z][A-Za-z0-9_]{3,31}', value):
            raise MirrorError('Use https://t.me/channelname.')
        if len(value) > 2000:
            raise MirrorError('Setting is too long (maximum 2000 characters).')
        return value

    async def complete_code(self, code: str):
        d = self.draft
        if not d or not d.client or not re.fullmatch(r'\d{4,8}', code):
            raise MirrorError('Enter all code digits first.')
        await d.client.sign_in(phone=d.values['phone'], code=code, phone_code_hash=d.values['phone_code_hash'])
        await self.finish_login(d)

    async def qr_login(self, d: Draft):
        qr_message = None
        try:
            while self.draft is d and d.step == 'qr':
                qr = await d.client.qr_login()
                buffer = io.BytesIO()
                qrcode.make(qr.url).save(buffer, format='PNG')
                if qr_message:
                    with contextlib.suppress(Exception):
                        await qr_message.delete()
                qr_message = await self.bot.send_photo(self.owner, BufferedInputFile(buffer.getvalue(), 'login.png'),
                    caption='Scan in Telegram Settings -> Devices -> Link Desktop Device. QR refreshes on expiry. /cancel stops login.')
                try:
                    await qr.wait(timeout=30)
                except asyncio.TimeoutError:
                    continue
                except errors.SessionPasswordNeededError:
                    d.step = 'password'
                    await self.say('QR accepted. 2FA required: send password or /cancel. It is deleted after reading.')
                    return
                async with self.guard:
                    if self.draft is d:
                        await self.finish_login(d)
                return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.alert(f'QR login failed ({type(exc).__name__}). Use /cancel and restart login.')
        finally:
            if qr_message:
                with contextlib.suppress(Exception):
                    await qr_message.delete()

    async def finish_login(self, d: Draft):
        if not await d.client.is_user_authorized():
            raise MirrorError('Login not completed. Restart Accounts -> Add account.')
        secret = {'api_id': d.values['api_id'], 'api_hash': d.values['api_hash'], 'session': d.client.session.save()}
        async with self.Session.begin() as s:
            await s.merge(Account(id=1, secret=self.fernet.encrypt(json.dumps(secret).encode()).decode()))
        self.client = d.client
        # StringSession stores auth, not the channel access-hash entity cache.
        async for _ in self.client.iter_dialogs():
            pass
        self.attach_handlers(self.client)
        await self.clear_draft()
        await self.say('Account connected. Credentials and session encrypted at rest. Add pairs next.', [[('Pairs', 'pairs:0')]])

    async def resolve_source(self, ref: str):
        if not self.client:
            raise MirrorError('No account connected.')
        invite = re.fullmatch(r'(?:https?://)?t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)', ref)
        try:
            if invite:
                token = invite.group(1)
                check = await self.client(functions.messages.CheckChatInviteRequest(token))
                if isinstance(check, types.ChatInviteAlready):
                    return check.chat, False, ref
                try:
                    result = await self.client(functions.messages.ImportChatInviteRequest(token))
                    return result.chats[0], False, ref
                except errors.InviteRequestSentError:
                    return None, True, ref
            username = ref.removeprefix('https://').removeprefix('http://').removeprefix('t.me/').strip('/').lstrip('@')
            if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{3,31}', username):
                raise MirrorError('Use @username or a supported t.me invite link.')
            entity = await self.client.get_entity(username)
            if not isinstance(entity, types.Channel) or not entity.broadcast:
                raise MirrorError('Source must be a broadcast channel, not a user or discussion group.')
            try:
                await self.client(functions.channels.JoinChannelRequest(entity))
            except errors.UserAlreadyParticipantError:
                pass
            except errors.InviteRequestSentError:
                return entity, True, ref
            return entity, False, ref
        except (errors.InviteHashExpiredError, errors.InviteHashInvalidError):
            raise MirrorError('Invite expired or invalid. Ask the source admin for a fresh invitation.')
        except errors.UserBannedInChannelError:
            raise MirrorError('Account is banned from this channel. Ask its admin to unblock it.')

    async def permissions(self, destination: int, using: str) -> str:
        chat = await self.bot.get_chat(destination)
        me = await self.bot.get_me()
        member = await self.bot.get_chat_member(destination, me.id)
        bot_ok = member.status == 'creator' or (member.status == 'administrator' and bool(member.can_post_messages))
        lines = [f'Destination channel: {"OK" if chat.type == "channel" else "NOT a channel"}',
                 f'Bot admin + post permission: {"OK" if bot_ok else "MISSING: add bot as admin with Post Messages"}']
        if using == 'userbot':
            if not self.client:
                lines.append('User account: NOT logged in')
            else:
                try:
                    rights = await self.client.get_permissions(destination, 'me')
                    good = rights.is_creator or (rights.is_admin and bool(getattr(rights, 'post_messages', False)))
                except Exception:
                    good = False
                lines.append(f'User account posting rights: {"OK" if good else "MISSING: add account as admin with Post Messages"}')
        return '\n'.join(lines)

    async def require_permissions(self, destination: int, using: str):
        report = await self.permissions(destination, using)
        if 'MISSING' in report or 'NOT' in report:
            raise MirrorError(report)

    async def activate(self, p: Pair):
        if not self.client or not p.source:
            raise MirrorError('Account/source unavailable. Check account and join approval.')
        await self.require_permissions(p.destination, self.cfg(p)['post_using'])
        async with self.ingest:
            last = await self.client.get_messages(p.source, limit=1)
            boundary = last[0].id if last else 0
            await self.patch(Pair, p.id, state='active', error='', cursor=boundary, baseline=boundary)
        self.wake.set()

    async def check_pending(self, p: Pair, manual=False):
        if p.state != 'pending' or not self.client:
            if manual:
                await self.say('This pair is not pending, or account is disconnected.')
            return
        try:
            source_ref = self.fernet.decrypt(p.source_ref.encode()).decode()
            invite = re.search(r't\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)', source_ref)
            entity = None
            if invite:
                result = await self.client(functions.messages.CheckChatInviteRequest(invite.group(1)))
                if isinstance(result, types.ChatInviteAlready):
                    entity = result.chat
            elif p.source:
                candidate = await self.client.get_entity(p.source)
                rights = await self.client.get_permissions(candidate, 'me')
                if rights:
                    entity = candidate
            if entity:
                # Prevent an unresolved private source from introducing a cycle later.
                sid = utils.get_peer_id(entity)
                edges = [(x.source, x.destination) for x in await self.rows(Pair) if x.id != p.id]
                reach = {p.destination}
                for _ in range(len(edges)+1):
                    reach |= {b for a, b in edges if a in reach}
                if sid in reach:
                    raise MirrorError('Approval accepted but activation would create a channel cycle. Delete this pair.')
                if not isinstance(entity, types.Channel) or not entity.broadcast:
                    raise MirrorError('Approved source is not a broadcast channel.')
                await self.patch(Pair, p.id, source=sid, source_name=entity.username or '', state='off')
                p = await self.get(Pair, p.id)
                await self.activate(p)
                await self.alert(f'Pair #{p.id}: approval accepted, now ON. New posts only.')
            elif manual:
                await self.say('Approval still pending. Telegram cannot reliably distinguish unanswered and rejected requests. Ask the source admin if necessary.')
        except errors.UserNotParticipantError:
            if manual:
                await self.say('Account is not a member yet. Ask the source admin to approve your request.')
        except errors.FloodWaitError:
            raise
        except (errors.InviteHashInvalidError, errors.InviteHashExpiredError):
            await self.patch(Pair, p.id, state='error', error='Invite invalid/expired. Recreate pair with a fresh invite.')
            await self.alert(f'Pair #{p.id}: invite invalid/expired. Recreate pair with a fresh invite.')
        except MirrorError as exc:
            await self.patch(Pair, p.id, state='error', error=str(exc))
            await self.alert(f'Pair #{p.id}: {exc}')

    async def rewrite_url(self, url: str, p: Pair, cfg: dict) -> str | None:
        match = TG.fullmatch(url)
        if not match:
            # Private t.me/c/<channel>/<post> links are recognized only for this source.
            private = re.fullmatch(r'(?:https?://)?t\.me/c/(\d+)/(\d+)', url, re.I)
            if private and p.source and int(private.group(1)) == int(str(p.source).removeprefix('-100')):
                mid = int(private.group(2))
                mapped = await self.mapped(p.id, mid)
                if mapped:
                    return self.destination_post_link(p, mapped)
                return self.link_fallback(url, cfg)
            return url
        name, post_id = match.groups()
        if name.lower() in {'joinchat', 'addstickers', 'addemoji', 'proxy', 'socks', 'share', 'login', 'confirmphone', 'iv', 'boost', 'invoice', 'contact', 'giftcode'}:
            return url
        if post_id and name.lower() == p.source_name.lower():
            mapped = await self.mapped(p.id, int(post_id))
            if mapped:
                return self.destination_post_link(p, mapped)
            return self.link_fallback(url, cfg)
        if wants(name, cfg) and cfg['link']:
            return cfg['link']
        return url

    def destination_post_link(self, p: Pair, mid: int) -> str:
        if p.destination_name:
            return f'https://t.me/{p.destination_name}/{mid}'
        return f'https://t.me/c/{str(p.destination).removeprefix("-100")}/{mid}'

    def link_fallback(self, original: str, cfg: dict):
        mode = cfg['post_link_fallback']
        return original if mode == 'keep' else (None if mode == 'remove' else cfg['link'] or original)

    async def transform(self, p: Pair, m) -> tuple[str, list]:
        cfg = self.cfg(p)
        text, entities = m.raw_text or '', copy.deepcopy(m.entities or [])
        if any(x.casefold() in text.casefold() for x in cfg['blacklist']):
            raise Skip('Keyword blacklist')
        if cfg['keywords'] and not any(x.casefold() in text.casefold() for x in cfg['keywords']):
            raise Skip('Keyword whitelist')
        if media_kind(m) in cfg['skip_media']:
            raise Skip('Media filter')
        link_entities = [e for e in entities if isinstance(e, LINK_TYPES)]
        has_url_button = any(isinstance(b, types.KeyboardButtonUrl) for row in getattr(getattr(m, 'reply_markup', None), 'rows', []) or [] for b in row.buttons)
        if cfg['link_filter'] == 'skip' and (URL.search(text) or MENTION.search(text) or link_entities or has_url_button):
            raise Skip('Link filter')
        changes: list[Edit] = []
        targets: dict[int, str | None] = {}
        def insert(a: int, b: int, value: str):
            if any(a < old.end and b > old.start for old in changes):
                return False
            changes.append(Edit(a, b, value))
            return True
        if cfg['link_filter'] == 'remove':
            ranges = [(py_index(text, e.offset), py_index(text, e.offset+e.length)) for e in link_entities]
            ranges += [x.span() for pattern in (URL, MENTION) for x in pattern.finditer(text)]
            # Merge overlapping link ranges so a shorter mention never truncates a URL.
            merged = []
            for a, b in sorted(ranges):
                if merged and a <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
                else:
                    merged.append((a, b))
            changes = [Edit(a, b, '') for a, b in merged]
            entities = [e for e in entities if not isinstance(e, LINK_TYPES)]
        else:
            for e in entities:
                if isinstance(e, types.MessageEntityTextUrl):
                    new_url = await self.rewrite_url(e.url, p, cfg)
                    targets[id(e)] = new_url
                    if new_url != e.url:
                        a, b = py_index(text, e.offset), py_index(text, e.offset+e.length)
                        # Preserve ordinary labels on post links; substitute source handles/labels
                        # on username links. The entire text-link target is always rewritten.
                        old = TG.fullmatch(e.url)
                        if old and not old.group(2) and cfg['username']:
                            insert(a, b, cfg['username'])
                elif isinstance(e, (types.MessageEntityMentionName, types.InputMessageEntityMentionName)):
                    uid = getattr(e.user_id, 'user_id', e.user_id)
                    if wants(str(uid), cfg) and cfg['username'] and cfg['link']:
                        a, b = py_index(text, e.offset), py_index(text, e.offset+e.length)
                        insert(a, b, cfg['username'])
                        targets[id(e)] = cfg['link']
            for match in list(TG.finditer(text)) + list(PRIVATE_POST.finditer(text)):
                value = await self.rewrite_url(match.group(), p, cfg)
                if value != match.group():
                    insert(*match.span(), value or '')
            for match in MENTION.finditer(text):
                if wants(match.group(1), cfg) and cfg['username']:
                    insert(*match.span(), cfg['username'])
            converted = []
            for e in entities:
                if id(e) in targets:
                    target = targets[id(e)]
                    if target is None:
                        continue  # remove hidden hyperlink, preserve its visible label
                    e = types.MessageEntityTextUrl(e.offset, e.length, target)
                converted.append(e)
            entities = converted
        text, entities = splice(text, entities, changes)
        for find, replace in cfg['replacements']:
            text, entities = splice(text, entities, [Edit(m.start(), m.end(), replace) for m in re.finditer(re.escape(find), text)])
        if not cfg['custom_emoji']:
            entities = [e for e in entities if not isinstance(e, types.MessageEntityCustomEmoji)]
        header = cfg['header'] + ('\n' if cfg['header'] else '')
        footer = ('\n' if cfg['footer'] else '') + cfg['footer']
        for e in entities:
            e.offset += u16(header)
        return header + text + footer, entities

    async def buttons(self, p: Pair, m, using: str):
        cfg = self.cfg(p)
        if cfg['link_filter'] == 'remove':
            return None
        rows = []
        unsupported = False
        for row in getattr(getattr(m, 'reply_markup', None), 'rows', []) or []:
            buttons = []
            for b in row.buttons:
                if not isinstance(b, types.KeyboardButtonUrl):
                    unsupported = True
                    continue
                if cfg['button_mode'] == 'remove':
                    continue
                url = b.url if cfg['button_mode'] == 'keep' else await self.rewrite_url(b.url, p, cfg)
                if not url:
                    continue
                label = b.text
                if cfg['button_mode'] == 'replace':
                    label, _ = splice(label, [], [Edit(x.start(), x.end(), cfg['username']) for x in MENTION.finditer(label) if cfg['username'] and wants(x.group(1), cfg)])
                buttons.append(InlineKeyboardButton(text=label, url=url))
            if buttons:
                rows.append(buttons)
        if unsupported:
            await self.alert(f'Pair #{p.id}: non-URL buttons omitted; buttons owned by another bot cannot be copied.')
        if using == 'userbot':
            if rows:
                await self.alert(f'Pair #{p.id}: URL keyboard omitted in userbot mode; ordinary user accounts cannot attach bot inline keyboards.')
            return None
        return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    async def preview(self, p: Pair):
        if not self.client or not p.source:
            raise MirrorError('Connect account and wait for source membership first.')
        latest = await self.client.get_messages(p.source, limit=1)
        if not latest:
            raise MirrorError('Source has no posts.')
        m = latest[0]
        try:
            started = time.perf_counter()
            text, ents = await self.transform(p, m)
        except Skip as exc:
            return await self.say(f'Sample would be skipped: {exc}')
        await self.say(f'PREVIEW ONLY, nothing posted to destination\nBefore:\n{m.raw_text[:1400]}\n\nAfter:\n{text[:1400]}\n\nTransform: {(time.perf_counter()-started)*1000:.1f} ms\nEntities: {len(ents)}')
        if text and u16(text) <= 4096:
            await self.bot.send_message(self.owner, text, entities=bot_entities(ents, self.cfg(p)['custom_emoji']),
                                        link_preview_options={'is_disabled': True})

    def attach_handlers(self, client: TelegramClient):
        async def fresh(event):
            if getattr(event.message, 'grouped_id', None):
                return
            await self.ingest_messages(event.chat_id, [event.message], 'new')
        async def album(event):
            await self.ingest_messages(event.chat_id, sorted(event.messages, key=lambda x: x.id), 'new')
        async def edited(event):
            await self.ingest_messages(event.chat_id, [event.message], 'edit')
        async def deleted(event):
            if event.chat_id:
                await self.ingest_delete(event.chat_id, event.deleted_ids)
        async def membership(update):
            if isinstance(update, (types.UpdateChannel, types.UpdateChannelParticipant)):
                for p in await self.rows(Pair, Pair.state == 'pending'):
                    try:
                        await self.check_pending(p)
                    except Exception:
                        pass
        client.add_event_handler(fresh, events.NewMessage())
        client.add_event_handler(album, events.Album())
        client.add_event_handler(edited, events.MessageEdited())
        client.add_event_handler(deleted, events.MessageDeleted())
        client.add_event_handler(membership, events.Raw())

    async def enqueue(self, p: Pair, ids: list[int], kind: str, suffix=''):
        digest = hashlib.sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest()
        key = f'{p.id}:{kind}:{digest}:{suffix}'
        from sqlalchemy.exc import IntegrityError
        async with self.Session.begin() as s:
            if await s.scalar(select(Job.id).where(Job.key == key)):
                return
            try:
                async with s.begin_nested():
                    s.add(Job(key=key, pair=p.id, destination=p.destination, ids=ids, kind=kind,
                              source_order=min(ids), due=time.time()+1.2 if kind == 'new' else 0))
                    await s.flush()
            except IntegrityError:
                return
        self.wake.set()

    async def ingest_messages(self, source: int, messages: list, kind: str):
        async with self.ingest:
            if self.pause:
                return
            pairs = await self.rows(Pair, Pair.source == source, Pair.state == 'active')
            for p in pairs:
                cfg = self.cfg(p)
                if kind == 'edit':
                    if not cfg['sync_edits'] or not await self.mapped(p.id, messages[0].id):
                        continue
                    stamp = messages[0].edit_date
                    await self.enqueue(p, [messages[0].id], 'edit', stamp.isoformat() if stamp else str(time.time_ns()))
                    continue
                new = [m for m in messages if m.id > p.baseline and not isinstance(m, types.MessageService)]
                if new:
                    await self.enqueue(p, [m.id for m in new], 'new')
                    # Only the recovery scan advances cursor. Live album events may
                    # arrive after a higher-ID singleton; baseline never advances here.

    async def ingest_delete(self, source: int, ids: list[int]):
        if self.pause:
            return
        for p in await self.rows(Pair, Pair.source == source, Pair.state == 'active'):
            if self.cfg(p)['sync_deletes']:
                await self.enqueue(p, sorted(ids), 'delete')

    async def commit_delivery(self, j: Job, destination_ids: list[int]):
        if len(j.ids) != len(destination_ids):
            raise Uncertain('Telegram returned an unexpected message count. Reconcile destination IDs manually.')
        async with self.Session.begin() as s:
            for source, dest in zip(j.ids, destination_ids):
                found = await s.scalar(select(Mapping).where(Mapping.pair == j.pair, Mapping.source_id == source))
                if found:
                    found.destination_id = dest
                else:
                    s.add(Mapping(pair=j.pair, source_id=source, destination_id=dest))
            await s.execute(update(Job).where(Job.id == j.id).values(state='done', error=''))
            await s.execute(update(Pair).where(Pair.id == j.pair).values(copied=Pair.copied+len(j.ids), last_post=time.time()))

    async def reply_id(self, p: Pair, m):
        parent = getattr(m, 'reply_to_msg_id', None)
        return await self.mapped(p.id, parent)

    async def dispatch(self, j: Job, p: Pair):
        if not self.client:
            raise MirrorError('User account is offline. Re-login in Accounts.')
        cfg = self.cfg(p)
        using = cfg['post_using']
        if j.kind == 'delete':
            ids = [d for source in j.ids if (d := await self.mapped(p.id, source))]
            if ids:
                if using == 'bot':
                    # Bot deletion limitations (permissions and age) are reported as failed jobs.
                    await self.bot.delete_messages(p.destination, ids)
                else:
                    await self.client.delete_messages(p.destination, ids)
            await self.patch(Job, j.id, state='done')
            return
        messages = await self.client.get_messages(p.source, ids=j.ids)
        if not all(messages):
            if j.kind == 'new':
                raise Skip('Source post was deleted before it could be copied.')
            raise MirrorError('Source edit is no longer accessible. Discard job or restore source access.')
        source_chat = await self.client.get_entity(p.source)
        restricted = bool(getattr(source_chat, 'noforwards', False)) or any(getattr(m, 'noforwards', False) for m in messages)
        if any(m.media and media_kind(m) == 'text' and not isinstance(m.media, types.MessageMediaWebPage) for m in messages):
            raise MirrorError('Unsupported media service type (paid media, location, game, etc.). This post was not silently reduced to text.')
        if j.kind == 'new':
            existing = [await self.mapped(p.id, m.id) for m in messages]
            if all(existing):
                await self.patch(Job, j.id, state='done')
                return
            if any(existing):
                raise Uncertain('Partial album mapping exists. Reconcile all destination IDs before retrying.')
        transformed = [await self.transform(p, m) for m in messages]
        # All members of an album pass filters together, avoiding partial album publication.
        if j.kind == 'new' and cfg['orphan_quote'] and not await self.reply_id(p, messages[0]):
            parent_id = messages[0].reply_to_msg_id
            if parent_id:
                parent = await self.client.get_messages(p.source, ids=parent_id)
                if parent:
                    # Parent text must pass the same replacement/filter pipeline.
                    parent_text, _ = await self.transform(p, parent)
                    quote = '> ' + parent_text[:120].replace('\n', ' ') + '\n'
                    text, ent = transformed[0]
                    for e in ent:
                        e.offset += u16(quote)
                    transformed[0] = (quote+text, ent)
        if j.kind == 'edit':
            await self.send_edit(p, messages[0], transformed[0], using)
            await self.patch(Job, j.id, state='done')
            return
        try:
            if using == 'userbot':
                sent_ids = await self.send_user(p, messages, transformed, reupload=restricted)
            else:
                try:
                    sent_ids = await self.send_bot(p, messages, transformed)
                except TelegramBadRequest as exc:
                    if cfg['custom_emoji'] and ('emoji' in str(exc).lower() or 'premium' in str(exc).lower()):
                        fallback = [(text, [e for e in ents if not isinstance(e, types.MessageEntityCustomEmoji)]) for text, ents in transformed]
                        sent_ids = await self.send_bot(p, messages, fallback)
                        await self.alert(f'Pair #{p.id}: custom emoji rejected by Telegram; base emoji used instead.')
                    else:
                        raise
        except (TelegramNetworkError, asyncio.TimeoutError, ConnectionError, OSError) as exc:
            raise Uncertain(f'Unknown delivery outcome ({type(exc).__name__}). Inspect destination and reconcile or explicitly retry.') from None
        # Any DB failure after send is deliberately left as 'sending'; worker quarantines it.
        await self.commit_delivery(j, sent_ids)

    async def send_user(self, p: Pair, messages: list, transformed: list, reupload=False):
        cfg = self.cfg(p)
        first = messages[0]
        if cfg['custom_emoji'] and not bool(getattr(await self.client.get_me(), 'premium', False)):
            transformed = [(text, [e for e in ents if not isinstance(e, types.MessageEntityCustomEmoji)]) for text, ents in transformed]
        reply = await self.reply_id(p, first)
        silent = first.silent if cfg['silent'] is None else cfg['silent']
        await self.buttons(p, first, 'userbot')
        # Protected/restricted sources cannot always reuse their Telegram file
        # references. Upload from temporary files, never from a RAM-sized blob.
        if reupload and any(m.photo or m.document for m in messages):
            if any(getattr(m, 'invert_media', False) for m in messages):
                raise MirrorError('Caption-above-media requires bot posting in this implementation.')
            with tempfile.TemporaryDirectory(prefix='mirror-user-') as directory:
                files = [str((await self.download(m, directory, cfg)).path) for m in messages]
                captions = [x[0] for x in transformed]
                entities = [x[1] for x in transformed]
                result = await self.client.send_file(p.destination, files if len(files) > 1 else files[0],
                    caption=captions if len(files) > 1 else captions[0],
                    formatting_entities=entities if len(files) > 1 else entities[0],
                    reply_to=reply, silent=silent, parse_mode=None,
                    voice_note=media_kind(first) == 'voice', video_note=media_kind(first) == 'video_note',
                    supports_streaming=media_kind(first) == 'video')
                return [x.id for x in result] if isinstance(result, list) else [result.id]
        if len(messages) > 1:
            if any(media_kind(m) not in {'photo', 'video', 'audio', 'document'} for m in messages):
                raise MirrorError('Unsupported album member type. Use ordinary photo/video/audio/document albums.')
            if any(getattr(m, 'invert_media', False) for m in messages):
                raise MirrorError('Caption-above-media is not supported in userbot mode. Switch this pair to bot posting.')
            result = await self.client.send_file(p.destination, [m.media for m in messages],
                caption=[x[0] for x in transformed], formatting_entities=[x[1] for x in transformed],
                reply_to=reply, silent=silent, parse_mode=None)
            return [m.id for m in result]
        text, ent = transformed[0]
        if getattr(first, 'invert_media', False):
            raise MirrorError('Caption-above-media requires bot posting in this implementation.')
        if media_kind(first) == 'poll':
            raise MirrorError('Polls require bot posting in this implementation.')
        if first.media and media_kind(first) != 'text':
            result = await self.client.send_file(p.destination, first.media, caption=text,
                formatting_entities=ent, reply_to=reply, silent=silent, parse_mode=None,
                voice_note=media_kind(first) == 'voice', video_note=media_kind(first) == 'video_note')
        else:
            result = await self.client.send_message(p.destination, text, formatting_entities=ent,
                reply_to=reply, silent=silent, parse_mode=None, link_preview=bool(first.web_preview))
        return [result.id]

    async def download(self, m, directory: str, cfg: dict) -> FSInputFile:
        size = getattr(getattr(m, 'file', None), 'size', 0) or 0
        if size > cfg['max_media_mb'] * 1024**2:
            raise MirrorError('Media exceeds pair size limit. Raise limit or use userbot posting; check Telegram limits.')
        filename = Path(directory) / (str(m.id) + (getattr(getattr(m, 'file', None), 'ext', '') or '.bin'))
        # Telethon streams to a file. aiogram FSInputFile streams that file on upload.
        path = await self.client.download_media(m, file=str(filename))
        if not path:
            raise MirrorError('Media cannot be downloaded with this account. Check source access.')
        if Path(path).stat().st_size > cfg['max_media_mb'] * 1024**2:
            raise MirrorError('Downloaded media exceeds configured size limit.')
        return FSInputFile(path, filename=getattr(getattr(m, 'file', None), 'name', None) or Path(path).name)

    async def send_bot(self, p: Pair, messages: list, transformed: list):
        cfg = self.cfg(p)
        first = messages[0]
        reply = await self.reply_id(p, first)
        common = {'chat_id': p.destination,
                  'disable_notification': first.silent if cfg['silent'] is None else cfg['silent']}
        if reply:
            common['reply_parameters'] = ReplyParameters(message_id=reply, allow_sending_without_reply=True)
        with tempfile.TemporaryDirectory(prefix='mirror-') as directory:
            if len(messages) > 1:
                if len(messages) > 10:
                    raise MirrorError('Album exceeds Bot API group size.')
                album = []
                for m, (text, ents) in zip(messages, transformed):
                    kind = media_kind(m)
                    cls = {'photo': InputMediaPhoto, 'video': InputMediaVideo,
                           'document': InputMediaDocument, 'audio': InputMediaAudio}.get(kind)
                    if not cls:
                        raise MirrorError(f'Unsupported album type {kind}.')
                    if u16(text) > 1024:
                        raise MirrorError('Caption exceeds Bot API length; use userbot posting or shorten header/footer.')
                    kw = {'media': await self.download(m, directory, cfg), 'caption': text or None,
                          'caption_entities': bot_entities(ents, cfg['custom_emoji'])}
                    if kind in {'photo', 'video'}:
                        kw.update(has_spoiler=bool(getattr(m.media, 'spoiler', False)),
                                  show_caption_above_media=bool(getattr(m, 'invert_media', False)))
                    album.append(cls(**kw))
                result = await self.bot.send_media_group(**common, media=album)
                return [m.message_id for m in result]
            m = first
            text, ents = transformed[0]
            entities = bot_entities(ents, cfg['custom_emoji'])
            kind = media_kind(m)
            common['reply_markup'] = await self.buttons(p, m, 'bot')
            if kind == 'text':
                if not text.strip():
                    raise Skip('Empty text after filters')
                if u16(text) > 4096:
                    raise MirrorError('Text exceeds Bot API 4096 UTF-16 units; shorten custom text or use userbot.')
                result = await self.bot.send_message(**common, text=text, entities=entities,
                    link_preview_options={'is_disabled': not bool(m.web_preview)})
            elif kind == 'poll':
                poll = m.poll.poll
                if poll.quiz:
                    raise MirrorError('Quiz correct answer is not reliably available to the listener. Quiz polls are not copied.')
                question = getattr(poll.question, 'text', poll.question)
                options = [InputPollOption(text=getattr(a.text, 'text', a.text)) for a in poll.answers]
                # Transform poll labels separately; message captions do not contain poll text.
                class Label:
                    photo = poll = document = None
                    def __init__(self, raw, entities):
                        self.raw_text, self.entities = raw, entities
                qtext, qent = await self.transform(p, Label(str(question), getattr(poll.question, 'entities', [])))
                for answer, option in zip(poll.answers, options):
                    value, ent = await self.transform(p, Label(str(getattr(answer.text, 'text', answer.text)), getattr(answer.text, 'entities', [])))
                    option.text = value
                    option.text_entities = bot_entities(ent, cfg['custom_emoji'])
                result = await self.bot.send_poll(**common, question=qtext,
                    question_entities=bot_entities(qent, cfg['custom_emoji']), options=options,
                    is_anonymous=not poll.public_voters, allows_multiple_answers=bool(poll.multiple_choice),
                    is_closed=bool(poll.closed))
            else:
                if u16(text) > 1024:
                    raise MirrorError('Caption exceeds Bot API length. Shorten custom text or use userbot.')
                upload = await self.download(m, directory, cfg)
                kw = dict(common)
                if kind not in {'sticker', 'video_note'}:
                    kw.update(caption=text or None, caption_entities=entities)
                elif text.strip():
                    raise MirrorError('Sticker/video-note cannot carry captions. Clear custom header/footer for this pair.')
                if kind in {'photo', 'video', 'animation'}:
                    kw.update(has_spoiler=bool(getattr(m.media, 'spoiler', False)),
                              show_caption_above_media=bool(getattr(m, 'invert_media', False)))
                method = getattr(self.bot, 'send_' + kind)
                kw[kind] = upload
                if kind == 'video':
                    kw['supports_streaming'] = True
                result = await method(**kw)
            return [result.message_id]

    async def send_edit(self, p: Pair, m, transformed, using: str):
        dest = await self.mapped(p.id, m.id)
        if not dest:
            raise Skip('Edited source was never copied')
        cfg = self.cfg(p)
        text, ents = transformed
        kind = media_kind(m)
        if kind in {'poll', 'sticker', 'video_note', 'voice'}:
            raise MirrorError(f'Edit sync not supported for {kind}. Turn off edit sync or discard this job.')
        try:
            if using == 'userbot':
                await self.client.edit_message(p.destination, dest, text,
                    formatting_entities=ents, parse_mode=None,
                    file=m.media if kind != 'text' else None,
                    link_preview=bool(m.web_preview))
                return
            markup = await self.buttons(p, m, 'bot')
            ent = bot_entities(ents, cfg['custom_emoji'])
            if kind == 'text':
                await self.bot.edit_message_text(chat_id=p.destination, message_id=dest, text=text,
                                                entities=ent, reply_markup=markup,
                                                link_preview_options={'is_disabled': not bool(m.web_preview)})
            else:
                cls = {'photo': InputMediaPhoto, 'video': InputMediaVideo,
                       'animation': __import__('aiogram.types', fromlist=['InputMediaAnimation']).InputMediaAnimation,
                       'audio': InputMediaAudio, 'document': InputMediaDocument}.get(kind)
                if not cls:
                    raise MirrorError('Unsupported media edit.')
                with tempfile.TemporaryDirectory(prefix='mirror-edit-') as directory:
                    kw = {'media': await self.download(m, directory, cfg), 'caption': text or None, 'caption_entities': ent}
                    if kind in {'photo', 'video', 'animation'}:
                        kw.update(has_spoiler=bool(getattr(m.media, 'spoiler', False)), show_caption_above_media=bool(getattr(m, 'invert_media', False)))
                    await self.bot.edit_message_media(chat_id=p.destination, message_id=dest, media=cls(**kw), reply_markup=markup)
        except TelegramBadRequest as exc:
            if 'message is not modified' not in str(exc).lower():
                raise
        except errors.MessageNotModifiedError:
            pass

    async def worker(self):
        """One delivery worker: bounded RAM and serialized destination publication.

        Global head-of-line selection preserves each destination's job order. A
        flood wait blocks its destination, not unrelated destinations. Source
        Telegram arrival order is retained; cross-source IDs have no total order.
        """
        while self.alive:
            try:
                if not self.pause and self.client:
                    jobs = await self.rows(Job, Job.state.in_(['pending', 'uncertain', 'failed']), order=Job.id, limit=512)
                    # Telethon assembles albums after single-message updates. Within
                    # each contiguous NEW-job run for a destination/pair, sort by source
                    # ID. Keep edit/delete/operator barriers in arrival order.
                    buckets = defaultdict(list)
                    for queued in jobs:
                        buckets[queued.destination].append(queued)
                    ordered = []
                    for bucket in buckets.values():
                        run = []
                        for queued in bucket:
                            if queued.kind == 'new' and queued.state == 'pending' and (not run or queued.pair == run[0].pair):
                                run.append(queued)
                            else:
                                ordered.extend(sorted(run, key=lambda x: x.source_order))
                                run = []
                                if queued.kind == 'new' and queued.state == 'pending':
                                    run.append(queued)
                                else:
                                    ordered.append(queued)
                        ordered.extend(sorted(run, key=lambda x: x.source_order))
                    jobs = ordered
                    seen = set()
                    for j in jobs:
                        if j.destination in seen:
                            continue
                        seen.add(j.destination)
                        if j.state != 'pending' or j.due > time.time():
                            continue
                        p = await self.get(Pair, j.pair)
                        if not p or p.state != 'active':
                            continue
                        await self.patch(Job, j.id, state='sending', attempts=j.attempts+1)
                        try:
                            await self.dispatch(j, p)
                        except Skip as exc:
                            await self.patch(Job, j.id, state='skipped', error=str(exc))
                        except (errors.FloodWaitError, TelegramRetryAfter) as exc:
                            seconds = getattr(exc, 'seconds', getattr(exc, 'retry_after', 60))
                            await self.patch(Job, j.id, state='pending', due=time.time()+seconds+1,
                                             error=f'Telegram flood wait: {seconds}s')
                        except (errors.AuthKeyUnregisteredError, errors.SessionRevokedError, errors.UserDeactivatedBanError):
                            await self.patch(Job, j.id, state='pending', error='Login revoked. Re-login.')
                            await self.account_dead()
                        except Uncertain as exc:
                            await self.patch(Job, j.id, state='uncertain', error=str(exc))
                            await self.alert(f'Job #{j.id}: delivery uncertain. Inspect destination, then resolve in Jobs. Later jobs to this destination are blocked.')
                        except (TelegramBadRequest, TelegramForbiddenError, errors.RPCError, MirrorError) as exc:
                            # Server rejection is known; error names only, no payload/secret logging.
                            msg = str(exc) if isinstance(exc, MirrorError) else type(exc).__name__ + ': check source/destination rights, message length and account status.'
                            await self.patch(Job, j.id, state='failed', error=msg[:600])
                            await self.patch(Pair, p.id, failures=p.failures+1, error=f'Job #{j.id}: {msg[:300]}')
                            await self.alert(f'Pair #{p.id} job #{j.id} failed: {msg[:500]} Fix the issue, then Jobs -> Retry or Discard.')
                        except Exception as exc:
                            await self.patch(Job, j.id, state='uncertain', error=f'{type(exc).__name__}: unknown outcome, inspect destination before retry.')
                            await self.alert(f'Job #{j.id} needs manual reconciliation after {type(exc).__name__}. Open Jobs.')
                self.wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), 1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.error('Worker temporarily unavailable: %s', type(exc).__name__)
                await asyncio.sleep(5)

    async def account_dead(self):
        if self.client:
            await self.client.disconnect()
            self.client = None
        self.pause = True
        await self.save_setting('pause', True)
        # Invalid stored auth keys must not be retried indefinitely on restart.
        async with self.Session.begin() as s:
            await s.execute(delete(Account))
        await self.say('Account session expired/revoked. Mirroring paused. Re-login, then Resume all.', [[('Re-login', 'login')]])

    async def recovery(self):
        if not self.client or self.pause:
            return
        for p in await self.rows(Pair, Pair.state == 'active'):
            try:
                # Hold boundary lock so live updates cannot jump the watermark
                # ahead of the recovery scan. Stream history, grouping albums.
                async with self.ingest:
                    pending = []
                    group = None
                    async def flush():
                        if pending:
                            await self.enqueue(p, [m.id for m in pending], 'new')
                            await self.patch(Pair, p.id, cursor=pending[-1].id)
                            p.cursor = pending[-1].id
                    async for m in self.client.iter_messages(p.source, min_id=p.cursor, reverse=True):
                        if isinstance(m, types.MessageService):
                            continue
                        gid = m.grouped_id
                        if pending and (not gid or gid != group):
                            await flush()
                            pending = []
                        pending.append(m)
                        group = gid
                        if not gid:
                            await flush()
                            pending = []
                    await flush()
            except errors.FloodWaitError as exc:
                await self.alert(f'Catch-up for pair #{p.id} delayed {exc.seconds}s by Telegram. It will retry automatically.')
                await asyncio.sleep(min(exc.seconds+1, 300))
            except (errors.ChannelPrivateError, errors.UserBannedInChannelError):
                await self.patch(Pair, p.id, state='error', error='Source inaccessible: kicked, banned or private. Restore membership and enable again.')
                await self.alert(f'Pair #{p.id}: source inaccessible. Check membership, then enable again.')
            except (errors.AuthKeyUnregisteredError, errors.SessionRevokedError):
                await self.account_dead()
                return
            except Exception as exc:
                LOG.warning('Catch-up postponed for pair %s: %s', p.id, type(exc).__name__)

    async def maintenance(self):
        tick = 0
        while self.alive:
            try:
                if self.draft and time.monotonic()-self.draft.touched > 600:
                    async with self.guard:
                        await self.clear_draft()
                    await self.alert('Admin/login prompt expired. Open /start to continue.')
                if self.client:
                    if not self.client.is_connected():
                        await self.client.connect()
                    if not await self.client.is_user_authorized():
                        await self.account_dead()
                    else:
                        for p in await self.rows(Pair, Pair.state == 'pending'):
                            try:
                                await self.check_pending(p)
                            except errors.FloodWaitError as exc:
                                await asyncio.sleep(min(exc.seconds+1, 300))
                                break
                            except Exception:
                                pass
                        # Recovery also catches missing event updates after reconnection.
                        if tick % 2 == 0:
                            await self.recovery()
                tick += 1
                # Keep durable queue bounded without deleting dedup message mappings.
                if tick % 120 == 0:
                    async with self.Session.begin() as s:
                        # Job keys for NEW remain protected by the mapping even after cleanup.
                        keep_from = (await s.scalar(select(Job.id).order_by(Job.id.desc()).limit(1)) or 0)-10000
                        await s.execute(delete(Job).where(Job.id < keep_from, Job.state.in_(['done', 'skipped', 'cancelled'])))
                        receipts = (await s.scalars(select(KV).where(KV.key.like('update:%')))).all()
                        for receipt in receipts:
                            if isinstance(receipt.value, dict) and receipt.value.get('time', 0) < time.time()-7*86400:
                                await s.delete(receipt)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning('Maintenance postponed: %s', type(exc).__name__)
            await asyncio.sleep(45)

    async def initialize(self):
        async with self.engine.begin() as c:
            await c.run_sync(Base.metadata.create_all)
        self.pause = await self.setting('pause', False)
        async with self.Session.begin() as s:
            await s.execute(update(Job).where(Job.state == 'sending').values(state='uncertain', error='Process stopped during delivery. Inspect destination and reconcile before retrying.'))
        row = await self.get(Account, 1)
        if row:
            try:
                credentials = json.loads(self.fernet.decrypt(row.secret.encode()))
                self.client = TelegramClient(StringSession(credentials['session']), credentials['api_id'], credentials['api_hash'],
                    auto_reconnect=True, connection_retries=10, flood_sleep_threshold=0,
                    sequential_updates=True)
                await self.client.connect()
                if not await self.client.is_user_authorized():
                    await self.account_dead()
                else:
                    async for _ in self.client.iter_dialogs():
                        pass
                    self.attach_handlers(self.client)
            except Exception as exc:
                if self.client:
                    await self.client.disconnect()
                    self.client = None
                await self.alert(f'Session restore failed ({type(exc).__name__}). Verify ENCRYPTION_KEY or re-login. Existing encrypted credentials were not overwritten.')
        await self.bot.set_webhook(self.public_url+self.webhook_path,
            secret_token=self.webhook_secret, allowed_updates=['message', 'callback_query'],
            drop_pending_updates=False, max_connections=1)
        # Start recovery before delivery, with event handlers already attached.
        await self.recovery()
        self.tasks = [asyncio.create_task(self.worker()), asyncio.create_task(self.maintenance())]
        self.ready = True
        interrupted = [r for r in await self.rows(KV) if r.key.startswith('update:') and isinstance(r.value, dict) and r.value.get('state') == 'processing']
        if interrupted:
            await self.alert('An admin action was interrupted before restart. Open /start and repeat it if necessary. Login drafts are not restored.')
        if await self.rows(Job, Job.state == 'uncertain'):
            await self.alert('Recovered uncertain jobs. Inspect destination and resolve them in Jobs before their destinations resume.')

    async def webhook(self, request: web.Request):
        supplied = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if not hmac.compare_digest(supplied, self.webhook_secret):
            return web.Response(status=403)
        if not self.ready:
            return web.Response(status=503)
        try:
            payload = await request.json()
            ident = payload.get('update_id')
            if not isinstance(ident, int):
                return web.Response(status=400)
            # At-most-once panel actions: a duplicate delivery must not toggle a
            # pair twice or treat the same login code as a password. A crashed
            # 'processing' action is reported on restart, not automatically replayed.
            async with self.webhook_lock:
                key = f'update:{ident}'
                if await self.get(KV, key):
                    return web.Response(text='duplicate')
                await self.save_setting(key, {'state': 'processing', 'time': time.time()})
                await self.dp.feed_raw_update(self.bot, payload)
                await self.save_setting(key, {'state': 'done', 'time': time.time()})
        except Exception as exc:
            LOG.error('Webhook handler failed: %s', type(exc).__name__)
            await self.alert('An admin action was interrupted. Open /start and re-enter the action; it will not be replayed automatically.')
            return web.Response(status=500)
        return web.Response(text='ok')

    async def stop(self):
        self.alive = False
        self.ready = False
        self.wake.set()
        await self.clear_draft()
        # Allow up to 20 seconds for in-flight send + mapping transaction.
        for task in self.tasks:
            if task is self.tasks[0]:
                try:
                    await asyncio.wait_for(asyncio.shield(task), 20)
                except (asyncio.TimeoutError, Exception):
                    task.cancel()
            else:
                task.cancel()
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self.client:
            await self.client.disconnect()
        await self.bot.session.close()
        await self.engine.dispose()

def _clean(value: Any) -> str:
    """Strip whitespace and quotes that often get pasted into dashboards."""
    text = str(value or '').strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in '"\'':
        text = text[1:-1].strip()
    return text

def _setting(*names: str) -> str:
    """Environment variables win; the CONFIG block is only a fallback."""
    for source in (os.environ, CONFIG):
        for name in names:
            value = _clean(source.get(name))
            if value:
                return value
    return ''

def load_config(public_url_arg: str | None = None, strict_url: bool = True) -> dict[str, Any]:
    """Validate every setting up front and report ALL problems in one message."""
    load_dotenv()
    problems: list[str] = []
    token = _setting('BOT_TOKEN')
    if not token:
        problems.append('BOT_TOKEN is missing. Create a bot with @BotFather and paste its token.')
    elif not re.fullmatch(r'\d{5,}:[A-Za-z0-9_-]{30,}', token):
        problems.append('BOT_TOKEN has the wrong format (expected 123456789:AAH...). Copy it again from @BotFather, without spaces or quotes.')
    owner = 0
    owner_text = _setting('OWNER_ID', 'ADMIN_ID')
    if not owner_text:
        problems.append('OWNER_ID (admin id) is missing. Send /start to @userinfobot to get your numeric Telegram id.')
    elif not re.fullmatch(r'\d{1,15}', owner_text) or int(owner_text) == 0:
        problems.append('OWNER_ID must be your numeric Telegram user id (digits only; not @username, not a group id).')
    else:
        owner = int(owner_text)
    key = _setting('ENCRYPTION_KEY')
    if not key:
        problems.append('ENCRYPTION_KEY is missing. Use this freshly generated key and keep it forever: ' + Fernet.generate_key().decode())
    else:
        try:
            Fernet(key.encode())
        except Exception:
            problems.append('ENCRYPTION_KEY is invalid; it must be a 44-character Fernet key. Generate one with: python bot.py --gen-key')
    database = _setting('DATABASE_URL')
    temporary_db = not database
    if temporary_db:
        database = 'sqlite+aiosqlite:///mirror.db'
        LOG.warning('DATABASE_URL is not set: using a TEMPORARY SQLite file. On Render all data is lost on every restart/redeploy.')
    elif not database.startswith(('postgres://', 'postgresql://', 'postgresql+asyncpg://', 'sqlite://', 'sqlite+aiosqlite://')):
        problems.append('DATABASE_URL must start with postgresql:// (or postgres://). Copy the Internal/External Database URL from your database page.')
    else:
        try:
            make_url(database)
        except Exception:
            problems.append('DATABASE_URL could not be parsed. Copy it again from your database provider (special characters in the password must be URL-encoded).')
    url = _clean(public_url_arg) or _setting('PUBLIC_URL', 'RENDER_EXTERNAL_URL')
    if url and '://' not in url:
        url = 'https://' + url
    url = url.rstrip('/')
    if not url.startswith('https://'):
        message = 'Public HTTPS URL not found. Render sets RENDER_EXTERNAL_URL automatically for Web Services (not Background Workers); elsewhere set PUBLIC_URL=https://your-host or pass --public-url.'
        if strict_url:
            problems.append(message)
        else:
            LOG.warning(message)
    try:
        port = int(os.getenv('PORT', '8080'))
    except ValueError:
        port = 8080
    if problems:
        raise SystemExit('\nCONFIGURATION ERROR - fix the following, then redeploy:\n'
                         + '\n'.join(f'  {i}. {text}' for i, text in enumerate(problems, 1)) + '\n')
    return {'token': token, 'owner': owner, 'key': key, 'database': database,
            'temporary_db': temporary_db, 'url': url, 'port': port}

def describe_config(cfg: dict[str, Any]) -> str:
    shown_db = make_url(cfg['database']).render_as_string(hide_password=True)
    return '\n'.join([
        'Configuration OK',
        f"  BOT_TOKEN       OK (bot id {cfg['token'].split(':', 1)[0]})",
        f"  OWNER_ID        OK ({cfg['owner']})",
        '  ENCRYPTION_KEY  OK',
        f"  DATABASE_URL    {shown_db}" + ('  [TEMPORARY - data lost on restart]' if cfg['temporary_db'] else ''),
        f"  PUBLIC_URL      {cfg['url'] or '(not set)'}",
        f"  PORT            {cfg['port']}",
    ])

def startup_hint(exc: Exception) -> str:
    """Actionable text for the commonest first-deploy failures (no secrets included)."""
    name = type(exc).__name__
    if name == 'TelegramUnauthorizedError':
        return 'BOT_TOKEN is wrong or was revoked in @BotFather.'
    if name == 'TelegramBadRequest':
        return 'Telegram rejected the webhook URL. PUBLIC_URL / RENDER_EXTERNAL_URL must be the https URL of this service.'
    if name in {'InvalidPasswordError', 'InvalidCatalogNameError', 'InvalidAuthorizationSpecificationError',
                'OperationalError', 'InterfaceError', 'CannotConnectNowError', 'ConnectionDoesNotExistError'} \
            or isinstance(exc, (OSError, TimeoutError)):
        return 'Database connection failed. Check DATABASE_URL (use the Internal Database URL when the DB is on Render in the same region).'
    return ''

async def serve(cfg: dict[str, Any]):
    app = App(cfg['token'], cfg['owner'], cfg['database'], cfg['key'], cfg['url'])
    server = web.Application(client_max_size=1024*1024)
    server.router.add_post(app.webhook_path, app.webhook)
    async def health(request):
        return web.json_response({'ok': app.ready and app.alive, 'uptime_seconds': int(time.time()-app.started),
                                  'account_connected': bool(app.client and app.client.is_connected())},
                                 status=200 if app.ready and app.alive else 503)
    server.router.add_get('/healthz', health)
    async def startup(_):
        # Bind HTTP server first so webhook can be configured without a boot-time
        # dependency on the service already accepting HTTPS traffic.
        async def boot():
            try:
                await app.initialize()
                if cfg['temporary_db']:
                    await app.alert('WARNING: DATABASE_URL is not set, so a TEMPORARY SQLite file is in use. Your account login, pairs and history are lost on every restart/redeploy. Set DATABASE_URL to a durable PostgreSQL database.')
            except Exception as exc:
                hint = startup_hint(exc)
                LOG.critical('Startup failed: %s. %s', type(exc).__name__, hint)
                await app.alert(f'Service startup failed ({type(exc).__name__}). ' + (hint or 'Check database connectivity, webhook URL and credentials, then restart.'))
                app.alive = False
        app.boot_task = asyncio.create_task(boot())
    async def cleanup(_):
        if not app.boot_task.done():
            app.boot_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await app.boot_task
        await app.stop()
    server.on_startup.append(startup)
    server.on_cleanup.append(cleanup)
    return server


def run_tests():
    """Offline tests; live Telegram behavior is a separate staging acceptance test."""
    import unittest
    class EntityTests(unittest.TestCase):
        def test_utf16(self):
            self.assertEqual(u16('a😀b'), 4)
            self.assertEqual(py_index('a😀b', 3), 2)
        def test_replace_after_astral(self):
            text = '😀 @source end'
            ent = types.MessageEntityBold(u16('😀 '), u16('@source'))
            out, ents = splice(text, [ent], [Edit(2, 9, '@mine')])
            self.assertEqual(out, '😀 @mine end')
            self.assertEqual((ents[0].offset, ents[0].length), (3, 5))
        def test_enclosing_style(self):
            text = 'x @source z'
            out, ents = splice(text, [types.MessageEntitySpoiler(0, u16(text))], [Edit(2, 9, '@longername')])
            self.assertEqual(ents[0].length, u16(out))
        def test_after_entity_shift(self):
            text = '@source 😀 code'
            ent = types.MessageEntityCode(u16('@source 😀 '), 4)
            out, ents = splice(text, [ent], [Edit(0, 7, '@mine')])
            self.assertEqual(ents[0].offset, u16('@mine 😀 '))
        def test_remove_link_entity(self):
            _, ents = splice('@source', [types.MessageEntityMention(0, 7)], [Edit(0, 7, '')])
            self.assertEqual(ents, [])
        def test_emails(self):
            self.assertEqual(list(MENTION.finditer('user@gmail.com')), [])
        def test_adjacent_changes(self):
            text, ent = splice('ab', [types.MessageEntityBold(0, 2)], [Edit(0, 1, '😀'), Edit(1, 2, 'X')])
            self.assertEqual(text, '😀X')
            self.assertEqual(ent[0].length, 3)
        def test_whitelist(self):
            cfg = dict(DEFAULTS, whitelist=['keepme'])
            self.assertFalse(wants('KeepMe', cfg))
            self.assertTrue(wants('replace', cfg))
        def test_overlap_rejected(self):
            with self.assertRaises(MirrorError):
                splice('abcdef', [], [Edit(0, 4, 'x'), Edit(2, 5, 'y')])
    class PipelineTests(unittest.IsolatedAsyncioTestCase):
        def harness(self, config=None, mapping=None):
            from types import SimpleNamespace
            class Harness:
                transform = App.transform
                rewrite_url = App.rewrite_url
                destination_post_link = App.destination_post_link
                link_fallback = App.link_fallback
                def cfg(self, pair):
                    return DEFAULTS | pair.config
                async def mapped(self, pid, mid):
                    return (mapping or {}).get(mid)
            pair = Pair(id=1, source=-1001234567890, destination=-1009876543210,
                source_name='sourcechan', destination_name='mychannel', config=DEFAULTS | {
                    'username': '@mychannel', 'link': 'https://t.me/mychannel'} | (config or {}))
            def message(text, entities=None):
                return SimpleNamespace(raw_text=text, entities=entities or [],
                    photo=None, poll=None, document=None, reply_markup=None)
            return Harness(), pair, message

        async def test_plain_and_hidden(self):
            app, pair, message = self.harness()
            text = '😀 @sourcechan contact'
            entity = types.MessageEntityTextUrl(u16('😀 @sourcechan '), u16('contact'), 'https://t.me/anothername')
            result, entities = await app.transform(pair, message(text, [entity]))
            self.assertEqual(result, '😀 @mychannel @mychannel')
            self.assertEqual(entities[0].url, 'https://t.me/mychannel')
            self.assertEqual(entities[0].offset, u16('😀 @mychannel '))
            self.assertEqual(entities[0].length, u16('@mychannel'))

        async def test_public_post_link(self):
            app, pair, message = self.harness(mapping={123: 456})
            result, _ = await app.transform(pair, message('https://t.me/sourcechan/123'))
            self.assertEqual(result, 'https://t.me/mychannel/456')

        async def test_private_post_link(self):
            app, pair, message = self.harness(mapping={123: 456})
            result, _ = await app.transform(pair, message('https://t.me/c/1234567890/123'))
            self.assertEqual(result, 'https://t.me/mychannel/456')

        async def test_skip_links(self):
            app, pair, message = self.harness({'link_filter': 'skip'})
            with self.assertRaises(Skip):
                await app.transform(pair, message('contact @sourcechan'))

        async def test_remove_links_preserve_style(self):
            app, pair, message = self.harness({'link_filter': 'remove'})
            text = '😀 before @sourcechan after'
            result, entities = await app.transform(pair, message(text, [types.MessageEntityBold(0, u16(text))]))
            self.assertEqual(result, '😀 before  after')
            self.assertEqual(entities[0].length, u16(result))

        async def test_email_is_not_username(self):
            app, pair, message = self.harness()
            text = 'user@gmail.com @sourcechan'
            result, _ = await app.transform(pair, message(text))
            self.assertEqual(result, 'user@gmail.com @mychannel')

        async def test_whitelisted_username(self):
            app, pair, message = self.harness({'whitelist': ['sourcechan']})
            result, _ = await app.transform(pair, message('@sourcechan @anothername'))
            self.assertEqual(result, '@sourcechan @mychannel')

    suite = unittest.TestSuite([
        unittest.defaultTestLoader.loadTestsFromTestCase(EntityTests),
        unittest.defaultTestLoader.loadTestsFromTestCase(PipelineTests),
    ])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Owner-only Telegram channel mirroring service')
    parser.add_argument('--public-url', help='HTTPS base URL outside Render; no Telegram polling is used')
    parser.add_argument('--self-test', action='store_true', help='Run offline UTF-16/entity tests, no Telegram credentials required')
    parser.add_argument('--gen-key', action='store_true', help='Print a new ENCRYPTION_KEY and exit')
    parser.add_argument('--check-config', action='store_true', help='Validate BOT_TOKEN, OWNER_ID, ENCRYPTION_KEY, DATABASE_URL and the public URL, then exit')
    args = parser.parse_args()
    if args.self_test:
        run_tests()
    if args.gen_key:
        print(Fernet.generate_key().decode())
        raise SystemExit(0)
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    # Third-party exception/request logging can expose peer phone/code payloads.
    for name in ('telethon', 'aiogram', 'aiohttp.access', 'sqlalchemy.engine'):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    cfg = load_config(args.public_url, strict_url=not args.check_config)
    if args.check_config:
        print(describe_config(cfg))
        raise SystemExit(0)
    web.run_app(serve(cfg), host='0.0.0.0', port=cfg['port'], access_log=None)
